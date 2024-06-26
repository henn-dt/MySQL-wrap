import mysql.connector as mysql
from collections import namedtuple
from itertools import repeat
from typing import Any, Optional, Set, Callable, Iterator
import json

import pandas as pd
import numpy

np = numpy


"""
    A very simple wrapper for mysql (mysql-connector) with some added Pandas integration functionalities.

    Methods:
        connect() - connects to mysql server
        getOne() - get a single row
        getAll() - get all rows
        lastId() - get the last insert id
        lastQuery() - get the last executed query
        insert() - insert a row
        insertBatch() - Batch Insert
        insertOrUpdate() - insert a row or update it if it exists
        update() - update rows
        delete() - delete rows
        query()  - run a raw sql query
        commit() - commits a transaction for transactional engines
        leftJoin() - do an inner left join query and get results

        pandas based methods:

        createTable() - creates a Table using a DataFrame as the input
        insertTable() - updates a Table using a DataFrame as the input
        updateTable() - updates a Table using a DataFrame as the input, adds missing columns and changes mismatched column types.
        createOrInsertTable() - creates a Table if it doesn´t exists, updates the records if it does
        createOrUpdateTable() - creates a Table if it doesn´t exists, updates the records if it does, adds missing columns and chages mismatched column types
        getTable() - get all rows, return as DataTrame       

    License: GNU GPLv2

    Kailash Nadh, http://nadh.in
    May 2013

    Updated by: 
    Milosh Bolic
    June 2019

    Emiliano Lupo
    June 2024
"""
DTypeDict  = {
    "VARCHAR" : ['string'],
    "DATETIME" : [ np.datetime64, 'datetime' , 'datetime64', 'datetime64[ns, <tz>]'],
    "FLOAT" : ['float32', 'float64', np.float64, 'numpy.float64', numpy.float64],
    "INT" : ['int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64'],
    "TINYINT" : ['boolean'],
    }

def getDataTypefromDType(DType : str) -> str:
    if isinstance(DType, str):
        DType = DType.lower()

    for datatype, dtypes in DTypeDict.items():
        if DType in dtypes:
            return datatype
    return "VARCHAR"

DataTypeLength = {
    "VARCHAR" : "255",
    "TINYINT" : "1"
}


def setMySqlFieldName(name : str) -> str:
    return ''.join(e for e in name if e.isalnum())


class MysqlWrap:
    conn = None
    cur = None
    conf = None

    def __init__(self, **kwargs):
        """ db = MysqlWrap(
	        host="127.0.0.1",
	        db="mydatabase",
	        user="username",
	        passwd="password",
	        keep_alive=True # try and reconnect timedout mysql connections?
            )
        """
        self.conf = kwargs
        self.conf["keep_alive"] = kwargs.get("keep_alive", False)
        self.conf["charset"] = kwargs.get("charset", "utf8")
        self.conf["host"] = kwargs.get("host", "localhost")
        self.conf["port"] = kwargs.get("port", 3306)
        self.conf["autocommit"] = kwargs.get("autocommit", False)
        self.conf["ssl"] = kwargs.get("ssl", False)
        self.connect()

    def connect(self):
        """Connect to the mysql server"""

        try:
            if not self.conf["ssl"]:
                self.conn = mysql.connect(db=self.conf['db'], host=self.conf['host'],
                                          port=self.conf['port'], user=self.conf['user'],
                                          passwd=self.conf['passwd'],
                                          charset=self.conf['charset'])
            else:
                self.conn = mysql.connect(db=self.conf['db'], host=self.conf['host'],
                                          port=self.conf['port'], user=self.conf['user'],
                                          passwd=self.conf['passwd'],
                                          ssl=self.conf['ssl'],
                                          charset=self.conf['charset'])
            self.cur = self.conn.cursor()
            self.conn.autocommit = self.conf["autocommit"]
        except:
            print("MySQL connection failed")
            raise

    def getOne(self, table=None, fields='*', where=None, order=None, limit=(0, 1)):
        """Get a single result

            table = (str) table_name
            fields = (field1, field2 ...) list of fields to select
            where = ("parameterizedstatement", [parameters])
                    eg: ("id=%s and name=%s", [1, "test"])
            order = [field, ASC|DESC]
            limit = [from, to]
        """

        cur = self._select(table, fields, where, order, limit)
        result = cur.fetchone()

        row = None
        if result:
            fields = [f[0] for f in cur.description]
            row = zip(fields, result)

        return dict(row)

    def getAll(self, table=None, fields='*', where=None, order=None, limit=None):
        """Get all results

            table = (str) table_name
            fields = (field1, field2 ...) list of fields to select
            where = ("parameterizedstatement", [parameters])
                    eg: ("id=%s and name=%s", [1, "test"])
            order = [field, ASC|DESC]
            limit = [from, to]
        """

        cur = self._select(table, fields, where, order, limit)
        result = cur.fetchall()

        rows = None
        if result:
            fields = [f[0] for f in cur.description]
            rows = [dict(zip(fields, r)) for r in result]

        return rows
    


    def lastId(self):
        """Get the last insert id"""
        return self.cur.lastrowid

    def lastQuery(self):
        """Get the last executed query"""
        try:
            return self.cur.statement
        except AttributeError:
            return self.cur._last_executed

    def leftJoin(self, tables=(), fields=(), join_fields=(), where=None, order=None, limit=None):
        """Run an inner left join query

            tables = (table1, table2)
            fields = ([fields from table1], [fields from table 2])  # fields to select
            join_fields = (field1, field2)  # fields to join. field1 belongs to table1 and field2 belongs to table 2
            where = ("parameterizedstatement", [parameters])
                    eg: ("id=%s and name=%s", [1, "test"])
            order = [field, ASC|DESC]
            limit = [limit1, limit2]
        """

        cur = self._select_join(tables, fields, join_fields, where, order, limit)
        result = cur.fetchall()

        rows = None
        if result:
            Row = namedtuple("Row", [f[0] for f in cur.description])
            rows = [Row(*r) for r in result]

        return rows

    def insert(self, table, data):
        """Insert a record"""

        query = self._serialize_insert(data)

        sql = "INSERT INTO %s (%s) VALUES(%s)" % (table, query[0], query[1])

        return self.query(sql, tuple(data.values())).rowcount

    def insertBatch(self, table, data):
        """Insert multiple record"""

        query = self._serialize_batch_insert(data)

        sql = "INSERT INTO %s (%s) VALUES %s" % (table, query[0], query[1])
        flattened_values = [v for sublist in data for k, v in iter(sublist.items())]

        return self.query(sql, flattened_values).rowcount

    def update(self, table, data, where=None):
        """Insert a record"""

        query = self._serialize_update(data)

        sql = "UPDATE %s SET %s" % (table, query)

        if where and len(where) > 0:
            sql += " WHERE %s" % where[0]

        values = tuple(data.values())

        return self.query(
            sql, values + where[1] if where and len(where) > 1 else values
        ).rowcount

    def insertOrUpdate(self, table, data, key_field):
        insert_data = data.copy()

        data = {k: data[k] for k in data if k not in key_field}

        insert = self._serialize_insert(insert_data)
        update = self._serialize_update(data)

        sql = "INSERT INTO %s (%s) VALUES(%s) ON DUPLICATE KEY UPDATE %s" % (table, insert[0], insert[1], update)

        return self.query(sql, tuple(insert_data.values()) + tuple(data.values())).rowcount
    
    def describe(self, table: str):

        sql = "EXPLAIN "+ table

        cursor = self.query(sql).fetchall()

        return {field[0] : {"Field" : field[0],
                "Type" : field[1].decode().upper(),
                "Null" : field[2],
                "Key" : field[3],
                "Default" : field[4],
                "Extra" : field[5]} for field in cursor}




    def delete(self, table, where=None):
        """Delete rows based on a where condition"""

        sql = "DELETE FROM %s" % table

        if where and len(where) > 0:
            sql += " WHERE %s" % where[0]

        return self.query(sql, where[1] if where and len(where) > 1 else None).rowcount

    def addIndex(self, table, index_name, fields=[]):
        sanitized_fields = ','.join(fields)
        sql = 'ALTER TABLE %s ADD INDEX %s (%s)' % (table, index_name, sanitized_fields)

        return self.query(sql)

    def dropIndex(self, table_name, index_name):
        sql = 'ALTER TABLE %s DROP INDEX %s' % (table_name, index_name)

        return self.query(sql)

    def query(self, sql, params=None):
        """Run a raw query"""

        # check if connection is alive. if not, reconnect

        try:
            self.cur.execute(sql, params)
        except mysql.OperationalError as e:
            # mysql timed out. reconnect and retry once
            if e[0] == 2006:
                self.connect()
                self.cur.execute(sql, params)
            else:
                raise
        except:
            print("Query failed")
            raise

        return self.cur

    def commit(self):
        """Commit a transaction (transactional engines like InnoDB require this)"""
        return self.conn.commit()

    def is_open(self):
        """Check if the connection is open"""
        return self.conn.open

    def end(self):
        """Kill the connection"""
        self.cur.close()
        self.conn.close()

        # ===

    def _table_exist(self, table : str):
        sql = "SHOW TABLES LIKE '{0}'".format(table)
        self.cur.execute(sql)
        if self.cur.fetchone():
            return True
        return False
    



    def _serialize_insert(self, data):
        """Format insert dict values into strings"""
        keys = ",".join(data.keys())
        vals = ",".join(["%s" for k in data])

        return [keys, vals]

    def _serialize_batch_insert(self, data):
        """Format insert dict values into strings"""

        keys = ",".join(data[0].keys())
        v = "(%s)" % ",".join(tuple("%s".rstrip(',') for v in range(len(data[0]))))
        l = ','.join(list(repeat(v, len(data))))

        return [keys, l]

    def _serialize_update(self, data):
        """Format update dict values into string"""
        return "=%s,".join(data.keys()) + "=%s"

    def _select(self, table=None, fields=(), where=None, order=None, limit=None):
        """Run a select query"""

        sql = "SELECT %s FROM `%s`" % (",".join(fields), table)

        # where conditions
        if where and len(where) > 0:
            sql += " WHERE %s" % where[0]

        # order
        if order:
            sql += " ORDER BY %s" % order[0]

            if len(order) > 1:
                sql += " %s" % order[1]

        # limit
        if limit:
            sql += " LIMIT %s" % limit[0]

            if len(limit) > 1:
                sql += ", %s" % limit[1]

        return self.query(sql, where[1] if where and len(where) > 1 else None)

    def _select_join(self, tables=(), fields=(), join_fields=(), where=None, order=None, limit=None):
        """Run an inner left join query"""

        fields = [tables[0] + "." + f for f in fields[0]] + \
                 [tables[1] + "." + f for f in fields[1]]

        sql = "SELECT %s FROM %s LEFT JOIN %s ON (%s = %s)" % \
              (",".join(fields),
               tables[0],
               tables[1],
               tables[0] + "." + join_fields[0],
               tables[1] + "." + join_fields[1]
               )

        # where conditions
        if where and len(where) > 0:
            sql += " WHERE %s" % where[0]

        # order
        if order:
            sql += " ORDER BY %s" % order[0]

            if len(order) > 1:
                sql += " " + order[1]

        # limit
        if limit:
            sql += " LIMIT %s" % limit[0]

            if len(limit) > 1:
                sql += ", %s" % limit[1]

        return self.query(sql, where[1] if where and len(where) > 1 else None)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.end()

# PANDAS METHODS

    
    def _map_dtype(self, _dtype : str):
        try : 
            return next([key for key, value in DTypeDict.items() if str(_dtype).lower() in value])
        except:
            return "VARCHAR"   

    def _is_json(self, _varchar : str):
        if not _varchar.startswith(("{", "[")):
            return False
        try:
            json.loads(_varchar)
        except ValueError as e:
            return False
        return True
    
    def _column_max_length(self, _column):
        return _column.str.len().max()

    def _column_max_decimals(self, _column):
        return _column.astype('str').str.split('.', expand=True).apply(lambda x:len(x)).max()
    
      
    def _serialize_datatypes(self, data : pd.DataFrame, key_field : str = None):
        key_flag = False
        datatypes = []

        for items, dtype in zip(data.items(), list(data.dtypes)):
            key = items[0]
            column = items[1]
            datatype = getDataTypefromDType(str(dtype))
            if datatype == "VARCHAR" and self._is_json(column[column.first_valid_index()]):
                datatype = "JSON"
            if datatype in DataTypeLength.keys():
                datatype += "(%s)" % (DataTypeLength[datatype])
            if not key_flag and key == key_field:
                key_field = True
                datatype += " NOT NULL PRIMARY KEY AUTO_INCREMENT"
            else:
                datatype += " NULL"
            
            datatypes.append(datatype)
        
        return datatypes

    """
    * createTable() - creates a Table using a DataFrame as the input
    * syncColumns() - adds missing columns and changes mismatched column types.
    * insertTable() - insert data in a Table using a DataFrame as the input, optionally adds missing columns and changes mismatched column types.
    updateTable() - updates a Table using a DataFrame as the input, optionally updates the columns.
    createOrInsertTable() - creates a Table if it doesn´t exists, insert data if it does, optionally updates the columns
    createOrUpdateTable() - creates a Table if it doesn´t exists, updates the records if it does, optionally updates the columns
    getTable() - get all rows, return as DataTrame   
    """
    
    def getTable(self, table=None, fields='*', where=None, order=None, limit=None) -> pd.DataFrame:
        """Get all results and return as a DataFrame

            table = (str) table_name
            fields = (field1, field2 ...) list of fields to select
            where = ("parameterizedstatement", [parameters])
                    eg: ("id=%s and name=%s", [1, "test"])
            order = [field, ASC|DESC]
            limit = [from, to]
        """
        cur = self._select(table, fields, where, order, limit)
        column_names = cur.column_names

        records = [dict(zip(column_names, record)) for record in cur.fetchall()]

        res_dataFrame = pd.DataFrame.from_dict(records)

        # dealing with Timestamps
        for column in res_dataFrame.select_dtypes(include = [np.datetime64, 'datetime' , 'datetime64']):
            pd.to_datetime(res_dataFrame[column], format = "%Y/%m/%d")
            res_dataFrame[column].fillna(pd.Timedelta(days=0))

        return res_dataFrame
    
    def createTable(self, table, data : pd.DataFrame, key_field : str = None):
        # check if table exists
        if self._table_exist(table):
            print("table {0} already exists in the database".format(table))
            return 

        # extract datatypes from pandas <- how to understand that some columns might be jsons?
        # map keys and datatypes to mysql datatypes
        keys = [setMySqlFieldName(key)  for key in  data.keys()]
        datatypes = self._serialize_datatypes(data, key_field)

        # add an id field if the key_field parameter is empty
        if not key_field or not len(key_field) > 0:
            keys = ["id "] + keys
            datatypes = ["INT NOT NULL PRIMARY KEY AUTO_INCREMENT"] + datatypes

        # serialize data from dataframe
        sql = "CREATE TABLE {0} ({1})".format(table, ",".join([" ".join((key, datatype)) for key, datatype in zip(keys, datatypes)]))

        # create table
        return self.query(sql)
    
    def syncColumns(self, table, data : pd.DataFrame, key_field : str = None):
        # todo: check for primary keys, and sync primary keys. 
         
        keys = data.keys()
        datatypes = self._serialize_datatypes(data)

        source_columns = { setMySqlFieldName(key) : 
                     {"Field" : setMySqlFieldName(key),
                      "Type" : str(datatype).split()[0],
                       "Null" : "NO" if key == key_field else "YES",
                        "Key" :  "PRI" if key == key_field else "",
                        "Default" : None,
                        "Extra" : ""}
                        for key, datatype in zip(keys, datatypes) }
        
        dest_columns = self.describe(table)

        sql = "ALTER TABLE {0} ".format(table)

        missing_keys = list(set(source_columns.keys()) - set(dest_columns.keys()))
        mismatched_fields = [key for key 
                             in list(set(dest_columns.keys()).intersection(set(source_columns.keys() )))
                             if source_columns[key]["Type"] != dest_columns[key]["Type"]]

        if not len(missing_keys) > 0 and not len(mismatched_fields) > 0:
            print("all fields are already included in the destination, and all datatypes match")
            return

        """         # check if all fields are included and with the same settings. 
        if all(field in dest_columns for field in source_columns):
            print("all columns in the source data are in the destination") """

      
        # adds missing keys
        if len(missing_keys) > 0:
            print("destination is missing these fields {0}".format(missing_keys))
            sql += " , ".join(["ADD COLUMN {0} {1} NULL".format(key, 
                                                           source_columns[key]["Type"],                                                           ) for key in missing_keys])
        else:
            print("all fields are already defined in the destination")
                
        # changes mismatched datatypes
        if len(mismatched_fields) > 0:
            print("changing data types in the destination: {0}".format( "\n".join(
                ["Field name: {0} from type {1} to type {2}".format(
            key, dest_columns[key]["Type"], source_columns[key]["Type"]) for key in mismatched_fields]
            ))) 
            sql += " , ".join(["CHANGE COLUMN {0} {0} {1} NULL".format(key, source_columns[key]["Type"]) for key in mismatched_fields])
        else:
            print("all fields have matching types")

        return self.query(sql)


    def insertDataFrame(self, table, data : pd.DataFrame, updateColumns : bool = False):
        if updateColumns:
            self.syncColumns(table, data)
        data = data.replace(np.nan, None)
        records = data.to_dict(orient='records')

        return self.insertBatch(table, records)