import logging

logger = logging.getLogger(__name__)
logging.basicConfig(filename=__name__+".log",  level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")

from dotenv import load_dotenv
load_dotenv()

import pandas as pd

import os

DB_HOST = os.environ.get("DATABASE_SERVER")
DB_USER = os.environ.get("DATABASE_USER")
DB_PW = os.environ.get("DATABASE_PASSWORD")

from src.mysql_wrap import MysqlWrap as mysql
from src.mysql_wrap import getDataTypefromDType

db = mysql(host = DB_HOST,
           db = "henn_dashboard",
           user=DB_USER,
           passwd = DB_PW,
           keep_alive = True,)

data = db.getTable("projects_key_data")

#logger.info(data)

directus  = mysql(host = DB_HOST,
           db = "henn_directus",
           user=DB_USER,
           passwd = DB_PW,
           keep_alive = True,)

assemblies = directus.getTable("assemblies")

#logger.info(assemblies)

products = directus.getTable("products")

#logger.info(products)

testdb = mysql(host = DB_HOST,
           db = "test",
           user=DB_USER,
           passwd = DB_PW,
           keep_alive = True,)

logger.info(testdb.getTable("export_log"))

logger.info(testdb._table_exist("export_log"))

logger.info(testdb._table_exist("test"))

data = pd.read_csv("https://media.geeksforgeeks.org/wp-content/uploads/nba.csv") 
""" 
for dtype in list(data.dtypes):
    logger.info(str(dtype))
    logger.info(getDataTypefromDType(dtype))

 """
# logger.info(testdb.createTable("testcreate" , data))

#sql = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = "column_name" "

sql = "explain testcreate"

#columns = testdb.query(sql)
""" 
print(testdb)
print(testdb.cur)
print(testdb.cur.fetchall())
 """

""" 
print(testdb.describe("testcreate"))

print(testdb.syncColumns("testcreate", data)) """

inputdict = {"Name": "Avery Bradley", "Team": "Boston Celtics", "Number": 0.0, "Position": "PG", "Age": 25.0, "Height": "6-2", "Weight": 180.0, "College": "Texas", "Salary" : 7730337.0}

#keys = " , ".join([item for item in inputdict.keys()])
#values = " , ".join(["""+item+""" if isinstance(item, str) else str(item) for item in inputdict.values()])

#testdb.query("INSERT INTO {0} ({1}) VALUES ({2})".format("testcreate", keys, values))
#testdb.commit()

#testdb.insert("testcreate", inputdict)
#testdb.commit()

testdb.insertDataFrame("testcreate", data, True)
testdb.commit()