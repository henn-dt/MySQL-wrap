import logging

logger = logging.getLogger(__name__)
logging.basicConfig(filename=__name__+".log",  level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import numpy as np

import os

DB_HOST = os.environ.get("DATABASE_SERVER")
DB_USER = os.environ.get("DATABASE_USER")
DB_PW = os.environ.get("DATABASE_PASSWORD")

from src.mysql_wrap import MysqlWrap as mysql


testdb = mysql(host = DB_HOST,
           db = "test",
           user=DB_USER,
           passwd = DB_PW,
           keep_alive = True,)

logger.info(testdb.getTable("export_log"))

logger.info(testdb.tableExist("export_log"))

logger.info(testdb.tableExist("test"))

data = pd.read_csv("https://media.geeksforgeeks.org/wp-content/uploads/nba.csv") 
""" 
for dtype in list(data.dtypes):
    logger.info(str(dtype))
    logger.info(getDataTypefromDType(dtype))

 """

tablename = "testpandas"

testdb.createInsertTable(tablename, data)
testdb.commit()

data["Salary"] = 2000

testdb.insertOrUpdateDataFrame(tablename, data, "Name")
testdb.commit()


#testdb.createTable(tablename, data)

first_data = data.iloc[0:1, :]

print(first_data)

second_data = data.iloc[1:2, :]

print(second_data)

update_data = data.iloc[0:2, :]

update_data["Salary"] = 2000

print(update_data)

#testdb.insertOrUpdateDataFrame(tablename, update_data, "Name")

#testdb.commit()

""" 
testdb.insertDataFrame(tablename, first_data)


testdb.insertDataFrame(tablename, second_data)

#testdb.insertDataFrame(tablename, data)

 """