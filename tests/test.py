import logging

logger = logging.getLogger(__name__)
logging.basicConfig(filename=__name__+'.log',  level=logging.INFO, format="%(asctime)s %(message)s", datefmt='%d/%m/%Y %I:%M:%S %p')

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
logger.info(testdb.createTable("testcreate" , data))

