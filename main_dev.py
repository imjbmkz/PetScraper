import os, sys
from loguru import logger
from dotenv import load_dotenv
from pet_products_scraper import utils
from pet_products_scraper import ZooplusETL

logger.remove()
logger.add("logs/std_out.log", rotation="10 MB", level="INFO")
logger.add("logs/std_err.log", rotation="10 MB", level="ERROR")
logger.add(sys.stdout, level="INFO")
logger.add(sys.stderr, level="ERROR")

load_dotenv()
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_DRIVER = os.getenv("MYSQL_DRIVER")

engine = utils.get_db_conn(
    drivername=MYSQL_DRIVER,
    username=MYSQL_USER,
    password=None,
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    database=MYSQL_DATABASE,
)

utils.execute_query(engine, "TRUNCATE TABLE stg_pet_products;")

with open("links/zooplus.txt", "r") as f:
    for line in f:
        link = line.strip()
        ZooplusETL().run("Zooplus", link, engine)

        break