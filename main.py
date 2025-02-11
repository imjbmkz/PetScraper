import os, sys, argparse
import datetime as dt
from loguru import logger
from dotenv import load_dotenv
from pet_products_scraper import utils
from pet_products_scraper import (
    PetProductsETL, 
    ZooplusETL, 
    PetsAtHomeETL, 
    PetPlanetETL, 
    JollyesETL,
    LilysKitchenETL,
    
)

SHOPS = [
    "Zooplus", 
    "PetsAtHome", 
    # "PetPlanet",
    "Jollyes",
    "LilysKitchen",
]

PROGRAM_NAME = "Pet Products Scraper"

def run_etl(shop: str) -> PetProductsETL:
    factory = {
        "Zooplus": ZooplusETL(),
        "PetsAtHome": PetsAtHomeETL(),
        "PetPlanet": PetPlanetETL(),
        "Jollyes": JollyesETL(),
        "LilysKitchen": LilysKitchenETL(),
    }

    if shop in factory:
        return factory[shop]
    else:
        raise ValueError(f"Shop {shop} is not supported. Please pass a valid shop.")

parser = argparse.ArgumentParser(
    prog=PROGRAM_NAME,
    description="Scrape product details from various pet shops."
)

parser.add_argument("task", choices=["get_links", "scrape"], help="Identify the task to be executed. get_links=get links from registered shops; scrape=scrape products.")
parser.add_argument("-s", "--shop", choices=SHOPS, help="Select a shop to scrape. Default: all shops.")
args = parser.parse_args()

if __name__=="__main__":

    start_time = dt.datetime.now()

    logger.remove()
    logger.add("logs/std_out.log", rotation="10 MB", level="INFO")
    logger.add("logs/std_err.log", rotation="10 MB", level="ERROR")
    logger.add(sys.stdout, level="INFO")
    logger.add(sys.stderr, level="ERROR")

    logger.info(f"{PROGRAM_NAME} has started")

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

    task = args.task
    shop = args.shop
    client = run_etl(shop)

    if task=="get_links":
        utils.execute_query(engine, "TRUNCATE TABLE stg_urls;")
        client.refresh_links(engine, "stg_urls")

        sql = utils.get_sql_from_file("insert_into_urls.sql")
        utils.execute_query(engine, sql)

    elif task=="scrape":
        utils.execute_query(engine, "TRUNCATE TABLE stg_pet_products;")
        client.run(engine, "stg_pet_products")

        sql = utils.get_sql_from_file("insert_into_pet_products.sql")
        utils.execute_query(engine, sql)

        sql = utils.get_sql_from_file("insert_into_pet_product_variants.sql")
        utils.execute_query(engine, sql)

        sql = utils.get_sql_from_file("insert_into_pet_product_variant_prices.sql")
        utils.execute_query(engine, sql)

    end_time = dt.datetime.now()
    duration = end_time - start_time
    logger.info(f"{PROGRAM_NAME} has ended. Elapsed: {duration}")