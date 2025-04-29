"""
Step 1: Make sure that you have a MySQL database running. Update the .env file as needed.
Step 2: Activate virtual environment. 
Step 3: Run get_links task: `python main.py get_links -s Zooplus`
Step 3: Run scrape task: `python main.py scrape -s Zooplus`
"""


import os
import sys
import argparse
import datetime as dt
from loguru import logger
from dotenv import load_dotenv
from pet_products_scraper import utils
from pet_products_scraper import (
    PetProductsETL,
    ZooplusETL,
    PetsAtHomeETL,
    JollyesETL,
    LilysKitchenETL,
    BitibaETL,
    PetSupermarketETL,
    PetPlanetETL,
    PurinaETL,
    DirectVetETL,
    FishKeeperETL,
    PetDrugsOnlineETL,
    ViovetETL,
    PetShopETL,
    VetShopETL,
    VetUKETL,
    BurnsPetETL,
    AsdaETL,
    TheRangeETL,
    OcadoETL,
    HarringtonsETL,
    BernPetFoodsETL,
    PetsCornerETL,
    OrijenETL,
    ThePetExpressETL,
    PetShopOnlineETL,
    TaylorPetFoodsETL,
    TheNaturalPetStoreETL,
    HealthyPetStoreETL,
    FarmAndPetPlaceETL,
    NaturesMenuETL,

    PetImage
)


SHOPS = [
    # "Zooplus",
    "PetsAtHome",
    "Jollyes",
    "LilysKitchen",
    "Bitiba",
    "PetSupermarket",
    "PetPlanet",
    "Purina",
    "DirectVet",
    "FishKeeper",
    "PetDrugsOnline",
    "Viovet",
    "PetShop",
    "VetShop",
    "VetUK",
    "BurnsPet",
    "ASDAGroceries",
    "TheRange",
    "Ocado",
    "Harringtons",
    "BernPetFoods",
    "PetsCorner",
    "Orijen",
    "ThePetExpress",
    "PetShopOnline",
    "TaylorPetFoods",
    "TheNaturalPetStore",
    "HealthyPetStore",
    "FarmAndPetPlace",
    "NaturesMenu",
]

PROGRAM_NAME = "Pet Products Scraper"


def run_etl(shop: str) -> PetProductsETL:
    factory = {
        "Zooplus": ZooplusETL(),  # Done WIP on how to bypass completly CloudFront
        "PetsAtHome": PetsAtHomeETL(),  # Done
        "Jollyes": JollyesETL(),  # Done
        "LilysKitchen": LilysKitchenETL(),  # Done
        "Bitiba": BitibaETL(),  # Done But WIP on how to bypass completly CloudFront
        "PetSupermarket": PetSupermarketETL(),  # Done
        "PetPlanet": PetPlanetETL(),  # Done
        "Purina": PurinaETL(),  # Done
        "DirectVet": DirectVetETL(),  # Done
        "FishKeeper": FishKeeperETL(),  # Done
        "PetDrugsOnline": PetDrugsOnlineETL(),  # Done
        "Viovet": ViovetETL(),  # Done
        "PetShop": PetShopETL(),  # Done
        "VetShop": VetShopETL(),  # Done
        "VetUK": VetUKETL(),  # Done
        "BurnsPet": BurnsPetETL(),  # Done
        "ASDAGroceries": AsdaETL(),  # Done
        "TheRange": TheRangeETL(),  # Done Scraping but Cloudfalre Problem
        "Ocado": OcadoETL(),  # Done
        "Harringtons": HarringtonsETL(),  # Done
        "BernPetFoods": BernPetFoodsETL(),  # Done
        "PetsCorner": PetsCornerETL(),  # Done
        "Orijen": OrijenETL(),  # Done
        "ThePetExpress": ThePetExpressETL(),  # Done
        "PetShopOnline": PetShopOnlineETL(),  # Done
        "TaylorPetFoods": TaylorPetFoodsETL(),  # Done
        "TheNaturalPetStore": TheNaturalPetStoreETL(),  # Done
        "HealthyPetStore": HealthyPetStoreETL(),  # Done
        "FarmAndPetPlace": FarmAndPetPlaceETL(),  # Done
        "NaturesMenu": NaturesMenuETL(),  # Done
    }

    if shop in factory:
        return factory[shop]
    else:
        raise ValueError(
            f"Shop {shop} is not supported. Please pass a valid shop.")


parser = argparse.ArgumentParser(
    prog=PROGRAM_NAME,
    description="Scrape product details from various pet shops."
)

parser.add_argument("task", choices=[
                    "get_links", "scrape", "get_image"], help="Identify the task to be executed. get_links=get links from registered shops; scrape=scrape products.")
parser.add_argument("-s", "--shop", choices=SHOPS,
                    help="Select a shop to scrape. Default: all shops.")
args = parser.parse_args()

if __name__ == "__main__":

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

    if task == "get_links":
        client = run_etl(shop)
        utils.execute_query(engine, "TRUNCATE TABLE stg_urls;")
        client.refresh_links(engine, "stg_urls")

        sql = utils.get_sql_from_file("insert_into_urls.sql")
        utils.execute_query(engine, sql)

    elif task == "scrape":
        client = run_etl(shop)
        utils.execute_query(engine, "TRUNCATE TABLE stg_pet_products;")
        client.run(engine, "stg_pet_products")

        sql = utils.get_sql_from_file("insert_into_pet_products.sql")
        utils.execute_query(engine, sql)

        sql = utils.get_sql_from_file("insert_into_pet_product_variants.sql")
        utils.execute_query(engine, sql)

        sql = utils.get_sql_from_file(
            "insert_into_pet_product_variant_prices.sql")
        utils.execute_query(engine, sql)

    elif task == "get_image":
        pi = PetImage('./csv/pet_product_variant_urls.csv')
        pi.extract(0.5, 1)  # Args (min_sec, max_sec)

    end_time = dt.datetime.now()
    duration = end_time - start_time
    logger.info(f"{PROGRAM_NAME} (shop={shop}) has ended. Elapsed: {duration}")
