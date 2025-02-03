import time
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from selenium.webdriver.common.by import By

from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

SHOP = "PetPlanet"
BASE_URL = "https://www.petplanet.co.uk"
CATEGORIES = {
    "dog_food": "/d7/dog_food", 
    "dog_products": "/d2/dog_products", 
    "cat_food": "/d34/cat_food", 
    "cat_products": "/d3/cat_products", 
    "other_small_furries": "/d298/other_small_furries", 
    "pet_health": "/d2709/pet_health"
}

class PetPlanetETL(PetProductsETL):
    
    def transform(self, soup: BeautifulSoup, url: str):
        pass

    def get_links(self, category: str) -> pd.DataFrame:
        # Data validation on category
        cleaned_category = category.lower()
        if cleaned_category not in CATEGORIES.keys():
            raise ValueError(f"Invalid category. Value must be in {CATEGORIES}")
        
        path = CATEGORIES[cleaned_category]
        url = f"{BASE_URL}{path}"

        urls = []

        driver = self.extract_from_driver(url)

        while True:
            try:
                # Scroll to the bottom to make the button visible
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)

                # Click the Show More button and wait for some time
                show_more_button = driver.find_element(By.ID, "ContentPlaceHolder1_ctl00_Shop1_ProdMenu1_LoadMoreBtn1")
                show_more_button.click()
                time.sleep(5)

                # Get the available product links
                products = driver.find_elements(By.CSS_SELECTOR, "div[class='col product-card']")
                new_urls = [p.get_property("href") for p in products]
                urls.extend(new_urls)

            except:
                break
        
        df = pd.DataFrame({"url": urls})
        df.drop_duplicates(inplace=True)
        df.insert(0, "shop", SHOP)

        return df

    def run(self, db_conn: Engine, table_name: str):
        pass

    def refresh_links(self, db_conn: Engine, table_name: str):
        execute_query(db_conn, f"TRUNCATE TABLE {table_name};")

        for category in CATEGORIES:
            df = self.get_links(category)
            self.load(df, db_conn, table_name)

        sql = get_sql_from_file("insert_into_urls.sql")
        execute_query(db_conn, sql)