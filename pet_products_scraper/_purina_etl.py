import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

SHOP = "Purina"
BASE_URL = "https://www.purina.co.uk"

class PurinaETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "Purina"
        self.BASE_URL = "https://www.purina.co.uk"
        self.CATEGORIES = ["/dog/dog-food", "/cat/cat-food"]
    
    def transform(self, soup: BeautifulSoup, url: str):
        pass

    def get_links(self, category: str) -> pd.DataFrame:
        # Data validation on category
        cleaned_category = category.lower()
        if cleaned_category not in self.CATEGORIES:
            raise ValueError(f"Invalid category. Value must be in {self.CATEGORIES}")
        
        # Construct link
        category_link = f"{self.BASE_URL}{category}"
        
        # Placeholders for incrementing values
        current_url = category_link
        page = 0
        urls = []

        while True:
            print("Scraping", current_url)
            soup = self.extract_from_url("GET", current_url)

            if soup:
                new_urls = soup.select("a[class*='product-tile_image']")
                if new_urls:
                    new_urls = [u["href"] for u in new_urls]
                    new_urls = [self.BASE_URL+u for u in new_urls]
                    urls.extend(new_urls)

                    page += 1
                    current_url = f"{category_link}?page={page}"
                    continue

                break

            else:
                break

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)

        return df

    def run(self, db_conn: Engine, table_name: str):
        pass