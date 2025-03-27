import re
import json
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

class OrijenETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "Orijen"
        self.BASE_URL = "https://www.orijenpetfoods.co.uk"
        self.CATEGORIES = [
            "/product-category/dog-food/",
            "/product-category/dog-food/puppy-food/",
            "/product-category/cat-food/",
            "/product-category/cat-food/kitten-food/",
        ]
    
    def transform(self, soup: BeautifulSoup, url: str):
        pass

    def get_links(self, category: str) -> pd.DataFrame:
        if category not in self.CATEGORIES:
            raise ValueError(
                f"Invalid category. Value must be in {self.CATEGORIES}")
        
        url = self.BASE_URL+category
        soup = self.extract_from_url("GET", url)

        if soup:
            atags = soup.find_all("a", "product-item__bg")
            urls = [atag["href"] for atag in atags]

            df = pd.DataFrame({"url": urls})
            df.insert(0, "shop", self.SHOP)
            return df
