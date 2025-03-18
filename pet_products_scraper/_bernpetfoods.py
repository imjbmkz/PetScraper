import re
import json
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

class BernPetFoodsETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "BernPetFoods"
        self.BASE_URL = "https://www.bernpetfoods.co.uk"
        self.CATEGORIES = [
            "/product-category/dog-food/",
            "/product-category/cat-food/",
            "/product-category/cat-litter/",
        ]
    
    def transform(self, soup: BeautifulSoup, url: str):
        pass

    def get_links(self, category: str) -> pd.DataFrame:
        if category not in self.CATEGORIES:
            raise ValueError(f"Invalid category. Value must be in {self.CATEGORIES}")
        
        # Construct link
        category_link = f"{self.BASE_URL}{category}"

        urls = []
        page = 1

        while True:

            if page==1:
                current_url = category_link
            else:
                current_url = f"{category_link}/page/{page}"

            # Parse request response 
            soup = self.extract_from_url("GET", current_url)
            if soup:

                # Get all product links
                product_cards = soup.find_all("div", class_="ftc-product")
                product_links = [product_card.find("a")["href"] for product_card in product_cards]
                urls.extend(product_links)

                page += 1
                continue

            break

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df