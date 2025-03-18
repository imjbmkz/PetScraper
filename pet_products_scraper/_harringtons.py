import re
import json
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

class HarringtonsETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "Harringtons"
        self.BASE_URL = "https://www.harringtonspetfood.com"
        self.CATEGORIES = ["/collections/harringtons-dog-food", "/collections/harringtons-cat-food"]
    
    def transform(self, soup: BeautifulSoup, url: str):
        pass

    def get_links(self, category: str) -> pd.DataFrame:
        if category not in self.CATEGORIES:
            raise ValueError(f"Invalid category. Value must be in {self.CATEGORIES}")
        
        # Construct link
        category_link = f"{self.BASE_URL}{category}"

        urls = []
        page = 1

        for page in range(1, 4): # need to update when there are new records

            if page==1:
                current_url = category_link
            else:
                current_url = f"{category_link}?page={page}"

            # Parse request response 
            soup = self.extract_from_url("GET", current_url)
            if soup:

                # Get all product links
                product_cards = soup.find_all("a", class_="card-product__img-link")
                product_links = [f"{self.BASE_URL}{product_card['href']}" for product_card in product_cards]
                urls.extend(product_links)

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df