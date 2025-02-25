import re
import json
import math
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

class BurnsPetETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "BurnsPet"
        self.BASE_URL = "https://burnspet.co.uk"
        self.CATEGORIES = ["/dog-food", "/cat-food"]

    def get_links(self, category: str) -> pd.DataFrame:
        # Data validation on category
        if category not in self.CATEGORIES:
            raise ValueError(f"Invalid category. Value must be in {self.CATEGORIES}")
        
        # Construct link
        category_link = f"{self.BASE_URL}{category}"

        urls = []
        page = 1

        while True:
            if page==1:
                category_link_page = category_link
            else:
                category_link_page = f"{category_link}/?paged={page}"

            # Parse request response 
            soup = self.extract_from_url("GET", category_link_page)
            if soup:
                links = soup.select("a[class*='home-productrange-slider-item __productlist']")
                links = [link["href"] for link in links]
                urls.extend(links)
                page += 1
            
            else:
                break

        if urls:
            df = pd.DataFrame({"url": urls})
            df.insert(0, "shop", self.SHOP)

            return df
        
    def transform(self, soup: BeautifulSoup, url: str):
        pass