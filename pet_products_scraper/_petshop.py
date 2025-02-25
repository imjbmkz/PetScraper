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

class PetShopETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "PetShop"
        self.BASE_URL = "https://www.petshop.co.uk"
        self.CATEGORIES = ["/Dog", "/Cat", "/Other-Pets", "/Other-Pets/Bird", "/Type/Own-Brand", "/Raw"]

    def get_links(self, category: str) -> pd.DataFrame:
        # Data validation on category
        if category not in self.CATEGORIES:
            raise ValueError(f"Invalid category. Value must be in {self.CATEGORIES}")
        
        # Construct link
        category_link = f"{self.BASE_URL}{category}"

        urls = []

        # Parse request response 
        soup = self.extract_from_url("GET", category_link)

        # Get the number of products and number of pages available
        n_products = int(soup.select_one("h1[class='facets-facet-browse-title']")["data-quantity"])
        n_products_per_page = 50
        n_pages = math.ceil(n_products / n_products_per_page) + 1

        for p in range(1, n_pages):
            if p==1:
                category_link_page = category_link
            else:
                category_link_page = f"{category_link}?page={p}"

            soup_page = self.extract_from_url("GET", category_link_page)
            if soup_page:
                product_links_a = soup_page.select("a[class='facets-item-cell-grid-link-image']")
                product_links = [self.BASE_URL + plink["href"] for plink in product_links_a]
                urls.extend(product_links)

        if urls:
            df = pd.DataFrame({"url": urls})
            df.insert(0, "shop", self.SHOP)

            return df
    
    def transform(self, soup: BeautifulSoup, url: str):
        pass