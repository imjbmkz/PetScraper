import json
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

SHOP = "LilysKitchen"
BASE_URL = "https://www.lilyskitchen.co.uk"

class LilysKitchenETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "LilysKitchen"
        self.BASE_URL = "https://www.lilyskitchen.co.uk"
        self.CATEGORIES = ["/for-dogs/all-dog-food-recipes", "/for-cats/all-cat-food-recipes"]
    
    def transform(self, soup: BeautifulSoup, url: str):
        
        if soup:
            script_data = None

            # Check which script tag holds the product data
            script_tags = soup.find_all("script")
            for script_tag in script_tags:
                script_tag_content = script_tag.text.strip() 
                if script_tag.text.startswith("pageContext = {"):
                    script_data = script_tag_content.replace("pageContext = ", "")
                    script_data = script_data[:-1] # remove semicolon in the last character
                    break

            # Parse the data into dataframe
            if script_data:
                # Parse product data
                product_data = json.loads(script_data)["analytics"]["product"]
                if isinstance(product_data, list):
                    df = pd.DataFrame(product_data)
                else:
                    df = pd.DataFrame([product_data])

                # Parse product rating
                rating = json.loads(soup.select("script[type*='application/ld+json']")[1].text)
                if "aggregateRating" in rating.keys():
                    rating_value = rating["aggregateRating"]["ratingValue"]
                    rating_value = f"{rating_value} out of 5"
                else:
                    rating_value = None
                df["rating"] = rating_value

                # Reformat dataframe
                df = df[["name", "rating", "description", "url", "unit_price", "unit_sale_price"]].copy()
                df.rename({"unit_price": "price", "unit_sale_price": "discounted_price"}, axis=1, inplace=True)
                
                # Additional columns
                if df["price"].values[0]:
                    df["discount_percentage"] = (df["price"] - df["discounted_price"]) / df["price"]
                df["shop"] = self.SHOP

                return df

    def get_links(self, category: str) -> pd.DataFrame:
        # Data validation on category
        cleaned_category = category.lower()
        if cleaned_category not in self.CATEGORIES:
            raise ValueError(f"Invalid category. Value must be in {self.CATEGORIES}")
        
        # Construct link
        category_link = f"{self.BASE_URL}{category}"

        # Parse request response 
        soup = self.extract_from_url("GET", category_link)
        if soup:

            script_data = None

            # Check which script tag holds the product data
            script_tags = soup.find_all("script")
            for script_tag in script_tags:
                script_tag_content = script_tag.text.strip() 
                if script_tag.text.startswith("pageContext = {"):
                    script_data = script_tag_content.replace("pageContext = ", "")
                    script_data = script_data[:-1] # remove semicolon in the last character
                    break

            # Parse the data into dataframe
            if script_data:
                product_data = json.loads(script_data)
                product_lists = product_data["analytics"]["listing"]["items"]
                df = pd.DataFrame(product_lists)[["url"]]
                df["url"] = self.BASE_URL + df["url"]
                df["shop"] = self.SHOP

                return df

    # def run(self, db_conn: Engine, table_name: str):
    #     pass
