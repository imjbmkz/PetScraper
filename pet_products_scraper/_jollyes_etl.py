import json
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

class JollyesETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "Jollyes"
        self.BASE_URL = "https://www.jollyes.co.uk"
        self.CATEGORIES = ["dog", "cat", "small-pet", "bird-wildlife", "fish", "reptile"]
    
    def transform(self, soup: BeautifulSoup, url: str) -> pd.DataFrame:
        try:

            data = json.loads(soup.select_one("section[class*='lazy-review-section']").select_one("script[type*='application']").text)

            # Get data from parsed JSON
            product_title = data["name"]
            description = data["description"]

            # Products sometimes have no ratings
            if "aggregateRating" in data.keys():
                rating = data["aggregateRating"]["ratingCount"]
            else:
                rating = None

            product_url = url.replace(self.BASE_URL, "")
            price = float(data["offers"]["price"])

            # Compile the data acquired into dataframe
            df = pd.DataFrame(
                {
                    "shop": "Jollyes",
                    "name": product_title,
                    "rating": rating,
                    "description": description,
                    "url": product_url,
                    "price": price,
                    # "variant": None, # each product collected url is a variant
                    # "discounted_price": None,
                    # "discount_percentage": None
                }, index=[0]
            )

            return df
        
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")

    def get_links(self, category: str) -> pd.DataFrame:
        # Data validation on category
        cleaned_category = category.lower()
        if cleaned_category not in self.CATEGORIES:
            raise ValueError(f"Invalid category. Value must be in {self.CATEGORIES}")
        
        # Parse the base url of the category
        category_link = f"{self.BASE_URL}/{cleaned_category}.html"
        soup = self.extract_from_url("GET", category_link)

        if soup:

            subcategory_links = []

            # Get the subcategory links from the left navigation menu
            ul_tags = soup.select("ul[class='second-category']")
            for ul_tag in ul_tags:
                links = ul_tag.select("a")
                for link in links:
                    subcategory_links.append(link["href"])

            urls = []
            start_index = 1

            for subcategory in subcategory_links:

                n = start_index

                while True:
                    # Parse link
                    url = f"{self.BASE_URL}{subcategory}?page={n}&perPage=100"
                    soup = self.extract_from_url("GET", url)

                    if soup:

                        # Get product tiles and get the href values 
                        product_tiles = soup.select("div[class*='product-tile']")
                        for product_tile in product_tiles:
                            urls.append(self.BASE_URL + product_tile.select_one("a")["href"])

                        # Check if this element is available
                        progress = soup.select_one("div[class*='progress-row w-100']")
                        if progress:
                            n += 1
                            continue

                        break

                    n += 1

            df = pd.DataFrame({"url": urls})
            df.insert(0, "shop", self.SHOP)

            return df

    def run(self, db_conn: Engine, table_name: str):
        sql = get_sql_from_file("select_unscraped_urls.sql")
        sql = sql.format(shop=self.SHOP)
        df_urls = self.extract_from_sql(db_conn, sql)

        for i, row in df_urls.iterrows():

            pkey = row["id"]
            url = row["url"]

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            soup = self.extract_from_url("GET", url)
            df = self.transform(soup, url)

            if df is not None:
                self.load(df, db_conn, table_name)
                update_url_scrape_status(db_conn, pkey, "DONE", now)

            else:
                update_url_scrape_status(db_conn, pkey, "FAILED", now)