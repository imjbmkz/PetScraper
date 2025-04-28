import json
import pandas as pd
import undetected_chromedriver as uc
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from selenium.webdriver.common.by import By

from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file


class PetsAtHomeETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "PetsAtHome"
        self.BASE_URL = "https://www.petsathome.com"
        self.CATEGORIES = ["dog", "cat", "small-animal",
                           "fish", "reptile", "bird-and-wildlife"]

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            # Get the data from encoded JSON
            product_data = soup.select_one("[id='__NEXT_DATA__']")
            product_data_dict = json.loads(product_data.text)

            # Get base details
            product_title = product_data_dict["props"]["pageProps"]["baseProduct"]["name"]
            rating = product_data_dict["props"]["pageProps"]["productRating"]
            rating = product_data_dict["props"]["pageProps"]["productRating"]
            if rating:
                rating = "{} out of 5".format(rating["averageRating"])
            else:
                rating = None
            description = product_data_dict["props"]["pageProps"]["baseProduct"]["description"]
            product_url = url.replace("https://www.petsathome.com", "")

            # Placeholder for variant details
            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            # Iterate through all product variants
            for variant in product_data_dict["props"]["pageProps"]["baseProduct"]["products"]:
                variants.append(variant["label"])

                price = variant["price"]["base"]
                discounted_price = variant["price"]["promotionBase"]

                prices.append(price)
                discounted_prices.append(discounted_price)

                if discounted_price:
                    discount_percentage = (price - discounted_price) / price
                else:
                    discount_percentage = None

                discount_percentages.append(discount_percentage)
                image_urls.append(', '.join(variant['imageUrls']))

            # Compile the data acquired into dataframe
            df = pd.DataFrame(
                {
                    "variant": variants,
                    "price": prices,
                    "discounted_price": discounted_prices,
                    "discount_percentage": discount_percentages,
                    "image_urls": image_urls
                }
            )
            df.insert(0, "url", product_url)
            df.insert(0, "description", description)
            df.insert(0, "rating", rating)
            df.insert(0, "name", product_title)
            df.insert(0, "shop", self.SHOP)

            return df

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")

    def get_links(self, category: str) -> pd.DataFrame:
        # Data validation on category
        cleaned_category = category.lower()
        if cleaned_category not in self.CATEGORIES:
            raise ValueError(
                f"Invalid category. Value must be in {self.CATEGORIES}")

        urls = []

        path = f"/product/listing/{category}"

        while True:

            # Construct link
            url = self.BASE_URL + path

            # Parse request response
            soup = self.extract_from_url("GET", url)

            wrappers = soup.select('a[class*="product-tile_wrapper"]')
            new_urls = [self.BASE_URL+s["href"] for s in wrappers]
            urls.extend(new_urls)

            next_page = soup.select_one('a[class*="results-pagination_more"]')
            if next_page:
                path = next_page["href"]

            else:
                break

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

    def image_scrape_product(self, url):
        soup = self.extract_from_url("GET", url)

        return {
            'shop': self.SHOP,
            'url': url,
            'image_urls': soup.find('meta', attrs={'property': "og:image"}).get('content')
        }
