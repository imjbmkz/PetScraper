import pandas as pd
import undetected_chromedriver as uc
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from selenium.webdriver.common.by import By

from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

SHOP = "PetsAtHome"
BASE_URL = "https://www.petsathome.com"
CATEGORIES = ["dog", "cat", "small-animal", "fish", "reptile", "bird-and-wildlife"]

class PetsAtHomeETL(PetProductsETL):

    def transform(self, driver: uc.Chrome, url: str):
        
        # Placeholder for some values
        rating = None
        variants = []
        prices = []
        discounted_prices = []
        discount_percentages = []

        # Check if ratings are available
        ratings = driver.find_elements(By.CSS_SELECTOR, "[id*='reviews']")
        if ratings:
            ratings[0].click()
            driver.implicitly_wait(5)
            rating = driver.find_element(By.CSS_SELECTOR, "p[class*='rating-breakdown_rating']").text

        # Get basic details, such as title and description
        product_title = driver.find_element(By.CSS_SELECTOR, "[class*='preview_title-base-product']").text
        description = driver.find_element("css selector", "[class*='truncated-text_truncated']").find_element(By.TAG_NAME, "p").text

        # Get product variants
        product_variants = driver.find_elements(By.CSS_SELECTOR, "[class*='product-selector_pill']")
        for variant in product_variants:

            # Price of a variant will show up when you select the variant
            variant.click()
            variants.append(variant.text)

            # There are two prices that could be listed: one regular price, and one easy-repeat price 
            prices_listed = driver.find_elements(By.CSS_SELECTOR, "span[class*='purchase-type-selector_price__']")
            if prices_listed:

                # Get the regular price
                price = float(prices_listed[0].text.replace("£", ""))
                prices.append(price)
                
                # Check if easy-repeat prices are available
                if len(prices_listed)>1:
                    discounted_price = float(prices_listed[1].text.replace("£", ""))
                    discounted_prices.append(discounted_price)
                    discount_percentage = ((price - discounted_price) / price) 
                    discount_percentages.append(discount_percentage)

                else:
                    discounted_prices.append(None)
                    discount_percentages.append(None)

        # Compile the data acquired into dataframe
        df = pd.DataFrame(
            {
                "variant": variants, 
                "price": prices,
                "discounted_price": discounted_prices,
                "discount_percentage": discount_percentages
            }
        )
        df.insert(0, "url", url)
        df.insert(0, "description", description)
        df.insert(0, "rating", rating)
        df.insert(0, "name", product_title)

        return df

    def get_links(self, category: str) -> pd.DataFrame:
        # Data validation on category
        cleaned_category = category.lower()
        if cleaned_category not in CATEGORIES:
            raise ValueError(f"Invalid category. Value must be in {CATEGORIES}")
        
        urls = []

        path = f"/product/listing/{category}"

        while True:
        
            # Construct link
            url = BASE_URL + path

            # Parse request response 
            soup = self.extract_from_url(url)

            wrappers = soup.select('a[class*="product-tile_wrapper"]')
            new_urls = [BASE_URL+s["href"] for s in wrappers]
            urls.extend(new_urls)

            next_page = soup.select_one('a[class*="results-pagination_more"]')
            if next_page:
                path = next_page["href"]

            else:
                break

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", SHOP)

        return df

    def run(self, db_conn: Engine, table_name: str):
        sql = get_sql_from_file("select_unscraped_urls.sql")
        sql = sql.format(shop=SHOP)
        df_urls = self.extract_from_sql(db_conn, sql)

        for i, row in df_urls.iterrows():

            pkey = row["id"]
            url = row["url"]

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            driver = self.extract_from_driver(url)
            df = self.transform(driver, url)

            if df is not None:
                self.load(df, db_conn, table_name)
                df = None

            else:
                update_url_scrape_status(db_conn, pkey, "FAILED", now)
            
            update_url_scrape_status(db_conn, pkey, "DONE", now)

    def refresh_links(self, db_conn: Engine, table_name: str):
        
        execute_query(db_conn, f"TRUNCATE TABLE {table_name};")

        for category in CATEGORIES:
            df = self.get_links(SHOP, category)
            self.load(df, db_conn, table_name)

        sql = get_sql_from_file("insert_into_urls.sql")
        execute_query(db_conn, sql)