import requests
import re
import json
import random
import cloudscraper
import pandas as pd

from datetime import datetime as dt
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from fake_useragent import UserAgent
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random,
)
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

MAX_RETRIES = 10
MAX_WAIT_BETWEEN_REQ = 2
MIN_WAIT_BETWEEN_REQ = 1
REQUEST_TIMEOUT = 30


class TheRangeETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "TheRange"
        self.BASE_URL = "https://www.therange.co.uk"
        self.CATEGORIES = [
            "/offers/category/pets/",
            "/pets/dogs/",
            "/pets/cats/",
            "/pets/pet-food/",
            "/pets/puppy/",
            "/pets/kittens/",
            "/pets/small-pets/",
            "/pets/reptiles/",
            "/pets/horses/",
            "/pets/birds/",
            "/pets/horses/",
            "/aquatics/aquarium-food/",
            "/aquatics/aquarium-gravel/",
            "/aquatics/aquarium-ornaments/",
            "/aquatics/aquarium-plants/",
            "/aquatics/fish-tank-accessories/",
            "/aquatics/fish-tanks-and-bowls/",
            "/pets/fish/aquarium-pumps/",
            "/pets/fish/aquarium-filters/",
            "/pets/fish/aquarium-heaters/",
            "/pets/fish/aquarium-treatments-and-fish-healthcare/",
            "/pets/fish/garden-ponds/",
            "/pets/pet-home-cleaning/",
            "/pets/pet-toys/",
            "/pets/pet-beds/",
            "/pets/pet-bowls/",
            "/pets/pet-grooming/",
            "/pets/pet-housing/",
            "/pets/pet-medicine/",
            "/pets/pet-ramps-and-stairs/",
            "/pets/pet-gates/",
            "/pets/high-visibility-pets/",
            "/pets/pet-memorials-and-keepsakes/",
            "/rewilding-project/",
            "/pets/pet-storage/",
            "/pets/pets-camping/",
            "/pets/pet-cooling/",
            "/pets/pet-brands/",
            "/best-sellers/pets/"
        ]

    def setup_cloudscraper(self):
        scraper = cloudscraper.create_scraper(
            browser={"browser": random.choice(["firefox", 'chrome']), "platform": "windows", "mobile": False})
        scraper.headers.update({"User-Agent": UserAgent().random})
        scraper.headers.update({"Referer": "https://www.therange.co.uk"})
        return scraper

    @retry(
        wait=wait_random(min=MIN_WAIT_BETWEEN_REQ, max=MAX_WAIT_BETWEEN_REQ),
        stop=stop_after_attempt(MAX_RETRIES),
        retry=retry_if_exception_type(
            (requests.RequestException, requests.HTTPError, requests.exceptions.HTTPError)),
        before_sleep=before_sleep_log(logger, "WARNING"),
        reraise=True,
    )
    def fetch_page(self, url: str) -> BeautifulSoup:
        try:
            logger.info(f"[INFO] Fetching: {url}")
            response = self.setup_cloudscraper().get(url, timeout=REQUEST_TIMEOUT)

            # ✅ Check Cloudflare Response
            if response.status_code == 403:
                logger.warning(
                    f"[WARNING] Cloudflare blocked the request! Retrying...")
                raise requests.RequestException(
                    "Cloudflare protection triggered.")

            response.raise_for_status()

            logger.info(
                f"Successfully extracted data from {url} {response.status_code}"
            )
            sleep_time = random.uniform(
                MIN_WAIT_BETWEEN_REQ, MAX_WAIT_BETWEEN_REQ)
            logger.info(f"Sleeping for {sleep_time} seconds...")
            return BeautifulSoup(response.content, "html.parser")

        except requests.RequestException as e:
            logger.error(f"[ERROR] Failed to fetch {url}: {e}")

    def transform(self, soup: BeautifulSoup, url: str) -> pd.DataFrame:
        try:
            product_name = soup.find('h1', id="product-dyn-title").get_text()
            product_description = soup.find(
                'p', id='product-dyn-desc').find(string=True)
            product_url = url.replace(self.BASE_URL, "")
            product_rating = "0/5"
            product_id = soup.find('input', id="product_id").get('value')
            clean_url = url.split('#')[0]

            review_container = self.setup_cloudscraper().get(
                f'{clean_url}?action=loadreviews&pid={product_id}&page=1', timeout=REQUEST_TIMEOUT)
            review_container.raise_for_status()
            if review_container.status_code == 200:
                product_rating_soup = BeautifulSoup(
                    review_container.content, "html.parser")

                if product_rating_soup.find('div', id="review-product-summary"):
                    product_rating = str(round((int(product_rating_soup.find('div', id="review-product-summary").findAll(
                        'div', class_="progress-bar")[0].get('aria-valuenow')) / 100) * 5, 2)) + '/5'

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []

            product_details = self.setup_cloudscraper().get(
                f'{clean_url}?json')
            product_details.raise_for_status()
            if product_details.status_code == 200:
                if len(product_details.json()['variant_arr']) > 1:
                    for var_details in product_details.json()['variant_arr']:
                        if " - " in var_details['name']:
                            variants.append(
                                var_details['name'].split(" - ")[1])

                        if var_details['price_was'] == None:
                            prices.append(var_details['price'] / 100)
                            discounted_prices.append(None)
                            discount_percentages.append(None)
                        else:
                            prices.append(var_details['price_was'] / 100)
                            discounted_prices.append(
                                var_details['price'] / 100)
                            discount_percentages.append(
                                var_details['price_was_percent'] / 100)

                else:
                    variants.append(None)
                    if product_details.json()['variant_arr'][0]['price_was'] == None:
                        prices.append(product_details.json()[
                                      'variant_arr'][0]['price'] / 100)
                        discounted_prices.append(None)
                        discount_percentages.append(None)
                    else:
                        prices.append(product_details.json()[
                                      'variant_arr'][0]['price_was'] / 100)
                        discounted_prices.append(product_details.json()[
                                                 'variant_arr'][0]['price'] / 100)
                        discount_percentages.append(product_details.json(
                        )['variant_arr'][0]['price_was_percent'] / 100)

            df = pd.DataFrame({"variant": variants, "price": prices,
                              "discounted_price": discounted_prices, "discount_percentage": discount_percentages})
            df.insert(0, "url", product_url)
            df.insert(0, "description", product_description)
            df.insert(0, "rating", product_rating)
            df.insert(0, "name", product_name)
            df.insert(0, "shop", self.SHOP)

            return df

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")

    def get_links(self, category: str) -> pd.DataFrame:
        if category not in self.CATEGORIES:
            raise ValueError(
                f"Invalid category. Value must be in {self.CATEGORIES}")

        try:
            category_link = f"{self.BASE_URL}{category}"
            urls = []
            soup = self.fetch_page(category_link)

            category_id = soup.find('div', id="root")['data-page-id']
            total_product = soup.find('div', id="root")['data-total-results']

            product_list = self.setup_cloudscraper().get(
                f'https://search.therange.co.uk/api/productlist?categoryId={category_id}&sort=relevance&limit={total_product}&filters=%7B"in_stock_f"%3A%5B"true"%5D%7D', timeout=REQUEST_TIMEOUT)
            product_list.raise_for_status()
            if product_list.status_code == 200:
                for url in product_list.json()['products']:
                    if url.get('variantPath') is not None:
                        urls.append(self.BASE_URL + '/' +
                                    url.get('variantPath'))

            df = pd.DataFrame({"url": urls})
            df.insert(0, "shop", self.SHOP)
            return df

        except (TypeError, KeyError):
            logger.error(
                f"Could not extract category details from {category_link}")
            return None

    def refresh_links(self, db_conn: Engine, table_name: str):
        execute_query(db_conn, f"TRUNCATE TABLE {table_name};")

        for category in self.CATEGORIES:
            df = self.get_links(category)
            if df is not None:
                self.load(df, db_conn, table_name)

        sql = get_sql_from_file("insert_into_urls.sql")
        execute_query(db_conn, sql)

    def run(self, db_conn: Engine, table_name: str):
        sql = get_sql_from_file("select_unscraped_urls.sql")
        sql = sql.format(shop=self.SHOP)
        df_urls = self.extract_from_sql(db_conn, sql)

        for i, row in df_urls.iterrows():

            pkey = row["id"]
            url = row["url"]

            now = dt.now().strftime("%Y-%m-%d %H:%M:%S")

            soup = self.fetch_page(url)
            df = self.transform(soup, url)

            if df is not None:
                self.load(df, db_conn, table_name)
                update_url_scrape_status(db_conn, pkey, "DONE", now)

            else:
                update_url_scrape_status(db_conn, pkey, "FAILED", now)
