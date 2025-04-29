import pandas as pd
import cloudscraper
import random
import requests
import math
from datetime import datetime as dt
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from loguru import logger
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random,
)
from .utils import execute_query, update_url_scrape_status, get_sql_from_file


CATEGORIES = ['/dogs/food',
              '/dogs/medication-supplements',
              '/dogs/training,toys,animal-wear,care-grooming,home-garden-travel,cleaning-hygiene-1',
              '/dogs/medication-supplements/prescription-medication',
              '/cats/food',
              '/cats/medication-supplements',
              '/cats/training,toys,animal-wear,care-grooming,home-garden-travel,cleaning-hygiene-1',
              '/cats/medication-supplements/prescription-medication',
              '/horses-ponies/medication-supplements/supplements-1',
              '/horses-ponies/animal-wear',
              '/horses-ponies/care-grooming',
              '/horses-ponies/medication-supplements/prescription-medication',
              '/saddlery',
              '/human/clothing-human/riding-wear',
              '/stable-yard-field-arena',
              '/horses-ponies/medication-supplements/non-prescription-medication-1/worm-control-1',
              '/small-animals/food',
              '/flp/small-animal-accessories',
              '/small-animals/medication-supplements',
              '/fish-1',
              '/reptiles/accessories',
              '/reptiles/food',
              '/reptiles/medication-supplements',
              '/birds/home-garden-travel/housing/cages-accessories',
              '/birds/food',
              '/birds/medication-supplements',
              '/medication-supplements/prescription-medication',
              '/farm-animals']


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]

MAX_RETRIES = 10
MAX_WAIT_BETWEEN_REQ = 2
MIN_WAIT_BETWEEN_REQ = 0
REQUEST_TIMEOUT = 30


class ViovetETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "Viovet"
        self.BASE_URL = "https://www.viovet.co.uk"
        self.CATEGORIES = CATEGORIES

    def setup_cloudscraper(self):
        scraper = cloudscraper.create_scraper(browser='chrome')
        random_user_agent = random.choice(USER_AGENTS)
        scraper.headers.update({"User-Agent": random_user_agent})
        scraper.headers.update({"Referer": "https://www.google.com"})
        return scraper

    @retry(
        wait=wait_random(min=MIN_WAIT_BETWEEN_REQ, max=MAX_WAIT_BETWEEN_REQ),
        stop=stop_after_attempt(MAX_RETRIES),
        retry=retry_if_exception_type(requests.RequestException),
        before_sleep=before_sleep_log(logger, "WARNING"),
        reraise=True,
    )
    def fetch_page(self, url: str) -> BeautifulSoup:
        try:
            logger.info(f"[INFO] Fetching: {url}")
            response = self.setup_cloudscraper().get(
                url, timeout=REQUEST_TIMEOUT)

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
            return BeautifulSoup(response.content, "html.parser")

        except requests.RequestException as e:
            logger.error(f"[ERROR] Failed to fetch {url}: {e}")
            # raise

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.select_one(
                'h1[id="product_family_heading"]').get_text()
            product_url = url.replace(self.BASE_URL, "")

            product_description_wrapper = soup.select_one(
                'div[itemprop="description"]').find('div').find_all('p')
            description_text = [para.get_text()
                                for para in product_description_wrapper]
            product_description = ' '.join(description_text)

            rating_average = ''
            if (soup.find('span', itemprop="ratingValue") == None):
                rating_average = '0/5'
            else:
                rating_average = soup.select_one(
                    'span[itemprop="ratingValue"]').get_text() + '/5'

            variants_wrapper = soup.find_all('li', 'product-select-item')

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            for variant in variants_wrapper:
                name_span = variant.find('span', 'name')
                clearance_label = name_span.find(
                    'span', 'clearance_product_label')
                if clearance_label:
                    clearance_label.extract()

                name_variant = name_span.get_text(strip=True)
                price_variant = float(variant.find(
                    'span', 'price').get_text(strip=True).replace("£", ""))

                variants.append(name_variant)
                prices.append(price_variant)
                discounted_prices.append(None)
                discount_percentages.append(None)
                image_urls.append(', '.join([
                    'https' + (src if src else data_src)
                    for img in soup.find_all('div', class_="swiper-slide")
                    if (img_tag := img.find('img')) and ((src := img_tag.get('src')) or (data_src := img_tag.get('data-src')))
                ]))

            df = pd.DataFrame({"variant": variants, "price": prices,
                               "discounted_price": discounted_prices, "discount_percentage": discount_percentages, "image_urls": image_urls})
            df.insert(0, "url", product_url)
            df.insert(0, "description", product_description)
            df.insert(0, "rating", rating_average)
            df.insert(0, "name", product_name)
            df.insert(0, "shop", self.SHOP)

            return df

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")

    def get_links(self, category: str) -> pd.DataFrame:
        cleaned_category = category.lower()
        if cleaned_category not in self.CATEGORIES:
            raise ValueError(
                f"Invalid category. Value must be in {self.CATEGORIES}")

        current_url = f"{self.BASE_URL}{category}"
        urls = []

        soup = self.fetch_page(current_url)

        pagination_length = 0
        product_number = int(soup.select_one('div[class*="products-area"]').find_all('div')[0].find_all(
            'span')[3].find('span').get_text().replace('Sort all ', '').replace(' product ranges by:', ''))
        if (product_number <= 36):
            page_url = f"{current_url}?page=1"
            page_pagination_source = self.fetch_page(page_url)
            product_list = page_pagination_source.select(
                'a[class*="ab_var_one grid-box _one-whole _no-padding _no-margin"][itemprop="url"]')

            for product in product_list:
                title_tag = product.find('h2', itemprop="name")
                if title_tag:
                    urls.append(self.BASE_URL + product.get('href'))

        else:
            pagination_length = math.ceil(product_number / 36)
            for i in range(1, pagination_length + 1):
                page_url = f"{current_url}?page={i}"
                page_pagination_source = self.fetch_page(page_url)
                product_list = page_pagination_source.select(
                    'a[class*="ab_var_one grid-box _one-whole _no-padding _no-margin"][itemprop="url"]')

                for product in product_list:
                    title_tag = product.find('h2', itemprop="name")
                    if title_tag:
                        urls.append(self.BASE_URL + product.get('href'))

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

            now = dt.now().strftime("%Y-%m-%d %H:%M:%S")

            soup = self.fetch_page(url)
            df = self.transform(soup, url)

            if df is not None:
                self.load(df, db_conn, table_name)
                update_url_scrape_status(db_conn, pkey, "DONE", now)

            else:
                update_url_scrape_status(db_conn, pkey, "FAILED", now)

    def refresh_links(self, db_conn: Engine, table_name: str):
        execute_query(db_conn, f"TRUNCATE TABLE {table_name};")

        for category in self.CATEGORIES:
            df = self.get_links(category)
            if df is not None:
                self.load(df, db_conn, table_name)

        sql = get_sql_from_file("insert_into_urls.sql")
        execute_query(db_conn, sql)

    def image_scrape_product(self, url):
        soup = self.fetch_page(url)

        return {
            'shop': self.SHOP,
            'url': url,
            'image_urls': soup.find('meta', attrs={'property': "og:image"}).get('content')
        }
