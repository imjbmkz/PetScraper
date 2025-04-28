import requests
import random
import math
import re
import json
import pandas as pd
from datetime import datetime as dt
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_random

from .utils import execute_query, update_url_scrape_status, get_sql_from_file


from fake_useragent import UserAgent

import asyncio
import nest_asyncio
from playwright.async_api import async_playwright
nest_asyncio.apply()

MAX_RETRIES = 10
MAX_WAIT_BETWEEN_REQ = 1
MIN_WAIT_BETWEEN_REQ = 0.5
REQUEST_TIMEOUT = 30


class HarringtonsETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "Harringtons"
        self.BASE_URL = "https://www.harringtonspetfood.com"
        self.CATEGORIES = ["/collections/harringtons-dog-food",
                           "/collections/harringtons-cat-food"]

    @retry(
        wait=wait_random(min=MIN_WAIT_BETWEEN_REQ,
                         max=MAX_WAIT_BETWEEN_REQ),
        stop=stop_after_attempt(MAX_RETRIES),
        retry=retry_if_exception_type(requests.RequestException),
        before_sleep=before_sleep_log(logger, "WARNING"),
        reraise=True,
    )
    async def extract_scrape_content(self, url, selector):
        soup = None
        browser = None
        try:
            async with async_playwright() as p:
                browser_args = {
                    "headless": True,
                    "args": ["--disable-blink-features=AutomationControlled"]
                }

                browser = await p.chromium.launch(**browser_args)
                context = await browser.new_context(
                    user_agent=UserAgent().random,
                    viewport={"width": random.randint(
                        1200, 1600), "height": random.randint(800, 1200)},
                    locale="en-US"
                )

                page = await context.new_page()

                await page.set_extra_http_headers({
                    "User-Agent": UserAgent().random,
                    "Accept-Language": "en-US,en;q=0.9",
                    "Origin": "https://www.harringtonspetfood.com",
                    "Referer": url,
                    "Request-Origin": "gi"
                })

                await page.goto(url, wait_until="domcontentloaded")
                await page.wait_for_selector(selector, timeout=30000)

                for _ in range(random.randint(3, 6)):
                    await page.mouse.wheel(0, random.randint(300, 700))
                    await asyncio.sleep(random.uniform(0.5, 1))

                for _ in range(random.randint(5, 10)):
                    await page.mouse.move(random.randint(0, 800), random.randint(0, 600))
                    await asyncio.sleep(random.uniform(0.5, 1))

                rendered_html = await page.content()
                logger.info(
                    f"Successfully extracted data from {url}"
                )
                sleep_time = random.uniform(
                    MIN_WAIT_BETWEEN_REQ, MAX_WAIT_BETWEEN_REQ)
                logger.info(f"Sleeping for {sleep_time} seconds...")
                return BeautifulSoup(rendered_html, "html.parser")

        except Exception as e:
            logger.error(f"An error occurred: {e}")

        finally:
            if browser:
                await browser.close()

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find(
                'h1', class_="header-product__heading").get_text()

            product_description = None

            if soup.find('div', class_="panel-product-description__single-content"):
                product_description = soup.find(
                    'div', class_="panel-product-description__single-content").get_text()
            else:
                product_description = soup.find(
                    'div', class_="panel-product-description__copy").get_text()

            product_url = url.replace(self.BASE_URL, "")
            product_rating = re.sub(r'[^\d.]', '', soup.find(
                'div', class_="okeReviews-reviewsSummary-starRating").find('span', class_="okeReviews-a11yText").get_text()) + '/5'

            variants = [None]
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            price_container = soup.find('div', class_="price__container")
            original_price = None
            sale_price = None
            discount = None

            savings_elem = price_container.select_one(
                ".sale-item-savings-amount")

            if savings_elem:
                savings_text = re.sub(
                    r'[^\d.-]', '', savings_elem.text.strip())
                try:
                    savings_value = float(savings_text) if savings_text else 0
                except ValueError:
                    savings_value = 0

                has_discount = savings_value > 0
            else:
                has_discount = False

            if has_discount:

                original_price_elem = price_container.select_one(
                    ".sale-compare-amounts s.price-item--regular")
                original_price = original_price_elem.text.strip().replace(
                    "RRP:", "").strip() if original_price_elem else None

                sale_price_elem = price_container.select_one(
                    ".price__sale .price-item--sale")
                sale_price = sale_price_elem.contents[0].strip().replace(
                    '£', '') if sale_price_elem else None

                discount_elem = price_container.select_one(
                    ".sale-item-discount-amount")
                discount = discount_elem.text.strip().replace(
                    '% off', '') if discount_elem else None
                discount = int(discount)/100

            else:
                no_discount_price_elem = price_container.select_one(
                    ".price__regular .price-item--regular")
                original_price = no_discount_price_elem.text.strip().replace(
                    "RRP", "").strip() if no_discount_price_elem else None

            prices.append(original_price.replace('£', ''))
            discounted_prices.append(sale_price)
            discount_percentages.append(discount)
            image_urls.append(
                soup.find('meta', attrs={'property': "og:image"}).get('content'))

            df = pd.DataFrame({
                "variant": variants,
                "price": prices,
                "discounted_price": discounted_prices,
                "discount_percentage": discount_percentages,
                "image_urls": image_urls
            })
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

        # Construct link
        category_link = f"{self.BASE_URL}{category}"

        urls = []
        soup = asyncio.run(self.extract_scrape_content(
            category_link, '#MainContent'))

        n_product = int(soup.find(
            'span', class_="boost-pfs-filter-total-product").find(string=True, recursive=False))
        pagination_length = math.ceil(n_product / 24)

        for i in range(1, pagination_length + 1):
            soup_pagination = asyncio.run(
                self.extract_scrape_content(f"{category_link}?page={i}", '#MainContent'))
            for prod_list in soup_pagination.find_all('li', class_="list-product-card__item"):
                urls.append(self.BASE_URL + prod_list.find('a',
                            class_="card-product__heading-link").get('href').replace('#', ''))

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df

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

            soup = asyncio.run(
                self.extract_scrape_content(url, "#MainContent"))
            df = self.transform(soup, url)

            if df is not None:
                self.load(df, db_conn, table_name)
                update_url_scrape_status(db_conn, pkey, "DONE", now)
            else:
                update_url_scrape_status(db_conn, pkey, "FAILED", now)

    def image_scrape_product(self, url):
        soup = asyncio.run(self.extract_scrape_content(url, '#MainContent'))

        return {
            'shop': self.SHOP,
            'url': url,
            'image_urls': soup.find('meta', attrs={'property': "og:image"}).get('content')
        }
