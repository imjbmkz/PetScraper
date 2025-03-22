import requests
import random
import time
import pandas as pd

from datetime import datetime as dt
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file
from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_random

from fake_useragent import UserAgent

import asyncio
import nest_asyncio
from playwright.async_api import async_playwright
nest_asyncio.apply()


MAX_RETRIES = 10
MAX_WAIT_BETWEEN_REQ = 1
MIN_WAIT_BETWEEN_REQ = 0.5
REQUEST_TIMEOUT = 30


class OcadoETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "Ocado"
        self.BASE_URL = "https://www.ocado.com"
        self.CATEGORIES = ["/browse/pets-home-garden-300818"]

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
                    "Origin": "https://www.ocado.com",
                    "Referer": url,
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
                soup = BeautifulSoup(rendered_html, "html.parser")
                return soup

        except Exception as e:
            logger.error(f"An error occurred: {e}")

        finally:
            if browser:
                await browser.close()

    async def product_list_scrolling(self, url, selector):
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
                    "Origin": "https://www.ocado.com",
                    "Referer": url,
                })

                await page.goto(url, wait_until="domcontentloaded")
                await page.wait_for_selector(selector, timeout=30000)

                logger.info(
                    "Starting to scrape the product list (Infinite scroll scrape)...")

                scroll_step = 300
                scroll_delay = 1

                current_position = 0
                page_height = await page.evaluate('() => document.body.scrollHeight')

                while current_position < page_height:
                    # Scroll to the current position
                    await page.evaluate(f'window.scrollTo(0, {current_position})')
                    current_position += scroll_step
                    time.sleep(scroll_delay)

                logger.info("Scraping complete. Extracting content...")

                rendered_html = await page.content()
                logger.info(
                    f"Successfully extracted data from {url}"
                )
                sleep_time = random.uniform(
                    MIN_WAIT_BETWEEN_REQ, MAX_WAIT_BETWEEN_REQ)
                logger.info(f"Sleeping for {sleep_time} seconds...")
                soup = BeautifulSoup(rendered_html, "html.parser")
                return soup.find('ul', class_="fops-regular")

        except Exception as e:
            logger.error(f"An error occurred: {e}")

        finally:
            if browser:
                await browser.close()

    def get_links(self, category: str) -> pd.DataFrame:
        if category not in self.CATEGORIES:
            raise ValueError(
                f"Invalid category. Value must be in {self.CATEGORIES}")

        category_link = f"{self.BASE_URL}{category}"
        soup = asyncio.run(self.extract_scrape_content(
            category_link, '.main-column'))
        n_product = int(soup.find('div', class_="main-column").find('div',
                        class_="total-product-number").find('span').get_text().replace(' products', ''))

        list_soup_product = asyncio.run(self.product_list_scrolling(
            f"{category_link}?display={n_product}", '.fops-regular'))
        product_list = [item for item in list_soup_product.find_all(
            'li', class_="fops-item") if 'fops-item--advert' not in item.get('class', [])]
        urls = [self.BASE_URL + product.find('a').get('href')
                for product in product_list if product.find('a')]

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find(
                'header', class_="bop-title").find('h1').get_text(strip=True)
            product_description = soup.find(
                'div', class_="gn-accordionElement__wrapper").find('div', class_="bop-info__content").get_text()
            product_url = url.replace(self.BASE_URL, "")
            product_rating = '0/5'

            product_rating_wrapper = soup.find('section', id='reviews').find(
                'span', attrs={'itemprop': 'ratingValue'})
            if product_rating_wrapper is not None:
                product_rating = product_rating_wrapper.get_text().strip() + '/5'

            variant = None
            price = None
            discounted_price = None
            discount_percentage = None

            if soup.find('header', class_="bop-title").find('span', class_="bop-catchWeight"):
                variant = soup.find('header', class_="bop-title").find('span',
                                                                       class_="bop-catchWeight").get_text(strip=True)

            if soup.find('span', class_="bop-price__old"):
                price = float(soup.find(
                    'span', class_="bop-price__old").get_text(strip=True).replace('£', ""))
                discounted_price = "{:.2f}".format(float(soup.find(
                    'h2', class_="bop-price__current").find('meta', attrs={'itemprop': 'price'}).get('content')))
                discount_percentage = round(
                    (price - float(discounted_price)) / price, 2)
            else:
                price = "{:.2f}".format(float(soup.find(
                    'h2', class_="bop-price__current").find('meta', attrs={'itemprop': 'price'}).get('content')))

            df = pd.DataFrame([{
                "url": product_url,
                "description": product_description,
                "rating": product_rating,
                "name": product_name,
                "shop": self.SHOP,
                "variant": variant,
                "price": price,
                "discounted_price": discounted_price,
                "discount_percentage": discount_percentage
            }])

            return df
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")

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
                self.extract_scrape_content(url, '#main-content'))
            df = self.transform(soup, url)

            if df is not None:
                self.load(df, db_conn, table_name)
                update_url_scrape_status(db_conn, pkey, "DONE", now)
            else:
                update_url_scrape_status(db_conn, pkey, "FAILED", now)
