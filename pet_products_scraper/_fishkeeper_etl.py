import requests
import json
import random
import math
import pandas as pd

from dotenv import load_dotenv
from os import getenv
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy.engine import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random,
)

from fake_useragent import UserAgent
import asyncio
import nest_asyncio
from playwright.async_api import async_playwright
nest_asyncio.apply()

MAX_RETRIES = 10
MAX_WAIT_BETWEEN_REQ = 1
MIN_WAIT_BETWEEN_REQ = 0
REQUEST_TIMEOUT = 30


class FishKeeperETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "FishKeeper"
        self.BASE_URL = "https://www.fishkeeper.co.uk"
        self.CATEGORIES = [
            "/aquarium-products",
            "/pond-products",
            "/marine",
        ]

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

    async def product_list_scroll(self, url, selector):
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
                    "Origin": "https://www.fishkeeper.co.uk",
                    "Referer": url,
                })

                await page.goto(url, wait_until="domcontentloaded")
                await page.wait_for_selector(selector, timeout=30000)

                logger.info(
                    "Starting to scrape the product list (Infinite scroll scrape)...")

                while True:
                    try:
                        await page.wait_for_selector('.ais-InfiniteHits-loadMore', timeout=3000)
                        logger.info("Expanding Product List")
                        await page.click('.ais-InfiniteHits-loadMore')
                        await asyncio.sleep(1)
                    except Exception:
                        logger.info(
                            "Cannot Expand Product List Anymore Scraping Cmplete")
                        break

                rendered_html = await page.content()
                logger.info(
                    f"Successfully extracted data from {url}"
                )
                sleep_time = random.uniform(
                    MIN_WAIT_BETWEEN_REQ, MAX_WAIT_BETWEEN_REQ)
                logger.info(f"Sleeping for {sleep_time} seconds...")
                soup = BeautifulSoup(rendered_html, "html.parser")
                return soup.find('ol', class_="ais-InfiniteHits-list")

        except Exception as e:
            logger.error(f"An error occurred: {e}")

        finally:
            if browser:
                await browser.close()

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            data = json.loads(soup.select_one(
                "script[type*='application/ld+json']").text)
            product_title = data["name"]

            rating = 0
            sku = data["mpn"]
            rating_wrapper = requests.get(
                f"https://api.feefo.com/api/10/products/ratings?merchant_identifier=maidenhead-aquatics&review_count=true&product_sku={sku}")
            if rating_wrapper.status_code == 200:
                json_data = rating_wrapper.json()
                products = json_data.get('products', [])

                if products and 'rating' in products[0]:
                    rating = float(products[0]['rating'])

            description = data["description"]
            product_url = url.replace(self.BASE_URL, "")

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            data_offers = data["offers"]

            if "offers" in data_offers.keys():
                for variant in data_offers["offers"]:
                    variants.append(variant['name'])
                    prices.append(variant['price'])
                    discounted_prices.append(None)
                    discount_percentages.append(None)
                    image_urls.append(variant['image'])

            else:
                variants.append(None)
                prices.append(data["offers"]['price'])
                discounted_prices.append(None)
                discount_percentages.append(None)
                image_urls.append(data['image'])

            df = pd.DataFrame({"variant": variants,
                               "price": prices,
                               "discounted_price": discounted_prices,
                               "discount_percentage": discount_percentages,
                               "image_urls": image_urls})

            df.insert(0, "url", product_url)
            df.insert(0, "description", description)
            df.insert(0, "rating", rating)
            df.insert(0, "name", product_title)
            df.insert(0, "shop", self.SHOP)

            return df

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")

    def get_links(self, category: str) -> pd.DataFrame:

        if category not in self.CATEGORIES:
            raise ValueError(
                f"Invalid category. Value must be in {self.CATEGORIES}")

        url = self.BASE_URL + category

        soup_pagination = asyncio.run(
            self.product_list_scroll(url, '.ais-InfiniteHits-list'))
        urls = [product.find('a').get('href') for product in soup_pagination.find_all(
            'li', class_="ais-InfiniteHits-item")]

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df

    def image_scrape_product(self, url):
        soup = self.extract_from_url("GET", url)

        return {
            'shop': self.SHOP,
            'url': url,
            'image_urls': soup.find('meta', attrs={'property': "og:image"}).get('content')
        }
