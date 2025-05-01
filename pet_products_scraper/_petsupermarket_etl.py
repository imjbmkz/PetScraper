import json
import random
import requests
import pandas as pd
from datetime import datetime as dt
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

MAX_RETRIES = 25
MAX_WAIT_BETWEEN_REQ = 1
MIN_WAIT_BETWEEN_REQ = 0.5
REQUEST_TIMEOUT = 30


class PetSupermarketETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "PetSupermarket"
        self.BASE_URL = "https://www.pet-supermarket.co.uk"
        self.CATEGORIES = ["/Dog/c/c000001", "/Cat/c/c000002",
                           "/Small-Animals/c/c008034", "/Birds/c/c008002"]

    @retry(
        wait=wait_random(min=MIN_WAIT_BETWEEN_REQ, max=MAX_WAIT_BETWEEN_REQ),
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
                await page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

                await page.set_extra_http_headers({
                    "User-Agent": UserAgent().random,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Upgrade-Insecure-Requests": "1",
                    "Connection": "keep-alive",
                    "Sec-Ch-Ua": "\"Not(A:Brand\";v=\"99\", \"Opera GX\";v=\"118\", \"Chromium\";v=\"133\"",
                    "Sec-Ch-Ua-Mobile": "?0",
                    "Sec-Ch-Ua-Platform": "\"Windows\"",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "same-origin",
                    "Sec-Fetch-User": "?1",
                    'Referer': 'https://www.google.com/',
                })

                await asyncio.sleep(random.uniform(MIN_WAIT_BETWEEN_REQ, MAX_WAIT_BETWEEN_REQ))
                await page.goto(url, wait_until="domcontentloaded")
                await page.wait_for_selector(selector, timeout=300000)

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

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_header = soup.select_one(
                "div[class*='product-header']")
            product_title = product_header.select_one(
                "h1[class*='name']").text
            rating = product_header.select_one(
                "div[class*='js-ratingCalc']")
            if rating:
                rating_rating = round(json.loads(
                    rating["data-rating"])["rating"], 2)
                rating_total = json.loads(rating["data-rating"])["total"]
                rating = f"{rating_rating}/{rating_total}"

            description_list = soup.select(
                "div[id*='product-details-tab']")[0].select("p")
            description = " ".join(
                [p.text for p in description_list]).strip()
            product_url = url.replace(self.BASE_URL, "")

            # Placeholder for variant details
            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []
            variant_tiles = product_header.select(
                "div[class*='variant-tile']")

            for variant_tile in variant_tiles:
                variant_tile_item = variant_tile.select_one("li")
                variant = variant_tile_item["data-product-feature-qualifier-name"]
                if variant_tile_item.has_attr("data-was-price"):
                    price = float(
                        variant_tile_item["data-was-price"].replace("£", ""))
                    discounted_price = float(
                        variant_tile_item["data-selling-price-value"].replace("£", ""))
                    discount_percentage = None
                    if price > 0:
                        discount_percentage = (
                            price - discounted_price) / price

                else:
                    price = float(
                        variant_tile_item["data-selling-price-value"])
                    discounted_price = None
                    discount_percentage = None

                variants.append(variant)
                prices.append(price)
                discounted_prices.append(discounted_price)
                discount_percentages.append(discount_percentage)
                image_urls.append(', '.join([img.get('src') for img in soup.find_all(
                    'div', attrs={'data-test': 'carousel-inner-wrapper'})[0].find_all('img')]))

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
        # # Data validation on category
        # cleaned_category = category.lower()
        # if cleaned_category not in self.CATEGORIES:
        #     raise ValueError(f"Invalid category. Value must be in {self.CATEGORIES}")

        category_url = f"{self.BASE_URL}{category}"
        current_url = category_url

        urls = []

        while True:
            soup = self.extract_from_url("GET", current_url)
            product_item_links = soup.select("a[class*='product-item-link']")
            if product_item_links:
                product_urls = [product_item_link["href"]
                                for product_item_link in product_item_links]
                urls.extend(product_urls)

                pagination_arrows = soup.select("a[class*='pagination-arrow']")
                if pagination_arrows:
                    pagination_arrow = pagination_arrows[-1]
                    if "next" in pagination_arrow["rel"]:
                        pagination_arrow_link = pagination_arrow["href"]
                        current_url = f"{self.BASE_URL}{pagination_arrow_link}"
                        continue

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

            now = dt.now().strftime("%Y-%m-%d %H:%M:%S")

            soup = asyncio.run(
                self.extract_scrape_content(url, '#feedbackButton'))
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
            'image_urls': soup.find('meta', attrs={'name': "sailthru.image.full"}).get('content')
        }
