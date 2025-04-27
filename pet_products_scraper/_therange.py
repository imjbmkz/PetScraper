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

from fake_useragent import UserAgent

import asyncio
import nest_asyncio
from playwright.async_api import async_playwright
nest_asyncio.apply()

MAX_RETRIES = 10
MAX_WAIT_BETWEEN_REQ = 300
MIN_WAIT_BETWEEN_REQ = 100
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
                    "headless": False,
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
                    "Accept": 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Encoding': 'gzip, deflate, br, zstd',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Cache-Control': 'max-age=0',
                    'Cookie': 'si=13-680dd27df36ef2.48880053; __cf_bm=nH2EM6Wj98NrLgphO4oGZY0uuAAQ0jFTghQSaABw4Kg-1745744871-1.0.1.1-ivbSDo3.HsS._KOy1bacHm0EvITTQPwod7nNJ4DowG29X3fYW6mushmLko3M9Equ_6VRwyHAwFxbUyLPxyMKcaCarAMwYj8Sl0LN1FhF7sKKNDn2dyGLSTYqx76qKCWA; cf_clearance=yt_ZhnivZ0n5RHFYO1zmvD6hzFyyP_NOXjkgv1AgCAY-1745744881-1.2.1.1-Tsszn2GM92T.7Gxsm_GfbB1UE_x36Zw3c_Czoey5EyBxtTLDf.MpnOzb2yk2YES9SP9BxKPJeK2.HMCg60yX6KyTWMomUBPMZIzFmIDm6tfmErQDGJl4obN08p7nKZGrpqI3NCg9VOF_K8J5VpTjdxuFcrqU3NXNw4PQOb2F07SAMMdY7E8.XNZj524ibaAizlzU6X8.TGEwPFT8u14Iimrh8X9i261J4GSCNi8DWJqsl_P96gSljqFU_wdxvTdw8tRSOTo__zsSH1ReZGduVklVmgnkqoTyMnYE9HkzSE9dZymTcik7fjr3XjH0aXhEpTVafjtJ5KHLjDAJZXK.B75gai2eMkKDiLGpADk5LKvqI.ENAZOvOn.qCu6T.FYY',
                    'Referer': 'https://www.google.com/',
                    'Priority': "u=0, i",
                    "Upgrade-Insecure-Requests": "1",
                    "Connection": "keep-alive",
                    "Sec-Ch-Ua": "\"Not(A:Brand\";v=\"99\", \"Opera GX\";v=\"118\", \"Chromium\";v=\"133\"",
                    "Sec-Ch-Ua-Mobile": "?0",
                    "Sec-Ch-Ua-Platform": "\"Windows\"",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "same-origin",
                    "Sec-Fetch-User": "?1"
                })

                await asyncio.sleep(random.uniform(MIN_WAIT_BETWEEN_REQ, MAX_WAIT_BETWEEN_REQ))
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

    @retry(
        wait=wait_random(min=MIN_WAIT_BETWEEN_REQ, max=MAX_WAIT_BETWEEN_REQ),
        stop=stop_after_attempt(MAX_RETRIES),
        retry=retry_if_exception_type(requests.RequestException),
        before_sleep=before_sleep_log(logger, "WARNING"),
        reraise=True,
    )
    async def get_json_product(self, url):
        browser = None
        try:
            async with async_playwright() as p:
                browser_args = {
                    "headless": True,
                    "args": ["--disable-blink-features=AutomationControlled"]
                }

                browser = await p.chromium.launch(**browser_args)
                context = await browser.new_context(locale="en-US")

                page = await context.new_page()
                await page.set_extra_http_headers({
                    "User-Agent": UserAgent().random,
                    "Accept": 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Encoding': 'gzip, deflate, br, zstd',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Cache-Control': 'max-age=0',
                    'Referer': 'https://www.google.com/',
                    'Priority': "u=0, i",
                    "Upgrade-Insecure-Requests": "1",
                    "Connection": "keep-alive",
                    "Sec-Ch-Ua": "\"Not(A:Brand\";v=\"99\", \"Opera GX\";v=\"118\", \"Chromium\";v=\"133\"",
                    "Sec-Ch-Ua-Mobile": "?0",
                    "Sec-Ch-Ua-Platform": "\"Windows\"",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "same-origin",
                    "Sec-Fetch-User": "?1"
                })

                await page.goto(url, wait_until="domcontentloaded")

                output = await page.content()
                logger.info(
                    f"Successfully extracted data from {url}"
                )
                sleep_time = random.uniform(
                    MIN_WAIT_BETWEEN_REQ, MAX_WAIT_BETWEEN_REQ)
                logger.info(f"Sleeping for {sleep_time} seconds...")
                soup = BeautifulSoup(output, 'html.parser')
                pre_tag = soup.find('pre')

                if pre_tag:
                    json_text = pre_tag.get_text()
                    output = json.loads(json_text)
                    logger.info(f"Successfully extracted JSON data from {url}")

                    # Sleep between requests
                    sleep_time = random.uniform(
                        MIN_WAIT_BETWEEN_REQ, MAX_WAIT_BETWEEN_REQ)
                    logger.info(f"Sleeping for {sleep_time} seconds...")

                    return output
                else:
                    logger.error(f"No <pre> tag found at {url}")
                    return None

        except Exception as e:
            logger.error(f"An error occurred: {e}")

        finally:
            if browser:
                await browser.close()

    def transform(self, soup: BeautifulSoup, url: str) -> pd.DataFrame:
        try:
            product_name = soup.find('h1', id="product-dyn-title").get_text()
            product_description = soup.find(
                'p', id='product-dyn-desc').find(string=True)
            product_url = url.replace(self.BASE_URL, "")
            product_rating = "0/5"
            product_id = soup.find('input', id="product_id").get('value')
            clean_url = url.split('#')[0]

            if not soup.find('div', class_="no_reviews_info"):
                product_rating_soup = asyncio.run(self.extract_scrape_content(
                    f'{clean_url}?action=loadreviews&pid={product_id}&page=1', '#review-product-summary'))

                if product_rating_soup.find('div', id="review-product-summary"):
                    product_rating = str(round((int(product_rating_soup.find('div', id="review-product-summary").findAll(
                        'div', class_="progress-bar")[0].get('aria-valuenow')) / 100) * 5, 2)) + '/5'

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            product_details = asyncio.run(
                self.get_json_product(f'{clean_url}?json'))
            if len(product_details['variant_arr']) > 1:
                for var_details in product_details['variant_arr']:
                    if " - " in var_details['name']:
                        variants.append(var_details['name'].split(" - ")[1])

                    if var_details['price_was'] == None:
                        prices.append(var_details['price'] / 100)
                        discounted_prices.append(None)
                        discount_percentages.append(None)

                    else:
                        prices.append(var_details['price_was'] / 100)
                        discounted_prices.append(var_details['price'] / 100)
                        discount_percentages.append(
                            var_details['price_was_percent'] / 100)

                    image_urls.append(
                        soup.find('meta', attrs={'property': "og:image"}).get('content'))

            else:
                variants.append(None)
                image_urls.append(
                    soup.find('meta', attrs={'property': "og:image"}).get('content'))
                if product_details['variant_arr'][0]['price_was'] == None:
                    prices.append(
                        product_details['variant_arr'][0]['price'] / 100)
                    discounted_prices.append(None)
                    discount_percentages.append(None)
                else:
                    prices.append(
                        product_details['variant_arr'][0]['price_was'] / 100)
                    discounted_prices.append(
                        product_details['variant_arr'][0]['price'] / 100)
                    discount_percentages.append(
                        product_details['variant_arr'][0]['price_was_percent'] / 100)

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

        try:
            category_link = f"{self.BASE_URL}{category}"
            urls = []
            soup = asyncio.run(
                self.extract_scrape_content(category_link,  '#root'))

            category_id = soup.find('div', id="root")['data-page-id']
            total_product = soup.find('div', id="root")['data-total-results']

            product_list = self.session.request(method="GET",
                                                url=f'https://search.therange.co.uk/api/productlist?categoryId={category_id}&sort=relevance&limit={total_product}&filters=%7B"in_stock_f"%3A%5B"true"%5D%7D', timeout=REQUEST_TIMEOUT)
            product_list.raise_for_status()
            if product_list.status_code == 200:
                for url in product_list.json()['products']:
                    if url.get('variantPath') is not None:
                        urls.append(self.BASE_URL + '/' +
                                    url.get('variantPath'))

            df = pd.DataFrame({"url": urls})
            df.insert(0, "shop", self.SHOP)
            return df

        except:
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

            soup = asyncio.run(self.extract_scrape_content(
                url,  '#variant_container'))
            df = self.transform(soup, url)

            if df is not None:
                self.load(df, db_conn, table_name)
                update_url_scrape_status(db_conn, pkey, "DONE", now)

            else:
                update_url_scrape_status(db_conn, pkey, "FAILED", now)
