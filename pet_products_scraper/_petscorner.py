import re
import json
import requests
import math
import random
import pandas as pd
from datetime import datetime as dt
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file
from fake_useragent import UserAgent


import asyncio
import nest_asyncio
from playwright.async_api import async_playwright
nest_asyncio.apply()

headers = {
    "User-Agent": UserAgent().random
}

MAX_RETRIES = 10
MAX_WAIT_BETWEEN_REQ = 1
MIN_WAIT_BETWEEN_REQ = 0
REQUEST_TIMEOUT = 30


class PetsCornerETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "PetsCorner"
        self.BASE_URL = "https://www.petscorner.co.uk"
        self.CATEGORIES = [
            '/dog/puppy-essentials/puppy-food/',
            '/dog/puppy-essentials/puppy-feeding-equipment/',
            '/dog/puppy-essentials/puppy-treats/',
            '/dog/puppy-essentials/puppy-toys/',
            '/dog/puppy-essentials/puppy-beds-blankets/',
            '/dog/puppy-essentials/puppy-pads-toilet-training/',
            '/dog/puppy-essentials/puppy-collars-leads-harnesses-tags/',
            '/dog/puppy-essentials/puppy-grooming/',
            '/dog/puppy-essentials/puppy-training/',
            '/dog/puppy-essentials/puppy-crates/',
            '/dog/puppy-essentials/puppy-travel/',
            '/dog/puppy-essentials/puppy-healthcare/',
            '/dog/dog-food/all-dog-food/',
            '/dog/dog-treats/',
            '/dog/dog-healthcare/',
            '/dog/dog-coats-jumpers/',
            '/dog/dog-travel/',
            '/dog/dog-collars-leads-harnesses-tags/',
            '/dog/dog-toys/all-dog-toys/',
            '/dog/dog-feeding-equipment/',
            '/dog/dog-grooming/',
            '/dog/dog-beds/all-dog-beds/',
            '/dog/dog-training/',
            '/dog/home-garden-gifts/',
            '/cat/kitten-essentials/kitten-food/',
            '/cat/kitten-essentials/kitten-feeding-equipment/',
            '/cat/kitten-essentials/kitten-treats/',
            '/cat/kitten-essentials/kitten-toys/',
            '/cat/kitten-essentials/kitten-beds-blankets/',
            '/cat/kitten-essentials/kitten-scratchers/',
            '/cat/kitten-essentials/kitten-litter-training/',
            '/cat/kitten-essentials/kitten-collars-id-tags/',
            '/cat/kitten-essentials/kitten-grooming/',
            '/cat/kitten-essentials/kitten-travel/',
            '/cat/kitten-essentials/kitten-healthcare/',
            '/cat/cat-food/all-cat-food/',
            '/cat/cat-treats/',
            '/cat/cat-healthcare/',
            '/cat/cat-scratchers-and-toys/',
            '/cat/cat-collars-leads-harnesses-tags/',
            '/cat/cat-flaps/',
            '/cat/cat-travel/',
            '/cat/cat-bowls-feeding-equipment/',
            '/cat/cat-grooming/',
            '/cat/cat-beds/',
            '/cat/cat-litter-accessories/',
            '/small-animal/small-animal-food-hay/',
            '/small-animal/small-animal-houses-bedding/',
            '/small-animal/small-animal-bowls-feeding-equipment/',
            '/small-animal/small-animal-healthcare/',
            '/small-animal/small-animal-toys-activities/',
            '/small-animal/small-animal-travel/',
            '/small-animal/small-animal-treats-gnaws/',
            '/pet-bird/pet-bird-food/',
            '/pet-bird/pet-bird-feeders/',
            '/pet-bird/pet-bird-cages-aviaries/',
            '/pet-bird/pet-bird-healthcare/',
            '/pet-bird/pet-bird-toys-treats/',
            '/pet-bird/home-farming/',
            '/wildlife/wild-animal-food/',
            '/wildlife/wild-bird-food/',
            '/wildlife/wild-bird-feeders/',
            '/reptile/reptile-food/',
            '/reptile/reptile-healthcare-cleaning/',
            '/reptile/reptile-feeding-equipment/',
            '/reptile/reptile-bedding-substrates/',
            '/reptile/reptile-vivarium-accessories/',
            '/fish/fish-tanks-accessories/',
            '/fish/fish-food/',
            '/fish/fish-treatments/'
        ]

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

    def get_links(self, category: str) -> pd.DataFrame:
        if category not in self.CATEGORIES:
            raise ValueError(
                f"Invalid category. Value must be in {self.CATEGORIES}")

        category_link = f"{self.BASE_URL}{category}"
        urls = []

        soup = self.extract_from_url('GET', category_link, headers=headers)
        n_products = int(
            soup.find('span', class_="total").get_text().replace(' products', ''))
        n_pages = math.ceil(n_products / 24)

        for p in range(1, n_pages + 1):
            soup_pages = self.extract_from_url(
                'GET', f'{category_link}?listing_page={p}', headers=headers)
            for product in soup_pages.find_all('div', class_="product-listing-column"):
                urls.append(self.BASE_URL + product.find('a').get('href'))

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find('h1', class_="product-name").get_text()
            product_description = None

            if soup.find('div', id="ctl00_Content_zneContent6_ctl05_ctl02"):
                product_description = soup.find(
                    'div', id="ctl00_Content_zneContent6_ctl05_ctl02").get_text()

            product_url = url.replace(self.BASE_URL, "")
            product_rating = '0/5'
            product_id = soup.find_all(
                'div', class_="notify-stock")[-1].get('data-productid')
            sku = None
            sku_tag = soup.find('div', id="feefo-product-review-widgetId")
            if sku_tag.get('data-parent-product-sku'):
                sku = f"parent_product_sku={sku_tag.get('data-parent-product-sku')}"
            else:
                sku = f"product_sku={sku_tag.get('data-product-sku')}"

            rating_wrapper = requests.get(
                f"https://api.feefo.com/api/10/importedreviews/summary/product?since_period=ALL&{sku}&merchant_identifier=pets-corner&origin=www.petscorner.co.uk")
            if rating_wrapper.status_code == 200:
                product_rating = str(rating_wrapper.json()[
                                     'rating']['rating']) + '/5'

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            if soup.find('div', class_="hidden-select"):
                for variant in soup.find('div', class_="fake-select").find_all('div', class_="text"):
                    variants.append(variant.get_text(strip=True))

                for price_list in soup.find('div', class_="hidden-select").find_all('input'):
                    if price_list.get('data-was-price') == '0.00':
                        prices.append(
                            float(price_list.get('data-product-price')))
                        discounted_prices.append(None)
                        discount_percentages.append(None)
                    else:
                        prices.append(float(price_list.get('data-was-price')))
                        discounted_prices.append(
                            float(price_list.get('data-product-price')))
                        discount_percentage = (float(price_list.get('data-was-price')) - float(
                            price_list.get('data-product-price'))) / float(price_list.get('data-was-price'))
                        discount_percentages.append(
                            "{:.2f}".format(round(discount_percentage, 2)))

                    image_urls.append(
                        soup.find('meta', attrs={'property': "og:image"}).get('content'))

            else:
                price_template = soup.find_all(
                    'span', attrs={'class': ['item-price', 'order-section']})[-1]

                if price_template.find('span', class_="was-price"):
                    variants.append(None)
                    prices.append(price_template.find(
                        'span', class_="was-price").get_text())
                    discounted_prices.append(
                        float(price_template.find('span', class_="price").get_text()))
                    discount_percentages.append(None)
                else:
                    variants.append(None)
                    prices.append(float(price_template.find(
                        'span', class_="price").get_text()))
                    discounted_prices.append(None)
                    discount_percentages.append(None)

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

    def run(self, db_conn: Engine, table_name: str):
        sql = get_sql_from_file("select_unscraped_urls.sql")
        sql = sql.format(shop=self.SHOP)
        df_urls = self.extract_from_sql(db_conn, sql)

        for i, row in df_urls.iterrows():

            pkey = row["id"]
            url = row["url"]

            now = dt.now().strftime("%Y-%m-%d %H:%M:%S")

            soup = asyncio.run(self.extract_scrape_content(
                url, '#ctl00_Content_zneContent6_ctl05_ctl02'))
            df = self.transform(soup, url)

            if df is not None:
                self.load(df, db_conn, table_name)
                update_url_scrape_status(db_conn, pkey, "DONE", now)
            else:
                update_url_scrape_status(db_conn, pkey, "FAILED", now)

    def image_scrape_product(self, url):
        soup = asyncio.run(self.extract_scrape_content(
            url, '#ctl00_Content_zneContent6_ctl05_ctl02'))

        return {
            'shop': self.SHOP,
            'url': url,
            'image_urls': soup.find('meta', attrs={'property': "og:image"}).get('content')
        }
