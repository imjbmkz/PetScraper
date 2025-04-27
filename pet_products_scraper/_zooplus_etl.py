import pandas as pd
import random
import requests
import math
import re
import json
import time
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
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
MAX_WAIT_BETWEEN_REQ = 10
MIN_WAIT_BETWEEN_REQ = 5
REQUEST_TIMEOUT = 30


class ZooplusETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "Zooplus"
        self.BASE_URL = "https://www.zooplus.co.uk"
        self.CATEGORIES = [
            '/shop/dogs/dry_dog_food',
            '/shop/dogs/wet_dog_food',
            '/shop/dogs/dog_treats_chews',
            '/shop/dogs/dog_health',
            '/shop/dogs/dog_beds_baskets',
            '/shop/dogs/dog_collars_dog_leads',
            '/shop/dogs/dog_flea_tick_treatments',
            '/shop/dogs/dog_toys_dog_training',
            '/shop/dogs/dog_grooming_care',
            '/shop/dogs/dog_bowls_feeders',
            '/shop/dogs/dog_cages_carriers',
            '/shop/dogs/dog_kennels_flaps',
            '/shop/dogs/dog_clothing',
            '/shop/dogs/agility_training',
            '/shop/dogs/dog_breed',
            '/shop/dogs/puppy_products',
            '/shop/dogs/senior_products',
            '/shop/cats/dry_cat_food',
            '/shop/cats/canned_cat_food_pouches',
            '/shop/cats/cat_litter',
            '/shop/cats/cat_flea_tick_treatments',
            '/shop/cats/cat_litter_litter_boxes',
            '/shop/cats/scratching_posts',
            '/shop/cats/cat_treats_catnip',
            '/shop/cats/supplements_specialty_cat_food',
            '/shop/cats/cat_health',
            '/shop/cats/cat_beds_baskets',
            '/shop/cats/cat_flaps_nets',
            '/shop/cats/cat_toys',
            '/shop/cats/cat_bowls_feeders',
            '/shop/cats/cat_carriers_travel',
            '/shop/cats/cat_breed',
            '/shop/cats/kitten_products',
            '/shop/cats/senior_products',
            '/shop/small_pets/hutches_cages',
            '/shop/small_pets/accessories',
            '/shop/small_pets/food',
            '/shop/small_pets/hay_and_bedding',
            '/shop/small_pets/runs_fencing',
            '/shop/small_pets/care_grooming',
            '/shop/small_pets/toys_transport',
            '/shop/small_pets/hutches',
            '/shop/small_pets/rabbits',
            '/shop/small_pets/guinea_pig',
            '/shop/small_pets/hamster',
            '/shop/small_pets/rat',
            '/shop/small_pets/mouse',
            '/shop/small_pets/gerbil',
            '/shop/small_pets/degus',
            '/shop/small_pets/chinchillas',
            '/shop/small_pets/ferret',
            '/shop/birds/bird_food',
            '/shop/birds/bird_cages_and_accessories',
            '/shop/birds/cage_accessories',
            '/shop/birds/snacks_and_supplements',
            '/shop/birds/toys',
            '/shop/birds/bedding_and_litter',
            '/shop/birds/wild_birds'
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
                    "headless": True,
                    "args": ["--disable-blink-features=AutomationControlled"]
                }

                browser = await p.chromium.launch(**browser_args)
                context = await browser.new_context(
                    locale="en-US"
                )

                page = await context.new_page()

                await page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                await page.set_extra_http_headers({
                    "Accept": 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Encoding': 'gzip, deflate, br, zstd',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Cache-Control': 'max-age=0',
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 OPR/118.0.0.0",
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
            product_data = json.loads(soup.select(
                "script[type*='application/ld+json']")[0].text)
            product_name = product_data['name']
            product_description = product_data['description']
            product_url = url.replace(self.BASE_URL, "")

            rating = '0/5'
            if "aggregateRating" in product_data.keys():
                rating = product_data["aggregateRating"]["ratingValue"]
                rating = f"{rating}/5"

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            pattern = r"^.*£"
            rrb_pattern = r"[^\d\.]"

            variants_list = soup.find(
                'div', class_="VariantList_variantList__PeaNd")
            if variants_list:
                variant_hopps = variants_list.select(
                    "div[data-hopps*='Variant']")
                for variant_hopp in variant_hopps:
                    variant = variant_hopp.select_one(
                        "span[class*='VariantDescription_description']").text
                    image_variant = variant_hopp.find('img').get('src')
                    discount_checker = variant_hopp.find(
                        'div', class_="z-product-price__note-wrap")

                    if discount_checker:
                        price = float(re.sub(rrb_pattern, "", variant_hopp.select_one(
                            "div[class*='z-product-price__nowrap']").text))
                        discounted_price = float(re.sub(pattern, "", variant_hopp.select_one(
                            "span[class*='z-product-price__amount']").text))
                        discount_percent = round(
                            (price - float(discounted_price)) / price, 2)
                    else:
                        price = float(re.sub(pattern, "", variant_hopp.select_one(
                            "span[class*='z-product-price__amount']").text))
                        discounted_price = None
                        discount_percent = None

                    variants.append(variant)
                    prices.append(price)
                    discounted_prices.append(discounted_price)
                    discount_percentages.append(discount_percent)
                    image_urls.append(image_variant)

            else:
                variant = soup.select_one(
                    "div[data-zta*='ProductTitle__Subtitle']").text
                discount_checker = soup.find('span', attrs={
                    'data-zta': 'SelectedArticleBox__TopSection'}).find('div', class_="z-product-price__note-wrap")

                if discount_checker:
                    price = float(re.sub(rrb_pattern, "", soup.find('span', attrs={
                        'data-zta': 'SelectedArticleBox__TopSection'}).find('div', class_="z-product-price__nowrap").get_text()))
                    discounted_price = float(re.sub(pattern, "", soup.find('span', attrs={
                        'data-zta': 'SelectedArticleBox__TopSection'}).find('span', class_="z-product-price__amount--reduced").get_text()))
                    discount_percent = round(
                        (price - float(discounted_price)) / price, 2)
                else:
                    price = float(re.sub(pattern, "", soup.find('span', attrs={
                        'data-zta': 'SelectedArticleBox__TopSection'}).find('span', class_="z-product-price__amount").get_text()))
                    discounted_price = None
                    discount_percent = None

                variants.append(variant)
                prices.append(price)
                discounted_prices.append(discounted_price)
                discount_percentages.append(discount_percent)
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
            df.insert(0, "rating", rating)
            df.insert(0, "name", product_name)
            df.insert(0, "shop", self.SHOP)

            return df

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")

    @retry(
        wait=wait_random(min=MIN_WAIT_BETWEEN_REQ, max=MAX_WAIT_BETWEEN_REQ),
        stop=stop_after_attempt(MAX_RETRIES),
        retry=retry_if_exception_type(requests.RequestException),
        before_sleep=before_sleep_log(logger, "WARNING"),
        reraise=True,
    )
    def get_product_links(self, url, headers):
        try:
            # Parse request response
            response = self.session.request(
                method="GET", url=url, headers=headers)
            response.raise_for_status()

            logger.info(
                f"Successfully extracted data from {url} {response.status_code}"
            )
            sleep_time = random.uniform(
                MIN_WAIT_BETWEEN_REQ, MAX_WAIT_BETWEEN_REQ)
            time.sleep(sleep_time)
            logger.info(f"Sleeping for {sleep_time} seconds...")
            return response

        except Exception as e:
            logger.error(f"Error in parsing {url}: {e}")

    def get_links(self, category: str) -> pd.DataFrame:
        if category not in self.CATEGORIES:
            raise ValueError(
                f"Invalid category. Value must be in {self.CATEGORIES}")

        headers = {
            "Accept": 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            "User-Agent": UserAgent().random,
            'Referer': 'https://www.bitiba.co.uk/',
            'Priority': "u=0, i",
            "Upgrade-Insecure-Requests": "1",
            "Connection": "keep-alive",
            "Sec-Ch-Ua": "\"Not(A:Brand\";v=\"99\", \"Opera GX\";v=\"118\", \"Chromium\";v=\"133\"",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "\"Windows\"",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1"
        }

        urls = []
        n_page_pagination = 1
        list_prod_api = self.get_product_links(
            f"https://www.zooplus.co.uk/api/discover/v1/products/list-faceted-partial?&path={category}&domain=zooplus.co.uk&language=en&page=1&size=24&ab=shop-10734_shop_product_catalog_api_enabled_targeted_delivery.enabled%2Bidpo-1141_article_based_product_cards_targeted_delivery.on%2Bidpo-1390_rebranding_foundation_targeted_delivery.on%2Bexplore-3092-price-redesign_targeted_delivery.on", headers=headers)
        if list_prod_api.status_code == 200:
            if list_prod_api.json()['pagination'] == None:
                for products in list_prod_api.json()['productList']['products']:
                    urls.append(products["path"])

            else:
                n_page_pagination = int(list_prod_api.json()[
                                        'pagination']["count"])

        if n_page_pagination > 1:
            for i in range(1, n_page_pagination + 1):
                pagination_url = f"https://www.zooplus.co.uk/api/discover/v1/products/list-faceted-partial?&path={category}&domain=zooplus.co.uk&language=en&page={i}&size=24&ab=shop-10734_shop_product_catalog_api_enabled_targeted_delivery.enabled%2Bidpo-1141_article_based_product_cards_targeted_delivery.on%2Bidpo-1390_rebranding_foundation_targeted_delivery.on%2Bexplore-3092-price-redesign_targeted_delivery.on"

                pagination_product_api = self.get_product_links(
                    pagination_url, headers=headers)
                if pagination_product_api.status_code == 200:
                    for products in pagination_product_api.json()['productList']['products']:
                        urls.append(products["path"])

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df
