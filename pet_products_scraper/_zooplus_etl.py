import pandas as pd
import random
import requests
import math
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
MAX_WAIT_BETWEEN_REQ = 1
MIN_WAIT_BETWEEN_REQ = 0.5
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
                    user_agent=UserAgent().random,
                    locale="en-US"
                )

                page = await context.new_page()

                await page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                await page.set_extra_http_headers({
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Upgrade-Insecure-Requests": "1",
                    "Referer": url,
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
        # Get product wrappers. Each wrapper may have varying content.
        product_wrappers = soup.select(
            'div[class*="ProductListItem_productWrapper"]')

        # Placeholder for consolidated data frames
        consolidated_data = []

        # Iterate through the wrappers
        for wrapper in product_wrappers:
            # Get the product title, rating, and description
            product_title = wrapper.select_one(
                'a[class*="ProductListItem_productInfoTitleLink"]').text
            rating = wrapper.select_one(
                'span[class*="pp-visually-hidden"]').text
            description = wrapper.select_one(
                'p[class*="ProductListItem_productInfoDescription"]').text
            product_url = wrapper.select_one(
                'a[class*="ProductListItem_productInfoTitleLink"]')["href"]

            # Get product variants. Each variant has their own price.
            product_variants = wrapper.select(
                'div[class*="ProductListItemVariant_variantWrapper"]')

            # Placeholder for variant details
            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            # Get the variant name, price, and reference price
            for variant in product_variants:
                variants.append(variant.select_one(
                    'span[class*="ProductListItemVariant_variantDescription"]').text)

                price = float(variant.select_one(
                    'span[class*="z-price__amount"]').text.replace("£", ""))

                # Not all products are discounted. Sometimes, there are no reference prices
                reference_price_span = variant.select_one(
                    'span[data-zta*="productReducedPriceRefPriceAmount"]')
                if reference_price_span:

                    # Reference price is the original price before discount
                    reference_price = float(
                        reference_price_span.text.replace("£", ""))
                    prices.append(reference_price)
                    discounted_prices.append(price)

                    # Calculate the discount percentage
                    discount_percentage = (
                        (reference_price - price) / reference_price)
                    discount_percentages.append(discount_percentage)

                else:
                    # If there is no reference price, then the product is not discounted
                    prices.append(price)
                    discounted_prices.append(None)
                    discount_percentages.append(None)

            # Compile the data acquired into dataframe
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

            consolidated_data.append(df)

        try:
            df_consolidated = pd.concat(
                consolidated_data, ignore_index=True)
            df_consolidated.insert(0, "shop", self.SHOP)

            return df_consolidated

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")

    @retry(
        wait=wait_random(min=MIN_WAIT_BETWEEN_REQ, max=MAX_WAIT_BETWEEN_REQ),
        stop=stop_after_attempt(MAX_RETRIES),
        retry=retry_if_exception_type(AttributeError),
        before_sleep=before_sleep_log(logger, "WARNING"),
        reraise=True
    )
    def get_links(self, category: str) -> pd.DataFrame:
        if category not in self.CATEGORIES:
            raise ValueError(
                f"Invalid category. Value must be in {self.CATEGORIES}")

        url = self.BASE_URL + category

        soup = asyncio.run(self.extract_scrape_content(
            url, '#shop-main-navigation'))

        pagination = soup.find('ul', class_="z-pagination__list")
        if pagination is None:
            raise AttributeError("Pagination element not found — retrying...")

        pagination_length = int(pagination.find_all('a')[-1].get_text())

        urls = []
        for i in range(1, pagination_length + 1):
            page_url = f"{url}?p={i}"
            try:
                links = self.get_product_links_from_page(page_url)
                urls.extend(links)
            except Exception as e:
                logger.error(f"Page {i} failed after retries: {e}")

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df

    @retry(
        wait=wait_random(min=MIN_WAIT_BETWEEN_REQ, max=MAX_WAIT_BETWEEN_REQ),
        stop=stop_after_attempt(MAX_RETRIES),
        retry=retry_if_exception_type(AttributeError),
        before_sleep=before_sleep_log(logger, "WARNING"),
        reraise=True
    )
    def get_product_links_from_page(self, url: str) -> list:
        soup_pagination = asyncio.run(self.extract_scrape_content(
            url, '#shop-main-navigation'))

        wrapper = soup_pagination.find(
            'div', class_="ProductsGrid_productCardsWrapper__IWmQO")

        if wrapper is None:
            raise AttributeError("Products grid not found — retrying...")

        product_cards = wrapper.find_all(
            'div', class_="ProductCard_productCard__g9uwD")

        return [self.BASE_URL + anchor.get('href') for product in product_cards if (anchor := product.find('a')) and anchor.get('href')]
