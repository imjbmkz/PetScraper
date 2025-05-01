import re
import requests
import random
import pandas as pd

from datetime import datetime as dt
from loguru import logger
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
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

import asyncio
import nest_asyncio
from playwright.async_api import async_playwright
nest_asyncio.apply()

MAX_RETRIES = 10
MAX_WAIT_BETWEEN_REQ = 1
MIN_WAIT_BETWEEN_REQ = 0.5
REQUEST_TIMEOUT = 30


class AsdaETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "ASDAGroceries"
        self.BASE_URL = "https://groceries.asda.com"
        self.CATEGORIES = [
            "/shelf/pet-food-accessories/dog-food-accessories/dog-treats-chews-biscuits/dental-treats-health-treats/1215662103573-1215680107518-1215680108312-1215684181111",
            '/shelf/pet-food-accessories/dog-food-accessories/dog-treats-chews-biscuits/natural-treats/1215662103573-1215680107518-1215680108312-1215684181112',
            "/shelf/pet-food-accessories/dog-food-accessories/dog-treats-chews-biscuits/rewarding-treats/1215662103573-1215680107518-1215680108312-1215684181113",
            "/shelf/pet-food-accessories/dog-food-accessories/dog-treats-chews-biscuits/dog-chews/1215662103573-1215680107518-1215680108312-1215685971307",
            '/shelf/pet-food-accessories/dog-food-accessories/dog-treats-chews-biscuits/dog-biscuits/1215662103573-1215680107518-1215680108312-1215685971308',
            "/shelf/pet-food-accessories/dog-food-accessories/dog-treats-chews-biscuits/meaty-treats/1215662103573-1215680107518-1215680108312-1215686353953",
            "/shelf/pet-food-accessories/dog-food-accessories/dog-treats-chews-biscuits/puppy-treats/1215662103573-1215680107518-1215680108312-1215680169706",
            "/shelf/pet-food-accessories/dog-food-accessories/wet-dog-food/dog-food-trays-pouches/1215662103573-1215680107518-1215685971325-1215685971327",
            "/shelf/pet-food-accessories/dog-food-accessories/wet-dog-food/dog-food-tins/1215662103573-1215680107518-1215685971325-1215685971328",
            "/shelf/pet-food-accessories/dog-food-accessories/dry-dog-food/dry-dog-food/1215662103573-1215680107518-1215685971326-1215685971332",
            "/shelf/pet-food-accessories/dog-food-accessories/dry-dog-food/small-dog-dry-food/1215662103573-1215680107518-1215685971326-1215685971335",
            "/aisle/pet-food-accessories/dog-food-accessories/advanced-nutrition-dog-food/1215662103573-1215680107518-1215686353954",
            "/shelf/pet-food-accessories/dog-food-accessories/natural-and-grain-free-dog-food/natural-dog-food/1215662103573-1215680107518-1215685992229-1215684791278",
            "/shelf/pet-food-accessories/dog-food-accessories/natural-and-grain-free-dog-food/grain-free-dog-food/1215662103573-1215680107518-1215685992229-1215685291916",
            "/aisle/pet-food-accessories/dog-food-accessories/premium-dog-food/1215662103573-1215680107518-1215685971336",
            "/aisle/pet-food-accessories/dog-food-accessories/puppy-food-0-2-years/1215662103573-1215680107518-1215680108559",
            "/aisle/pet-food-accessories/dog-food-accessories/senior-dog-food-7-years/1215662103573-1215680107518-1215680108507",
            "/shelf/pet-food-accessories/dog-food-accessories/small-dog-food/small-dog-wet-food/1215662103573-1215680107518-1215686354594-1215685971331",
            "/shelf/pet-food-accessories/dog-food-accessories/small-dog-food/small-dog-dry-food/1215662103573-1215680107518-1215686354594-1215685971335",
            "/aisle/pet-food-accessories/dog-food-accessories/dog-food-bigger-packs/1215662103573-1215680107518-1215680109305",
            "/shelf/pet-food-accessories/dog-food-accessories/dog-healthcare-accessories/dog-supplements/1215662103573-1215680107518-1215680109181-1215686355272",
            "/shelf/pet-food-accessories/dog-food-accessories/dog-healthcare-accessories/poo-bags-puppy-pads/1215662103573-1215680107518-1215680109181-1215680182517",
            "/shelf/pet-food-accessories/dog-food-accessories/dog-healthcare-accessories/dog-accessories-grooming/1215662103573-1215680107518-1215680109181-1215680182582",
            "/shelf/pet-food-accessories/dog-food-accessories/dog-healthcare-accessories/flea-worming-treatments/1215662103573-1215680107518-1215680109181-1215680182786",
            "/aisle/pet-food-accessories/dog-food-accessories/dog-toys/1215662103573-1215680107518-1215684881632",
            "/shelf/pet-food-accessories/dog-food-accessories/asda-hero-dog-food-treats/asda-hero-dog-food/1215662103573-1215680107518-1215686355822-1215686355823",
            "/shelf/pet-food-accessories/dog-food-accessories/asda-hero-dog-food-treats/asda-hero-dog-treats/1215662103573-1215680107518-1215686355822-1215686355824",
            "/shelf/pet-food-accessories/cat-food-accessories/cat-treats-milk/cat-biscuit-treats/1215662103573-1215680108192-1215680109365-1215680112761",
            "/shelf/pet-food-accessories/cat-food-accessories/cat-treats-milk/cat-sticks-liquid-treats/1215662103573-1215680108192-1215680109365-1215685241502",
            "/shelf/pet-food-accessories/cat-food-accessories/cat-treats-milk/natural-dental-health-treats/1215662103573-1215680108192-1215680109365-1215686353956",
            "/shelf/pet-food-accessories/cat-food-accessories/cat-treats-milk/cat-kitten-milk/1215662103573-1215680108192-1215680109365-1215680113065",
            "/shelf/pet-food-accessories/cat-food-accessories/premium-cat-food/premium-cat-food-pouches-trays/1215662103573-1215680108192-1215685241913-1215685292477",
            "/shelf/pet-food-accessories/cat-food-accessories/premium-cat-food/premium-cat-food-tins/1215662103573-1215680108192-1215685241913-1215685292478",
            "/shelf/pet-food-accessories/cat-food-accessories/wet-cat-food/cat-food-pouches/1215662103573-1215680108192-1215685971312-1215680113812",
            "/shelf/pet-food-accessories/cat-food-accessories/wet-cat-food/cat-food-tins-trays/1215662103573-1215680108192-1215685971312-1215680114029",
            "/shelf/pet-food-accessories/cat-food-accessories/wet-cat-food/premium-wet-cat-food/1215662103573-1215680108192-1215685971312-1215685971315",
            "/aisle/pet-food-accessories/cat-food-accessories/dry-cat-food-biscuits/1215662103573-1215680108192-1215685971317",
            "/aisle/pet-food-accessories/cat-food-accessories/natural-cat-food/1215662103573-1215680108192-1215685992231",
            "/aisle/pet-food-accessories/cat-food-accessories/advanced-nutrition-cat-food/1215662103573-1215680108192-1215685971321",
            "/aisle/pet-food-accessories/cat-food-accessories/kitten-food-0-1-years/1215662103573-1215680108192-1215680109568",
            "/aisle/pet-food-accessories/cat-food-accessories/senior-cat-food-7-years/1215662103573-1215680108192-1215680109466",
            "/aisle/pet-food-accessories/cat-food-accessories/cat-food-cat-litter-bigger-packs/1215662103573-1215680108192-1215680112713",
            "/aisle/pet-food-accessories/cat-food-accessories/cat-litter/1215662103573-1215680108192-1215680112617",
            "/shelf/pet-food-accessories/cat-food-accessories/cat-healthcare-accessories/cat-care-accessories/1215662103573-1215680108192-1215680112665-1215680114894",
            "/shelf/pet-food-accessories/cat-food-accessories/cat-healthcare-accessories/flea-worming-treatments/1215662103573-1215680108192-1215680112665-1215680118634",
            "/shelf/pet-food-accessories/cat-food-accessories/premium-cat-food/premium-cat-food-pouches-trays/1215662103573-1215680108192-1215685241913-1215685292477",
            "/shelf/pet-food-accessories/cat-food-accessories/premium-cat-food/premium-cat-food-tins/1215662103573-1215680108192-1215685241913-1215685292478",
            "/shelf/pet-food-accessories/cat-food-accessories/asda-tiger-cat-food-treats/asda-tiger-cat-food/1215662103573-1215680108192-1215686355825-1215686355826",
            "/shelf/pet-food-accessories/cat-food-accessories/asda-tiger-cat-food-treats/asda-tiger-cat-treats/1215662103573-1215680108192-1215686355825-1215686355827"
            "/shelf/pet-food-accessories/cat-food-accessories/cat-treats-milk/cat-biscuit-treats/1215662103573-1215680108192-1215680109365-1215680112761",
            "/shelf/pet-food-accessories/cat-food-accessories/cat-treats-milk/cat-sticks-liquid-treats/1215662103573-1215680108192-1215680109365-1215685241502",
            "/shelf/pet-food-accessories/cat-food-accessories/cat-treats-milk/natural-dental-health-treats/1215662103573-1215680108192-1215680109365-1215686353956",
            "/shelf/pet-food-accessories/cat-food-accessories/cat-treats-milk/cat-kitten-milk/1215662103573-1215680108192-1215680109365-1215680113065",
            "/shelf/pet-food-accessories/cat-food-accessories/premium-cat-food/premium-cat-food-tins/1215662103573-1215680108192-1215685241913-1215685292478",
            "/shelf/pet-food-accessories/cat-food-accessories/premium-cat-food/premium-cat-food-pouches-trays/1215662103573-1215680108192-1215685241913-1215685292477",
            "/shelf/pet-food-accessories/cat-food-accessories/wet-cat-food/premium-wet-cat-food/1215662103573-1215680108192-1215685971312-1215685971315",
            "/shelf/pet-food-accessories/cat-food-accessories/wet-cat-food/cat-food-tins-trays/1215662103573-1215680108192-1215685971312-1215680114029",
            "/shelf/pet-food-accessories/cat-food-accessories/wet-cat-food/cat-food-pouches/1215662103573-1215680108192-1215685971312-1215680113812",
            "/aisle/pet-food-accessories/cat-food-accessories/dry-cat-food-biscuits/1215662103573-1215680108192-1215685971317",
            "/aisle/pet-food-accessories/cat-food-accessories/natural-cat-food/1215662103573-1215680108192-1215685992231",
            "/aisle/pet-food-accessories/cat-food-accessories/advanced-nutrition-cat-food/1215662103573-1215680108192-1215685971321",
            "/shelf/pet-food-accessories/cat-food-accessories/cat-healthcare-accessories/flea-worming-treatments/1215662103573-1215680108192-1215680112665-1215680118634",
            "/shelf/pet-food-accessories/cat-food-accessories/cat-healthcare-accessories/cat-care-accessories/1215662103573-1215680108192-1215680112665-1215680114894",
            "/aisle/pet-food-accessories/cat-food-accessories/cat-litter/1215662103573-1215680108192-1215680112617",
            "/shelf/pet-food-accessories/cat-food-accessories/cat-food-cat-litter-bigger-packs/cat-litter-bigger-packs/1215662103573-1215680108192-1215680112713-1215680119049",
            "/shelf/pet-food-accessories/cat-food-accessories/cat-food-cat-litter-bigger-packs/cat-food-bigger-packs/1215662103573-1215680108192-1215680112713-1215680118851",
            "/aisle/pet-food-accessories/cat-food-accessories/senior-cat-food-7-years/1215662103573-1215680108192-1215680109466",
            "/aisle/pet-food-accessories/cat-food-accessories/kitten-food-0-1-years/1215662103573-1215680108192-1215680109568",
            "/shelf/pet-food-accessories/cat-food-accessories/asda-tiger-cat-food-treats/asda-tiger-cat-treats/1215662103573-1215680108192-1215686355825-1215686355827",
            "/shelf/pet-food-accessories/cat-food-accessories/asda-tiger-cat-food-treats/asda-tiger-cat-food/1215662103573-1215680108192-1215686355825-1215686355826"
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
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://groceries.asda.com",
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
                soup = BeautifulSoup(rendered_html, "html.parser")
                return soup

        except Exception as e:
            logger.error(f"An error occurred: {e}")

        finally:
            if browser:
                await browser.close()

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find(
                'h1', class_="pdp-main-details__title").get_text()

            description_wrapper = soup.find(
                'div', class_="pdp-description-reviews__product-details-cntr")
            product_description = None

            if description_wrapper:
                product_description = description_wrapper.get_text()

            product_url = url.replace(self.BASE_URL, "")
            product_rating = '0/5'
            product_wrapper = soup.find(
                'div', class_="pdp-main-details__rating")

            if product_wrapper:
                product_rating = product_wrapper.get(
                    'aria-label').split(" ")[0] + '/5'

            variant = None
            price = float(soup.find('div', class_="pdp-main-details__price-container").find('strong', {'class': [
                'co-product__price', 'pdp-main-details__price']}).find(string=True, recursive=False).strip().replace('£', ''))
            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            if soup.find('div', class_="pdp-main-details__weight"):
                variants.append(
                    soup.find('div', class_="pdp-main-details__weight").get_text())
            else:
                variants.append(variant)

            image_urls.append(
                soup.find('meta', attrs={'property': "og:image"}).get('content'))

            price = float(re.search(r"(\d+\.\d+)", soup.find('strong',
                          class_="co-product__price pdp-main-details__price").text).group(1))
            was_price_tag = soup.find(
                'span', class_="co-product__was-price pdp-main-details__was-price")

            if was_price_tag:
                real_price_text = was_price_tag.text
                real_price_match = re.search(r"(\d+\.\d+)", real_price_text)

                if real_price_match:
                    real_price = float(real_price_match.group(1))

                    prices.append(real_price)
                    discounted_prices.append(price)
                    discount_percentages.append(
                        round((real_price - price) / real_price, 2))

            else:
                prices.append(price)
                discounted_prices.append(None)
                discount_percentages.append(None)

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

        category_link = f"{self.BASE_URL}{category}"
        urls = []

        soup = asyncio.run(self.extract_scrape_content(
            category_link, '#main-content'))

        if soup.find('div', class_="co-pagination"):
            n_pages = int(
                soup.find('div', class_="co-pagination__max-page").text)

            for p in range(1, n_pages):
                soup_page_pagination = asyncio.run(
                    self.extract_scrape_content(f"{category_link}?page={p}", '#main-content'))
                for product_container in soup_page_pagination.find_all('ul', class_="co-product-list__main-cntr"):
                    for product_list in product_container.find_all('li'):
                        if product_list.find('a'):
                            urls.append(self.BASE_URL +
                                        product_list.find('a').get('href'))

        else:
            for product_container in soup.find_all('ul', class_="co-product-list__main-cntr"):
                for product_list in product_container.find_all('li'):
                    if product_list.find('a'):
                        urls.append(self.BASE_URL +
                                    product_list.find('a').get('href'))

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
                self.extract_scrape_content(url, '#main-content'))
            df = self.transform(soup, url)

            if df is not None:
                self.load(df, db_conn, table_name)
                update_url_scrape_status(db_conn, pkey, "DONE", now)
            else:
                update_url_scrape_status(db_conn, pkey, "FAILED", now)

    def image_scrape_product(self, url):
        soup = asyncio.run(self.extract_scrape_content(url, '#main-content'))

        return {
            'shop': self.SHOP,
            'url': url,
            'image_urls': soup.find('meta', attrs={'property': "og:image"}).get('content')
        }
