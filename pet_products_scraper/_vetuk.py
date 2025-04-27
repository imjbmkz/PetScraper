import re
import json
import math
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file


class VetUKETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "VetUK"
        self.BASE_URL = "https://www.vetuk.co.uk"
        self.CATEGORIES = []
        # self.CATEGORIES = ["/Dog", "/Cat", "/Other-Pets",
        #                   "/Other-Pets/Bird", "/Type/Own-Brand", "/Raw"]

    def get_links(self, category: str) -> pd.DataFrame:
        soup = self.extract_from_url('get', category)
        urls = []
        if (soup.find('div', class_='category-box')):
            category_url = soup.find_all('div', class_="category-box")[1:]
            for url in category_url:
                product_links_soup = self.extract_from_url(
                    'get', url.find('a').get('href'))

                for product_url in product_links_soup.find_all('h3', class_="itemTitle"):
                    urls.append(product_url.find('a').get('href'))
        else:
            product_list = soup.find_all('h3', class_="itemTitle")
            for url in product_list:
                urls.append(url.find('a').get('href'))

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)

        return df

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            variant_wrapper = soup.find_all('div', class_="priceOption")
            if soup.find(string="(Sold Out)"):
                variant_len = len(variant_wrapper)
                variant_sold_out = 0
                for variant in variant_wrapper:
                    if variant.find('span', string="(Sold Out)"):
                        variant_sold_out += 1

                if variant_sold_out == variant_len:
                    logger.info(f"Skipping {url} as it is sold out. ")
                    return None

            product_name = soup.find(
                'div', id="product-name").find('h1').get_text()
            product_url = url.replace(self.BASE_URL, "")
            product_description_wrapper = soup.find(
                'div', class_="products-description").find_all('p')
            descriptions = []
            for description_wrap in product_description_wrapper:
                if (description_wrap.find('span') == None or description_wrap.find('strong') == None):
                    descriptions.append(description_wrap.get_text())

            product_description = ' '.join(descriptions)

            rating_wrapper = soup.find('div', id='reviews')
            rating_count = int(rating_wrapper.find(
                'h3').get_text().replace('Reviews (', '').replace(')', ''))
            product_rating = ''
            if (rating_count > 0):
                product_rating = f'{rating_wrapper.find("span", class_="star-rating-widget").get("data-rating")}/5'
            else:
                product_rating = '0/5'

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            for v in variant_wrapper:
                if "(Sold Out)" in v.find('span').get_text():
                    continue

                variant_name = ''
                price = 0

                if soup.find('select', id="attribute-selector"):
                    variant_name = v.find(
                        'p', class_='displayOptionName').get_text()
                else:
                    if "(" in product_name and ")" in product_name:
                        variant_name = product_name.split(
                            '(')[1].replace(')', '')
                    else:
                        variant_name = soup.find(
                            'p', class_="manufacturer-name").get_text().replace('Manufacturer: ', '')

                variants.append(variant_name)
                image_urls.append(
                    soup.find('img', class_="product-image-main").get('src'))

                if v.find('span', class_='retailPrice'):
                    if "Now: £" in v.find('span', class_='retailPrice').get_text():
                        price = v.find('span', class_='retailPrice').get_text().replace(
                            'Now: £', '')
                    else:
                        price = v.find(
                            'span', class_='retailPrice').get_text().replace('£', '')

                    prices.append(price)
                else:
                    prices.append(None)

                discount_percenrage = None
                if v.find('span', class_="discountSaving"):
                    if "Save: " in v.find('span', class_='discountSaving').get_text():
                        discount_percenrage = v.find('span', class_='discountSaving').get_text(
                        ).replace('Save: ', '').replace('%', '')
                    else:
                        discount_percenrage = v.find(
                            'span', class_='discountSaving').get_text().replace('£', '')

                discount_percentages.append(discount_percenrage)

                discount_price = None
                if v.find('span', class_="wasPrice"):
                    if "Was: £" in v.find('span', class_='wasPrice').get_text():
                        discount_price = v.find(
                            'span', class_='wasPrice').get_text().replace('Was: £', '')
                    else:
                        discount_price = v.find(
                            'span', class_='wasPrice').get_text().replace('£', '')

                    discounted_prices.append(float(discount_price))
                else:
                    discounted_prices.append(discount_price)

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

    def refresh_links(self, db_conn: Engine, table_name: str):
        execute_query(db_conn, f"TRUNCATE TABLE {table_name};")
        logger.info("Gathering the categories links ..")
        soup = self.extract_from_url('get', self.BASE_URL)
        url_links_query = [
            soup.find_all('div', class_='menu-list')[2].find_all('dd'),  # Dog
            soup.find_all('div', class_='menu-list')[3].find_all('dd'),  # Cat
            soup.find_all(
                'div', class_='menu-list')[4].find_all('dd'),  # Horses
            # Small Animals
            soup.find_all('div', class_='menu-list')[5].find_all('dd'),
            soup.find_all('div', class_='menu-list')[6].find_all('dd'),  # Fish
            soup.find_all(
                'div', class_='menu-list')[7].find_all('dd'),  # Birds
            # Veterinary Supplies
            soup.find_all('div', class_='menu-list')[8].find_all('dd')
        ]

        for url_links in url_links_query:
            for url in url_links:
                self.CATEGORIES.append(url.find('a').get('href'))

        logger.info("Finished gathering category links...")

        for category in self.CATEGORIES:
            df = self.get_links(category)
            if df is not None:
                self.load(df, db_conn, table_name)

        sql = get_sql_from_file("insert_into_urls.sql")
        execute_query(db_conn, sql)
