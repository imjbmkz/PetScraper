import re
import json
import math
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file


class NaturesMenuETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "NaturesMenu"
        self.BASE_URL = "https://www.naturesmenu.co.uk"
        self.CATEGORIES = [
            '/dog-food/raw-dog-food',
            '/dog-food/wet-food',
            '/dog-food/dog-food-mixer',
            '/dog-food/bestsellers',
            '/dog-food/selling-fast',
            '/dog-food/treats',
            '/dog-food/raw-treats',
            '/dog-food/puppy',
            '/cat-food'
        ]

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find(
                'h2', class_="product-type").get_text() + ' ' + soup.find('h1', class_="name").get_text()
            product_description = None

            if soup.find('div', class_="description"):
                product_description = soup.find(
                    'div', class_="description").find('p').get_text()

            product_url = url.replace(self.BASE_URL, "")
            product_rating = '0/5'

            if soup.find('div', class_="pdp-feefo-product-reviews-summary-rating-border"):
                product_rating = soup.find(
                    'div', class_="pdp-feefo-product-reviews-summary-rating-border").find('p').get_text(strip=True) + "/5"

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            price_info = soup.find('button', class_="add-to-cart")

            if price_info.get('data-item-id-bundle') == 'null':
                variants.append(price_info.get('data-item-variant'))
                image_urls.append(
                    soup.find('meta', attrs={'property': "og:image"}).get('content'))
                prices.append(price_info.get('data-item-price'))
                discounted_prices.append(None)
                discount_percentages.append(None)
            else:
                variants = [price_info.get(
                    'data-item-variant'), price_info.get('data-item-variant-bundle')]
                prices = [price_info.get(
                    'data-item-price'), price_info.get('data-item-price')]
                image_urls = [soup.find('meta', attrs={'property': "og:image"}).get(
                    'content'), soup.find('meta', attrs={'property': "og:image"}).get('content')]
                discounted_prices = [None, None]
                discount_percentages = [None, None]

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

        url = self.BASE_URL+category
        soup = self.extract_from_url("GET", url)
        n_product = int(soup.find('div', id="search-result-counter-sm").get_text(
            strip=True).replace(' products', '').replace(' product', ''))
        pagination_length = math.ceil(n_product / 12)
        urls = []

        for i in range(1, pagination_length + 1):
            soup_pagination = self.extract_from_url("GET", f"{url}?page={i}")
            for prod_list in soup_pagination.find('div', class_="product-grid").find_all('div', class_="product"):
                urls.append(self.BASE_URL + prod_list.find('a').get('href'))

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
