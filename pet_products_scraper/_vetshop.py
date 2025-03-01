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


class VetShopETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "VetShop"
        self.BASE_URL = "https://www.vetshop.co.uk"
        self.CATEGORIES = ["/Dog", "/Cat", "/other-pets", "/other-pets/bird"]

    def get_links(self, category: str) -> pd.DataFrame:
        # Data validation on category
        if category not in self.CATEGORIES:
            raise ValueError(
                f"Invalid category. Value must be in {self.CATEGORIES}")

        # Construct link
        category_link = f"{self.BASE_URL}{category}"

        urls = []

        # Parse request response
        soup = self.extract_from_url("GET", category_link)

        # Get the number of products and number of pages available
        n_products = int(soup.select_one(
            "h1[class='facets-facet-browse-title']")["data-quantity"])
        n_products_per_page = 24
        n_pages = math.ceil(n_products / n_products_per_page) + 1

        for p in range(1, n_pages):
            if p == 1:
                category_link_page = category_link
            else:
                category_link_page = f"{category_link}?page={p}"

            soup_page = self.extract_from_url("GET", category_link_page)
            if soup_page:
                product_links_a = soup_page.select(
                    "a[class='facets-item-cell-grid-link-image']")
                product_links = [self.BASE_URL + plink["href"]
                                 for plink in product_links_a]
                urls.extend(product_links)

        if urls:
            df = pd.DataFrame({"url": urls})
            df.insert(0, "shop", self.SHOP)

            return df

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find(
                'h1', class_="item-details-content-header-title").find(string=True, recursive=False).get_text()

            description_wrapper = soup.find(
                'div', id="item-details-content-container-0")
            product_description = None

            if description_wrapper:
                product_description = description_wrapper.get_text(strip=True)

            product_url = url.replace(self.BASE_URL, "")
            product_rating = '0/5'
            rating_wrapper = soup.find(
                'div', class_="product-reviews-center-container-header")

            if rating_wrapper.find('h3', class_="product-reviews-center-container-header-number"):
                product_rating = rating_wrapper.find(
                    'span', class_="global-views-star-rating-value").get_text(strip=True) + '/5'

            variant = None
            price = 0
            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []

            discount_price = None
            discount_percentage = None

            if "-" in product_name:
                variant = product_name.split("-")[1]

            variants.append(variant)

            was_price = soup.find(
                'div', class_="item-views-blb-price-options-compare-price")

            if was_price:
                price = float(was_price.find(
                    'span').get_text().replace('£', ''))
                discount_price = float(soup.find_all(
                    'p', class_="item-views-blb-price-option-price")[1].get_text().replace('£', ''))
                discount_percentage = (price - discount_price) / price
            else:
                price = float(soup.find_all(
                    'p', class_="item-views-blb-price-option-price")[1].get_text().replace('£', ''))

            prices.append(price)
            discounted_prices.append(discount_price)
            discount_percentages.append(discount_percentage)

            df = pd.DataFrame({"variant": variants, "price": prices,
                              "discounted_price": discounted_prices, "discount_percentage": discount_percentages})
            df.insert(0, "url", product_url)
            df.insert(0, "description", product_description)
            df.insert(0, "rating", product_rating)
            df.insert(0, "name", product_name)
            df.insert(0, "shop", self.SHOP)
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
