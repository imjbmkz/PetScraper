import requests
import json
import math
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy.engine import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file
from fake_useragent import UserAgent


class PetShopOnlineETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "PetShopOnline"
        self.BASE_URL = "https://pet-shop-online.co.uk"
        self.CATEGORIES = [
            '/collections/natural-dog-treats',
            '/collections/wet-dog-food',
            '/collections/grain-free-dog-food',
            '/collections/large-breed-dog-food',
            '/collections/senior-dog-food',
            '/collections/small-breed-dog-food',
            '/collections/working-dog-food',
            '/collections/dry-dog-food',
            '/collections/better-bones-dog-treats',
            '/collections/rosewood-dog-treats',
            '/collections/dog-chews',
            '/collections/dry-puppy-food',
            '/collections/wet-puppy-food',
            '/collections/grain-free-puppy-food',
            '/collections/broth-and-meal-toppers',
            '/collections/dog-dental-hygiene',
            '/collections/dog-grooming',
            '/collections/dog-shampoo-deodorants',
            '/collections/healthcare-treatments',
            '/collections/dog-bowls',
            '/collections/dog-collars',
            '/collections/dog-leads',
            '/collections/dog-poo-bags-wipes',
            '/collections/travel-essentials',
            '/collections/balls-for-dogs',
            '/collections/soft-dog-toys',
            '/collections/tough-dog-toys',
            '/collections/rosewood-dog-toys',
            '/collections/toy-baskets',
            '/collections/dog-puppy-birthday-collection',
            '/collections/lick-mats-for-dogs',
            '/collections/dry-cat-food',
            '/collections/wet-cat-food',
            '/collections/dry-kitten-food',
            '/collections/wet-kitten-food',
            '/collections/cat-treats',
            '/collections/cat-kitten-healthcare',
            '/collections/cat-litter',
            '/collections/wild-bird-food',
            '/collections/pet-bird-food',
            '/collections/rabbit-food',
            '/collections/rabbit-treats',
            '/collections/rabbit-chews',
            '/collections/guinea-pig-food',
            '/collections/guinea-pig-treats',
            '/collections/guinea-pig-chews',
            '/collections/hamster-food',
            '/collections/hamster-treats',
            '/collections/hamster-chews',
            '/collections/small-pet-bedding'
        ]

    def get_links(self, category: str) -> pd.DataFrame:
        if category not in self.CATEGORIES:
            raise ValueError(
                f"Invalid category. Value must be in {self.CATEGORIES}")

        url = self.BASE_URL+category
        soup = self.extract_from_url("GET", url)
        n_product = int(soup.find(
            'p', class_="collection__products-count").get_text().replace(' products', '').replace(' product', ''))
        pagination_length = math.ceil(n_product / 24)
        urls = []

        for i in range(1, pagination_length + 1):
            soup_pagination = self.extract_from_url("GET", f"{url}?page={i}")
            for prod_list in soup_pagination.find('div', class_="product-list--collection").find_all('div', class_="product-item--vertical"):
                urls.append(self.BASE_URL + prod_list.find('a').get('href'))

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find(
                'h1', class_="product-meta__title").get_text()
            product_description = None

            if soup.find('div', class_="product-block-list__item--description"):
                product_description = soup.find('div', class_="product-block-list__item--description").find(
                    'div', class_="text--pull").get_text(strip=True)

            product_url = url.replace(self.BASE_URL, "")
            product_rating = '0/5'

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            headers = {
                "User-Agent": UserAgent().random,
                'Accept': 'application/json'
            }

            product_info = requests.get(url, headers=headers)

            for variant_info in product_info.json()['product']["variants"]:
                variants.append(variant_info.get('title'))
                image_urls.append(
                    soup.find('meta', attrs={'property': "og:image"}).get('content'))

                if (variant_info.get('compare_at_price') != ""):
                    price = float(variant_info.get('compare_at_price'))
                    discount_price = float(variant_info.get('price'))
                    discount_percentage = round(
                        (price - discount_price) / price, 2)

                    prices.append(price)
                    discounted_prices.append(discount_price)
                    discount_percentages.append(discount_percentage)

                else:
                    prices.append(variant_info.get('price'))
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

    def image_scrape_product(self, url):
        soup = self.extract_from_url("GET", url)

        return {
            'shop': self.SHOP,
            'url': url,
            'image_urls': soup.find('meta', attrs={'property': "og:image"}).get('content')
        }
