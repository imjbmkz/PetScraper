import re
import json
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file


class TaylorPetFoodsETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "TaylorPetFoods"
        self.BASE_URL = "https://www.taylorspetfoods.co.uk"
        self.CATEGORIES = [
            '/wild-dog-food-142-c.asp',
            '/naked-dog-food-123-c.asp',
            '/tribal-fresh-pressed-120-c.asp'
            '/lovel-113-c.asp',
            '/gentle-dog-food-82-c.asp',
            '/orijen-dog-food-15-c.asp',
            '/acana-dog-food-26-c.asp',
            '/natures-menu-dog-food-25-c.asp',
            '/nutriment-100-c.asp',
            '/canagan-117-c.asp',
            '/carnilove-106-c.asp',
            '/nature-diet-56-c.asp',
            '/taylors-signature-79-c.asp',
            '/collards-dog-food-112-c.asp',
            '/green-dog-dog-food-28-c.asp',
            '/skinners-dog-food-32-c.asp',
            '/autarky-dog-food-33-c.asp',
            '/taylors-own-label-dog-food-35-c.asp',
            '/bearts-dog-food-37-c.asp',
            '/gentle-dog-treats-129-c.asp',
            '/lovel-dog-treats-110-c.asp',
            '/natures-menu-132-c.asp',
            '/pet-munchies-133-c.asp',
            '/yakers-134-c.asp',
            '/whimzees-130-c.asp',
            '/tribal-rewards-127-c.asp',
            '/dog-control-23-c.asp',
            '/orijen-cat-food-59-c.asp',
            '/acana-cat-food-61-c.asp',
            '/natures-menu-cat-food-62-c.asp',
            '/canagan-118-c.asp',
            '/carnilove-107-c.asp',
            '/rosies-122-c.asp',
            '/duchess-cans--pouches-126-c.asp',
            '/taylors-own-label-121-c.asp',
            '/webbox-cat-food-70-c.asp',
            '/hi-life-96-c.asp',
            '/cat-litter-39-c.asp',
            '/rabbit-food-44-c.asp',
            '/guinea-pig-food-43-c.asp',
            '/small-furry-animal-bedding--hay-74-c.asp',
            '/chicken-feed-47-c.asp',
            '/chicken-bedding-48-c.asp',
            '/caged-bird-16-c.asp',
            '/wild-bird-seed-52-c.asp',
            '/wild-bird-fat-balls-suet-pellets--mealworms-53-c.asp',
            '/horse-75-c.asp',
            '/puppy-hour-slot-1339-p.asp'
        ]

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find(
                'div', class_="product-heading-d").find('h1').get_text()
            product_description = None

            if soup.find('div', id='tab-one'):
                product_description = soup.find(
                    'div', id='tab-one').find('span').get_text(strip=True)

            product_url = url.replace(self.BASE_URL, "")
            product_rating = '0/5'

            product_info = json.loads(
                soup.find('script', attrs={'type': 'application/ld+json'}).get_text())

            if isinstance(product_info, dict):
                product_info = [product_info]

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            for variant in product_info:
                variants.append(variant.get('name').replace(
                    f"{product_name} - ", ''))
                prices.append(variant['offers']['price'])
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

    def get_links(self, category: str) -> pd.DataFrame:
        if category not in self.CATEGORIES:
            raise ValueError(
                f"Invalid category. Value must be in {self.CATEGORIES}")

        url = self.BASE_URL+category
        soup = self.extract_from_url("GET", url)

        urls = [self.BASE_URL + '/' + product.find('a').get('href')
                for product in soup.find_all('div', class_="product-item")]
        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df
