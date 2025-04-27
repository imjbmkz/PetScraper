import re
import json
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file


class HealthyPetStoreETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "HealthyPetStore"
        self.BASE_URL = "https://healthypetstore.co.uk"
        self.CATEGORIES = [
            '/product-category/cats/cat-food/raw-cat-food/raw-minced-cat-food/',
            '/product-category/cats/cat-food/raw-cat-food/raw-bones-for-cats/',
            '/product-category/cats/cat-food/raw-cat-food/raw-meat-chunks-for-cats/',
            '/product-category/cats/cat-food/raw-cat-food/raw-offal-for-cats/',
            '/product-category/cats/cat-food/raw-cat-food/raw-ready-made-meals-frozen-cat-food/',
            '/product-category/cats/cat-food/raw-cat-food/raw-whole-prey-for-cats/',
            '/product-category/cats/cat-food/raw-cat-food/dairy-for-cats/',
            '/product-category/cats/cat-food/dry-cat-food/',
            '/product-category/cats/cat-food/wet-cat-food/',
            '/product-category/cats/cat-food/all-cat-food/',
            '/product-category/cats/oils-food-toppers-for-cats/',
            '/product-category/cats/cats-treats/cat-dry-treats/',
            '/product-category/cats/cats-treats/cat-frozen-treats/',
            '/product-category/cats/cats-treats/cat-training-treats/',
            '/product-category/cats/cats-treats/all-cat-treats/',
            '/product-category/cats/cat-meal-preparation-supplies/',
            '/product-category/cats/healthcare-for-cats/',
            '/product-category/cats/care-grooming-cats/',
            '/product-category/cats/cat-toys-puzzles/cat-soft-toys/',
            '/product-category/cats/cat-toys-puzzles/cat-scratchers/',
            '/product-category/cats/cat-toys-puzzles/cat-wands-attachments/',
            '/product-category/cats/cat-toys-puzzles/all-cat-toys-puzzles/',
            '/product-category/cats/cat-toys-puzzles/cat-interactive-toys-puzzles/',
            '/product-category/cats/cat-collars-lead-harnesses/',
            '/product-category/cats/cat-beds-harnesses/',
            '/product-category/cats/cat-training-equipment/',
            '/product-category/cats/cat-travel-equipment/',
            '/product-category/dogs/dog-food/raw-dog-food/minces/',
            '/product-category/dogs/dog-food/raw-dog-food/raw-bones-for-dogs/',
            '/product-category/dogs/dog-food/raw-dog-food/raw-meat-chunks-for-dogs/',
            '/product-category/dogs/dog-food/raw-dog-food/raw-offal-for-dogs/',
            '/product-category/dogs/dog-food/raw-dog-food/raw-fish-for-dogs/',
            '/product-category/dogs/dog-food/raw-dog-food/vegetables-fruits-for-dogs/',
            '/product-category/dogs/dog-food/raw-dog-food/ready-made-meals-dog-food/',
            '/product-category/dogs/dog-food/raw-dog-food/raw-whole-prey-for-dogs/',
            '/product-category/dogs/dog-food/raw-dog-food/dairy-for-dogs/',
            '/product-category/dogs/dog-food/dry-dog-food/',
            '/product-category/dogs/dog-food/wet-dog-food/',
            '/product-category/dogs/dog-food/all-dog-food/',
            '/product-category/dogs/oils-food-toppers-for-dogs/',
            '/product-category/dogs/dog-treats/dog-dental-chews/',
            '/product-category/dogs/dog-treats/dog-biscuits/',
            '/product-category/dogs/dog-treats/dog-chew-treat/',
            '/product-category/dogs/dog-treats/dog-pate/',
            '/product-category/dogs/dog-treats/dog-training-treats/',
            '/product-category/dogs/dog-treats/dried-meat-dog-treats/',
            '/product-category/dogs/dog-treats/frozen-dog-treats/',
            '/product-category/dogs/dog-treats/all-dog-treats/',
            '/product-category/dogs/dog-bowls-feeding-supplies/',
            '/product-category/dogs/healthcare-for-dogs/',
            '/product-category/dogs/dog-care-grooming/',
            '/product-category/dogs/do-toys-puzzles/dog-soft-toys/',
            '/product-category/dogs/do-toys-puzzles/dog-chew-toys/',
            '/product-category/dogs/do-toys-puzzles/dog-balls-tugs-frisbees/',
            '/product-category/dogs/do-toys-puzzles/dog-tug-toys/',
            '/product-category/dogs/do-toys-puzzles/dog-interactive-toys-puzzles/',
            '/product-category/dogs/do-toys-puzzles/all-dog-toys/',
            '/product-category/dogs/dog-collars-leads-and-harnesses/',
            '/product-category/dogs/dog-coats/',
            '/product-category/dogs/dog-beds-blankets/',
            '/product-category/dogs/dog-training-equipment/',
            '/product-category/dogs/dog-travel-equipment/',
            '/product-category/ferrets/ferret-raw-food/raw-minced-ferret-food/',
            '/product-category/ferrets/ferret-raw-food/raw-bones-for-ferrets/',
            '/product-category/ferrets/ferret-raw-food/raw-meaty-chunks-for-ferrets/',
            '/product-category/ferrets/ferret-raw-food/raw-whole-prey-for-ferrets/',
            '/product-category/ferrets/ferret-raw-food/raw-offal-for-ferrets/',
            '/product-category/ferrets/ferret-treats/',
            '/product-category/ferrets/ferret-care-grooming/',
            '/product-category/ferrets/healthcare-for-ferrets//product-category/ferrets/healthcare-for-ferrets/',
            '/product-category/ferrets/ferret-training-equipment/',
            '/product-category/reptiles/raw-reptile-food/'
        ]

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find('h1', class_="product_title").get_text()
            product_description = None

            if soup.find('div', class_="woocommerce-product-details__short-description"):
                product_description = soup.find(
                    'div', class_="woocommerce-product-details__short-description").get_text(strip=True)

            product_url = url.replace(self.BASE_URL, "")
            product_rating = '0/5'

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            if soup.find('form', class_="variations_form"):
                for price_data in json.loads(soup.find('form', class_="variations_form").get('data-product_variations')):
                    variants.append(price_data['attributes'].get(
                        'attribute_pa_variations-sizes') or price_data['attributes'].get('attribute_pa_size'))
                    image_urls.append(
                        soup.find('meta', attrs={'property': "og:image"}).get('content'))
                    if price_data.get('display_price') != price_data.get('display_regular_price'):
                        price = float(price_data.get('display_regular_price'))
                        discounted_price = float(
                            price_data.get('display_price'))
                        discount_percentage = "{:.2f}".format(
                            (price - discounted_price) / price)

                        prices.append(price)
                        discounted_prices.append(discounted_price)
                        discount_percentages.append(discount_percentage)
                    else:
                        prices.append(float(price_data.get('display_price')))
                        discounted_prices.append(None)
                        discount_percentages.append(None)

            else:
                variants.append(None)
                image_urls.append(
                    soup.find('meta', attrs={'property': "og:image"}).get('content'))
                if soup.find('p', class_="price").find('del'):
                    price = float(soup.find('p', class_="price").find(
                        'del').find('bdi').get_text().replace('£', ''))
                    discounted_price = float(soup.find('p', class_="price").find(
                        'ins').find('bdi').get_text().replace('£', ''))
                    discount_percentage = "{:.2f}".format(
                        (price - discounted_price) / price)

                    prices.append(price)
                    discounted_prices.append(discounted_price)
                    discount_percentages.append(discount_percentage)
                else:
                    prices.append(float(soup.find('p', class_="price").find(
                        'bdi').get_text().replace('£', '')))
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

        url = self.BASE_URL+category
        soup = self.extract_from_url("GET", f"{url}?showall=1")

        urls = [product.find('a').get('href') for product in soup.find(
            'ul', class_="products").find_all('li', class_="product")]
        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)

        return df
