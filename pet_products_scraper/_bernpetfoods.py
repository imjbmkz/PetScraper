import requests
import re
import json
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file


class BernPetFoodsETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "BernPetFoods"
        self.BASE_URL = "https://www.bernpetfoods.co.uk"
        self.CATEGORIES = [
            "/product-category/dog-food/",
            "/product-category/cat-food/",
            "/product-category/cat-litter/",
        ]

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find(
                'h1', class_="product_title").get_text(strip=True)
            product_description = soup.find(
                'div', class_="description_fullcontent").get_text(separator=' ', strip=True)
            product_url = url.replace(self.BASE_URL, "")

            product_id = re.search(
                r'postid-(\d+)', ' '.join(soup.body['class'])).group(0)

            rating_wrapper = requests.get(
                f"https://api.feefo.com/api/10/reviews/summary/product?since_period=ALL&parent_product_sku={product_id}&merchant_identifier=bern-pet-foods&origin=www.bernpetfoods.co.uk")
            rating = int(rating_wrapper.json()['rating']['rating'])
            product_rating = f'{rating}/5'

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            if (soup.find('form', class_="variations_form")):
                for price_details in json.loads(soup.find('form', class_="variations_form").get('data-product_variations')):
                    variant = price_details.get('weight_html')
                    price = None
                    discounted_price = None
                    discount_percentage = None

                    if price_details.get('display_price') == price_details.get('display_regular_price'):
                        price = price_details.get('display_price')
                    else:
                        price = price_details.get('display_regular_price')
                        discounted_price = price_details.get(
                            'display_price')
                        discount_percentage = "{:.2f}".format(
                            (price - discounted_price) / price)

                    div_img = soup.find(
                        "div", class_="woocommerce-product-gallery__image")
                    image_url = None
                    if div_img:
                        image_url = div_img.find("img")["src"]
                        image_urls.append(image_url)

                    variants.append(variant)
                    prices.append(price)
                    discounted_prices.append(discounted_price)
                    discount_percentages.append(discount_percentage)
                    image_urls.append(image_url)

            else:
                variants.append(None)
                prices.append(
                    float(soup.find('p', class_="price").get_text().replace('£', '')))
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

        # Construct link
        category_link = f"{self.BASE_URL}{category}"

        urls = []
        page = 1

        while True:

            if page == 1:
                current_url = category_link
            else:
                current_url = f"{category_link}/page/{page}"

            # Parse request response
            soup = self.extract_from_url("GET", current_url)
            if soup:

                # Get all product links
                product_cards = soup.find_all("div", class_="ftc-product")
                product_links = [product_card.find(
                    "a")["href"] for product_card in product_cards]
                urls.extend(product_links)

                page += 1
                continue

            break

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df
