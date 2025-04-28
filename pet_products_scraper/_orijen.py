import requests
import json
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file


class OrijenETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "Orijen"
        self.BASE_URL = "https://www.orijenpetfoods.co.uk"
        self.CATEGORIES = [
            "/product-category/dog-food/",
            "/product-category/dog-food/puppy-food/",
            "/product-category/cat-food/",
            "/product-category/cat-food/kitten-food/",
        ]

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find('h1', class_="product_title").get_text()
            product_description = soup.find(
                'div', class_="badges-and-information__description").get_text(strip=True)
            product_url = url.replace(self.BASE_URL, "")
            product_rating = '0/5'
            product_id = soup.find(
                'input', attrs={'name': 'product_id'}).get('value')

            rating_wrapper = requests.get(
                f"https://api.feefo.com/api/10/reviews/summary/product?since_period=ALL&parent_product_sku={product_id}&merchant_identifier=orijen-pet-foods&origin=www.orijenpetfoods.co.uk")
            rating = rating_wrapper.json()['rating']['rating']
            product_rating = f'{rating}/5'

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            variant_list_wrapper = json.loads(soup.find(
                'form', class_="variations_form cart").get('data-product_variations'))
            for variant_list in variant_list_wrapper:
                variants.append(variant_list.get('weight_html'))
                prices.append(variant_list.get('display_price'))
                discounted_prices.append(None)
                discount_percentages.append(None)
                image_urls.append(', '.join(img.find('img').get(
                    'src') for img in soup.find_all('div', class_="gallery-slider__image")))

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

        if soup:
            atags = soup.find_all("a", "product-item__bg")
            urls = [atag["href"] for atag in atags]

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
