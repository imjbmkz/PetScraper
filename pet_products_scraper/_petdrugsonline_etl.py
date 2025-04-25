import pandas as pd
import math
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file


class PetDrugsOnlineETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "PetDrugsOnline"
        self.BASE_URL = "https://www.petdrugsonline.co.uk"
        self.CATEGORIES = ['/pet-food', '/dog', '/cat', '/horse', '/small-pet']

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find(
                'h1', class_="page-title").find('span').get_text()
            product_url = url.replace(self.BASE_URL, "")

            product_description = " ".join([p.get_text(strip=True) for p in soup.find('div', class_="product-attribute-description")
                                            .find('div', class_="product-attribute-value")
                                            .find_all(['p', 'strong'])])
            product_rating = soup.find(
                'span', class_='review-summary-rating-text').get_text(strip=True)

            variant_wrapper = soup.find(
                'ul', id='custom-select-attribute-results').find_all('li')
            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            for variant in variant_wrapper:
                variants.append(variant.find(
                    'span', class_="custom-option-col-label").get_text(strip=True))
                prices.append(float(variant.find(
                    'span', class_="price-wrapper").find('span').get_text().replace('£', '')))
                image_urls.append(
                    soup.find('div', class_="product-gallery").find('img').get('src'))

                if (variant.find('span', class_="custom-option-col-inner").get_text(strip=True) != ""):
                    previous_price = float(variant.find('span', class_="custom-option-col-inner").find(
                        'span', class_='vet-price').find('span', class_='price').get_text().replace('£', ''))
                    saving_price = float(variant.find('span', class_="custom-option-col-inner").find(
                        'span', class_='saving-price').find('span', class_='price').get_text().replace('£', ''))

                    discount_percentage = round(
                        (saving_price / previous_price) * 100, 2)
                    discounted_prices.append(saving_price)
                    discount_percentages.append(discount_percentage)
                else:
                    discounted_prices.append(None)
                    discount_percentages.append(None)

            df = pd.DataFrame({"variant": variants, "price": prices,
                               "discounted_price": discounted_prices, "discount_percentage": discount_percentages, "image_urls": image_urls})
            df.insert(0, "url", product_url)
            df.insert(0, "description", product_description)
            df.insert(0, "rating", product_rating)
            df.insert(0, "name", product_name)
            df.insert(0, "shop", self.SHOP)

            return df

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")

    def get_links(self, category: str) -> pd.DataFrame:
        cleaned_category = category.lower()
        if cleaned_category not in self.CATEGORIES:
            raise ValueError(
                f"Invalid category. Value must be in {self.CATEGORIES}")

        current_url = f"{self.BASE_URL}{category}"
        urls = []

        soup = self.extract_from_url('GET', current_url)
        initial_number_product = int(
            soup.find('p', id='toolbar-amount').find_all('span')[1].get_text())
        all_product_number = int(
            soup.find('p', id='toolbar-amount').find_all('span')[2].get_text())
        times_to_click = math.ceil(all_product_number / initial_number_product)

        for i in range(1, times_to_click + 1):
            page_url = f"{current_url}?p={i}"
            page_pagination_source = self.extract_from_url('GET', page_url)
            product_list = page_pagination_source.find(
                "ol", class_="products list items product-items").find_all('li')

            for product in product_list:
                urls.append(product.find('a').get('href'))

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df

    # def run(self, db_conn: Engine, table_name: str):
    #     pass

    # def refresh_links(self, db_conn: Engine, table_name: str):
    #     pass
