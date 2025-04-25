import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file


class PurinaETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "Purina"
        self.BASE_URL = "https://www.purina.co.uk"
        self.CATEGORIES = ["/dog/dog-food", "/cat/cat-food"]

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find(
                'h1', class_="dsu-product--title").get_text(strip=True)
            product_url = url.replace('https://www.bitiba.co.uk', "")
            product_description = soup.find(
                'meta', attrs={'property': 'og:description'}).get('content')
            product_rating = '0/5'

            rating_wrapper = soup.find(
                'div', attrs={'class': ['review-stats test1']})
            if rating_wrapper:
                product_rating = rating_wrapper.find(
                    'div', class_='count').getText(strip=True)

            variants = [None]
            prices = [None]
            discounted_prices = [None]
            discount_percentages = [None]

            image_urls = [', '.join([self.BASE_URL + img.find('img').get('src') for img in soup.find(
                'div', class_="carousel-media").find_all('div', class_="field__item")])]
            df = pd.DataFrame(
                {
                    "variant": variants,
                    "price": prices,
                    "discounted_price": discounted_prices,
                    "discount_percentage": discount_percentages,
                    "image_urls": image_urls
                }
            )

            df.insert(0, "url", product_url)
            df.insert(0, "description", product_description)
            df.insert(0, "rating", product_rating)
            df.insert(0, "name", product_name)
            df.insert(0, "shop", self.SHOP)

            return df

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")

    def get_links(self, category: str) -> pd.DataFrame:
        # Data validation on category
        cleaned_category = category.lower()
        if cleaned_category not in self.CATEGORIES:
            raise ValueError(
                f"Invalid category. Value must be in {self.CATEGORIES}")

        # Construct link
        category_link = f"{self.BASE_URL}{category}"

        # Placeholders for incrementing values
        current_url = category_link
        page = 0
        urls = []

        while True:
            print("Scraping", current_url)
            soup = self.extract_from_url("GET", current_url)

            if soup:
                new_urls = soup.select("a[class*='product-tile_image']")
                if new_urls:
                    new_urls = [u["href"] for u in new_urls]
                    new_urls = [self.BASE_URL+u for u in new_urls]
                    urls.extend(new_urls)

                    page += 1
                    current_url = f"{category_link}?page={page}"
                    continue

                break

            else:
                break

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)

        return df
