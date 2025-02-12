import json
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

SHOP = "Bitiba"
BASE_URL = "https://www.bitiba.co.uk"

class BitibaETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "Bitiba"
        self.BASE_URL = "https://www.bitiba.co.uk"
        self.CATEGORIES = ["/shop/dogs", "/shop/dogs_accessories", "/shop/cats", "/shop/cats_accessories", "/shop/veterinary", "/shop/small_pets"]
    
    def transform(self, soup: BeautifulSoup, url: str):

        try:

            if soup:
            
                product_data_list = soup.select("script[type*='application/ld+json']")
                if product_data_list:
                    product_data = json.loads(product_data_list[0].text)

                    product_title = product_data["name"]
                    rating = None
                    if "aggregateRating" in product_data.keys():
                        rating = product_data["aggregateRating"]["ratingValue"]
                        rating = f"{rating}/5"
                    description = product_data["description"]
                    product_url = url.replace(self.BASE_URL, "")

                    # Placeholder for variant details
                    variants = []
                    prices = []
                    discounted_prices = []
                    discount_percentages = []

                    for offer in product_data["offers"]:

                        reference_value = offer["priceSpecification"]["referenceQuantity"]["value"]
                        reference_unit = offer["priceSpecification"]["referenceQuantity"]["unitCode"]
                        variant = f"{reference_value} {reference_unit}"
                        price = offer["priceSpecification"]["price"]

                        variants.append(variant)
                        prices.append(price)

                    df = pd.DataFrame({"variant": variants, "price": prices})
                    df.insert(0, "url", product_url)
                    df.insert(0, "description", description)
                    df.insert(0, "rating", rating)
                    df.insert(0, "name", product_title)
                    df.insert(0, "shop", self.SHOP)

                    return df
        
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")

    def get_links(self, category: str) -> pd.DataFrame:
        # Data validation on category
        cleaned_category = category.lower()
        if cleaned_category not in self.CATEGORIES:
            raise ValueError(f"Invalid category. Value must be in {self.CATEGORIES}")

        # Construct link
        category_link = f"{self.BASE_URL}{cleaned_category}"

        urls = []

        # Parse request response 
        soup = self.extract_from_url("GET", category_link)
        if soup:

            # Get links from product group cards
            product_group_cards = soup.select("a[class*='ProductGroupCard_productGroupLink']")
            product_group_links = [self.BASE_URL + card["href"] for card in product_group_cards]

            for product_group_link in product_group_links:

                current_url = product_group_link

                # Loop through all the pages of the category link
                while True:

                    soup = self.extract_from_url("GET", current_url)
                    if soup:

                        products_list = soup.select("script[type*='application/ld+json']")
                        if products_list:

                            product_list_json = json.loads(products_list[-1].text)
                            if "itemListElement"in product_list_json.keys():
                                product_urls = pd.DataFrame(json.loads(products_list[-1].text)["itemListElement"])["url"].to_list()
                                urls.extend(product_urls)

                                # Repeat the process if there are new pages
                                next_page_a = soup.find("a", attrs={"data-zta": "paginationNext"})
                                if next_page_a:
                                    current_url = next_page_a["href"]
                                    continue
                    
                    # Break if there are no more pages; continue to next product group link
                    break

            df = pd.DataFrame({"url": urls})
            df.insert(0, "shop", self.SHOP)

            return df