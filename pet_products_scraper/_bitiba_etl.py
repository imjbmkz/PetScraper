import re
import json
import time
import pandas as pd
from datetime import datetime as dt
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy.engine import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

from fake_useragent import UserAgent

headers = {
    "Accept": 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'max-age=0',
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 OPR/118.0.0.0",
    'Referer': 'https://www.bitiba.co.uk/',
    'Priority': "u=0, i",
    "Upgrade-Insecure-Requests": "1",
    "Connection": "keep-alive",
    "Sec-Ch-Ua": "\"Not(A:Brand\";v=\"99\", \"Opera GX\";v=\"118\", \"Chromium\";v=\"133\"",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "\"Windows\"",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1"
}


class BitibaETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "Bitiba"
        self.BASE_URL = "https://www.bitiba.co.uk"
        self.CATEGORIES = ["/shop/dogs", "/shop/dogs_accessories", "/shop/cats",
                           "/shop/cats_accessories", "/shop/veterinary", "/shop/small_pets"]

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_data_list = soup.select(
                "script[type*='application/ld+json']")
            if product_data_list:
                product_data = json.loads(product_data_list[0].text)

                product_title = product_data["name"]
                rating = '0/5'
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
                image_urls = []

                pattern = r"^.*£"
                rrb_pattern = r"[^\d\.]"

                variants_list = soup.find(
                    'div', class_="VariantList_variantList__PeaNd")
                if variants_list:
                    variant_hopps = variants_list.select(
                        "div[data-hopps*='Variant']")
                    for variant_hopp in variant_hopps:

                        variant = variant_hopp.select_one(
                            "span[class*='VariantDescription_description']").text
                        image_variant = variant_hopp.find('img').get('src')
                        discount_checker = variant_hopp.find(
                            'div', class_="z-product-price__note-wrap")

                        if discount_checker:
                            price = float(re.sub(rrb_pattern, "", variant_hopp.select_one(
                                "div[class*='z-product-price__nowrap']").text))
                            discounted_price = float(re.sub(pattern, "", variant_hopp.select_one(
                                "span[class*='z-product-price__amount']").text))
                            discount_percent = round(
                                (price - float(discounted_price)) / price, 2)
                        else:
                            price = float(re.sub(pattern, "", variant_hopp.select_one(
                                "span[class*='z-product-price__amount']").text))
                            discounted_price = None
                            discount_percent = None

                        variants.append(variant)
                        prices.append(price)
                        discounted_prices.append(discounted_price)
                        discount_percentages.append(discount_percent)
                        image_urls.append(image_variant)

                else:
                    variant = soup.select_one(
                        "div[data-zta*='ProductTitle__Subtitle']").text
                    discount_checker = soup.find('span', attrs={
                                                 'data-zta': 'SelectedArticleBox__TopSection'}).find('div', class_="z-product-price__note-wrap")

                    if discount_checker:
                        price = float(re.sub(rrb_pattern, "", soup.find('span', attrs={
                                      'data-zta': 'SelectedArticleBox__TopSection'}).find('div', class_="z-product-price__nowrap").get_text()))
                        discounted_price = float(re.sub(pattern, "", soup.find('span', attrs={
                                                 'data-zta': 'SelectedArticleBox__TopSection'}).find('span', class_="z-product-price__amount--reduced").get_text()))
                        discount_percent = round(
                            (price - float(discounted_price)) / price, 2)
                    else:
                        price = float(re.sub(pattern, "", soup.find('span', attrs={
                                      'data-zta': 'SelectedArticleBox__TopSection'}).find('span', class_="z-product-price__amount").get_text()))
                        discounted_price = None
                        discount_percent = None

                    variants.append(variant)
                    prices.append(price)
                    discounted_prices.append(discounted_price)
                    discount_percentages.append(discount_percent)
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
            raise ValueError(
                f"Invalid category. Value must be in {self.CATEGORIES}")

        # Construct link
        category_link = f"{self.BASE_URL}{cleaned_category}"

        urls = []

        # Parse request response
        soup = self.extract_from_url("GET", category_link)
        if soup:

            # Get links from product group cards
            product_group_cards = soup.select(
                "a[class*='ProductGroupCard_productGroupLink']")
            product_group_links = [self.BASE_URL + card["href"]
                                   for card in product_group_cards]

            for product_group_link in product_group_links:

                current_url = product_group_link

                # Loop through all the pages of the category link
                while True:

                    soup = self.extract_from_url("GET", current_url)
                    if soup:

                        products_list = soup.select(
                            "script[type*='application/ld+json']")
                        if products_list:

                            product_list_json = json.loads(
                                products_list[-1].text)
                            if "itemListElement" in product_list_json.keys():
                                product_urls = pd.DataFrame(json.loads(
                                    products_list[-1].text)["itemListElement"])["url"].to_list()
                                urls.extend(product_urls)

                                # Repeat the process if there are new pages
                                next_page_a = soup.find(
                                    "a", attrs={"data-zta": "paginationNext"})
                                if next_page_a:
                                    current_url = next_page_a["href"]
                                    continue

                    # Break if there are no more pages; continue to next product group link
                    break

            df = pd.DataFrame({"url": urls})
            df.insert(0, "shop", self.SHOP)

            return df

    def run(self, db_conn: Engine, table_name: str):
        sql = get_sql_from_file("select_unscraped_urls.sql")
        sql = sql.format(shop=self.SHOP)
        df_urls = self.extract_from_sql(db_conn, sql)

        for i, row in df_urls.iterrows():

            pkey = row["id"]
            url = row["url"]

            now = dt.now().strftime("%Y-%m-%d %H:%M:%S")

            soup = self.extract_from_url("GET", url, headers=headers)
            time.sleep(302)
            df = self.transform(soup, url)

            if df is not None:
                self.load(df, db_conn, table_name)
                update_url_scrape_status(db_conn, pkey, "DONE", now)

            else:
                update_url_scrape_status(db_conn, pkey, "FAILED", now)

    def image_scrape_product(self, url):
        soup = self.extract_from_url("GET", url, headers=headers)

        return {
            'shop': self.SHOP,
            'url': url,
            'image_urls': soup.find('meta', attrs={'property': "og:image"}).get('content')
        }
