import math
import re
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

not_include_category_links = [
    '2131-hill-s-prescription-diet',
    '2133-hill-s-science-plan',
    '2136-royal-canin-veterinary-diet',
    '3147-hill-s-vetessentials',
    '2137-specific',
    '1173925-royal-canin-veterinary-care',
    '1173933-trovet',
    '3131-hill-s-prescription-diet',
    '3133-hill-s-science-plan',
    '3134-iams',
    '3136-royal-canin-veterinary-diet',
    '3137-specific',
    '3139-hill-s-vetessentials',
    '1173895-eukanuba-veterinary-diet',
    '1173926-royal-canin-veterinary-care',
    '1173940-ideal-balance',
    '1173833-advantage',
    '1173834-advantix',
    '1173837-frontline-spot-on',
    '1173838-frontline-combo',
    '1173839-scalibor',
    '1173840-drontal',
    '1173842-flubenol',
    '1173843-panacur',
    '1173922-milbemax',
    '1173890-petosan',
    '1173851-adaptil-dap',
    '1173853-feliway',
    '1173869-aboistop',
    '1173876-mikki',
    '1173875-lupi',
    '1173871-kong',
    '1173874-halti',
    '1173863-furminator',
    '1173870-clix',
    '1173878-urine-off',
    '1173941-sureflap',
    '1173947-medical-pet-shirts',
    '1173948-ezydog',
    '1173901-glucosamine-extra',
    '1173900-coseq',
    '1173888-zentonil',
    '1173887-seraquin',
    '1173864-megaderm',
    # '3116-active',
    # '3128-ageing',
    # '322-collars',
    # '323-spray',
    # '332-pastes-and-gels'
]
# This is a Brand Category which not align with the layer type of the main category
# so mostly of these product will exist in main category too and for avoidance of
# duplicate products


class DirectVetETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "DirectVet"
        self.BASE_URL = "https://www.direct-vet.co.uk"
        self.CATEGORIES = self.get_category_links()

    def get_category_links(self):
        soup = self.extract_from_url('GET', self.BASE_URL)
        category_links = []

        row_wrapper = soup.find(
            'ul', class_='nav navbar-nav megamenu').find_all('div', class_='row')
        for wrapper in row_wrapper:
            for link in wrapper.find_all('li', class_="clearfix"):
                href = link.find('a').get('href').replace(
                    'https://www.direct-vet.co.uk/', '')
                if href not in not_include_category_links:
                    category_links.append(href)

        return category_links

    def transform(self, soup: BeautifulSoup, url: str) -> pd.DataFrame:
        product_name = soup.find('h1', itemprop="name").get_text()
        product_url = url.replace(self.BASE_URL, "")
        product_description = soup.find(
            'div', id="short_description_content").get_text(strip=True)
        product_rating = ''

        rating_wrapper = soup.find(
            'div', id="product_comments_block_extra").find('div', 'star_content')
        if (rating_wrapper):
            rating_list_wrapper = soup.find('div', id="product_comments_block_tab").find_all(
                'div', itemprop="reviewRating")
            rate_list = [int(rating.find('meta', itemprop="ratingValue").get(
                'content')) for rating in rating_list_wrapper]

            avg_rating = round(sum(rate_list) / len(rate_list), 2)
            product_rating = f"{int(avg_rating) if avg_rating.is_integer() else avg_rating}/5"
        else:
            product_rating = '0/5'

        variant_wrapper = soup.find('table', id='ct_matrix')
        variants = []
        prices = []
        discounted_prices = []
        discount_percentages = []

        if (variant_wrapper):
            for variant in variant_wrapper.find('tbody').find_all('tr'):
                variant_info = ''
                if (variant.find('td', attrs={'data-label': "Select"})):
                    variant_info = variant.find(
                        'td', attrs={'data-label': "Select"}).get_text()
                elif (variant.find('td', attrs={'data-label': "Color"})):
                    variant_info = variant.find(
                        'td', attrs={'data-label': "Color"}).get_text()
                else:
                    variant_info = variant.find(
                        'td', attrs={'data-label': "Size"}).get_text()

                variants.append(variant_info)

                if (variant.find('td', attrs={'data-label': "Price"}).find('strike')):
                    former_price = float(variant.find(
                        'td', attrs={'data-label': "Price"}).find('strike').get_text().replace('£', ''))
                    current_price = float(variant.find('td', attrs={'data-label': "Price"}).find(
                        'strong', class_="strongprice").get_text().replace('£', ''))

                    discounted_prices.append(former_price - current_price)
                    discount_percentages.append(
                        round(((former_price - current_price) / former_price) * 100, 2))
                    prices.append(float(current_price))

                else:
                    prices.append(float(variant.find(
                        'td', attrs={'data-label': "Price"}).get_text().replace('£', '')))
                    discounted_prices.append(None)
                    discount_percentages.append(None)
        else:
            variant_info = ''
            if (soup.find('div', id="short_description_content").find('h2')):
                variant_info = soup.find('div', id="short_description_content").find(
                    'h2').get_text(strip=True).replace('- ', '').replace('-', '').strip()
            elif (soup.find('div', id="short_description_content").find('p')):
                if (soup.find('div', id="short_description_content").find_all('p')[0]):
                    variant_info = soup.find('div', id="short_description_content").find_all(
                        'p')[0].get_text(strip=True).replace('- ', '').replace('-', '').strip()
                elif (soup.find('div', id="short_description_content").find_all('p')[1]):
                    variant_info = soup.find('div', id="short_description_content").find_all(
                        'p')[1].get_text(strip=True).replace('- ', '').replace('-', '').strip()
                elif (soup.find('div', id="short_description_content").find_all('p')[2]):
                    variant_info = soup.find('div', id="short_description_content").find_all(
                        'p')[2].get_text(strip=True).replace('- ', '').replace(' -', '').strip()
            else:
                variant_info = None

            variants.append(variant_info)
            prices.append(
                float(soup.find('span', itemprop="price").get_text().replace('£', '')))
            discounted_prices.append(None)
            discount_percentages.append(None)

        df = pd.DataFrame({"variant": variants, "price": prices})
        df.insert(0, "url", product_url)
        df.insert(0, "description", product_description)
        df.insert(0, "rating", product_rating)
        df.insert(0, "name", product_name)
        df.insert(0, "shop", self.SHOP)

        return df

    def get_links(self, category: str) -> pd.DataFrame:
        current_url = f"{self.BASE_URL}/{category}"
        urls = []

        soup = self.extract_from_url('GET', current_url)

        if (soup.find('small', class_="heading-counter").get_text() == 'There are no products in this category.'):
            return None

        product_count = int(re.sub(r"There is |There are | products\.| product.", "",
                                   soup.find('small', class_="heading-counter").get_text()))
        pagination_page_num = math.ceil(product_count / 12)

        for i in range(1, pagination_page_num + 1):
            page_url = f"{current_url}?selected_filters=page-{i}"

            page_pagination_source = self.extract_from_url('GET', page_url)
            for link in page_pagination_source.find_all('a', class_="product_img_link"):
                urls.append(link.get('href'))

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df

    # def run(self, db_conn: Engine, table_name: str):
    #     pass

    # def refresh_links(self, db_conn: Engine, table_name: str):
    #     pass
