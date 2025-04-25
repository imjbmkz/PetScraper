import re
import json
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file


class ThePetExpressETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "ThePetExpress"
        self.BASE_URL = "https://www.thepetexpress.co.uk"
        self.CATEGORIES = [
            "/dog-food/",
            "/puppy-beds/",
            "/puppy-bowls/",
            '/puppy-shampoo-grooming/',
            '/puppy-collars/',
            '/puppy-pads/',
            '/puppy-toys/',
            '/puppy-food/',
            '/puppy-flea-treatments/',
            '/puppy-treats-chews/',
            '/puppy-worming-treatments/',
            "/dog-beds/",
            "/dog-bowls/",
            "/dog-feeding-mats/",
            "/automatic-dog-feeders/",
            "/coats-and-clothing/",
            "/dog-collars/",
            '/dog-leads/',
            '/dog-collar-and-lead-sets/',
            '/dog-lead-accessories/',
            '/dogs-harnesses/',
            '/training-collars-leads/',
            "/dog-crates/",
            "/dog-tick-treatments/",
            "/dog-flea-treatments/",
            "/dogs-health-wormers/",
            "/dog-grooming/",
            "/dog-poop-bags-scoops/",
            "/stain-odour-removal/",
            "/dog-subscription-boxes/",
            "/dog-toys/",
            "/anti-barking/",
            '/dog-clicker-training/',
            '/dog-muzzles/',
            '/toilet-training/',
            '/puppy-training-pads/',
            '/dog-sanitary-pants-diapers/',
            '/dog-training-accessories/',
            '/treat-bags/',
            '/dog-whistles/',
            '/car-travel-accessories/',
            '/dog-travel-bags/',
            '/dog-prams-bike-trailers/',
            '/dog-carriers/',
            '/dog-flaps-and-doors/',
            '/dog-water-bottles/',
            '/dog-tethering-tie-outs/'
            '/other-dog-home-accessories/',
            "/cat-food/",
            "/kitten-scratch-posts/",
            "/kitten-toys/",
            '/kitten-treats/',
            '/kitten-worming-treatments/',
            "/outdoor-cat-kennels-shelters/",
            "/cat-beds/",
            "/cat-bowls/",
            "/cat-feeding-mats/",
            "/automatic-cat-feeders/",
            "/cat-plastic-carriers/",
            "/cat-flaps/",
            "/cat-flap-spares-accessories/",
            "/cat-flea-treatments/",
            "/cat-tick-treatments/",
            "/cats-health-wormers/",
            "/bird-food/",
            "/pigeon-food/",
            "/poultry-chicken-food/",
            "/bird-lighting/",
            '/birds-toileting-grits-and-sands/',
            '/birds-living-cage-fittings/',
            '/bird-cage-drinkers-and-feeders/',
            "/bird-cage-cleaning/",
            "/birds-health-supplements/",
            "/pigeon-supplies/",
            "/reptile-food/",
            "/reptile-vivarium-starter-kits/",
            "/vivariums-and-cabinets/",
            "/reptiles-living-heating/",
            "/light-canopies-luminaires/",
            "/mercury-vapour-lamps/",
            "/reptile-uv-bulbs-lamps/",
            "/fittings-fixtures-brackets/",
            "/vivarium-humidity/",
            "/reptiles-living-accessories/",
            "/reptile-substrates/",
            "/reptiles-feeding-drinkers-and-feeders/",
            "/reptiles-supplements/",
            "/vivarium-cleaning-equipment/",
            "/other-reptile-equipment/"
            "/small-animals-food/",
            "/bedding/",
            "/small-animals-feeding/",
            "/cages/",
            "/hutches/",
            '/small-pet-cage-accessories/',
            '/rabbit-guinea-pig-runs/',
            "/small-pet-carriers/",
            "/hammocks-hanging-beds/",
            "/cage-cleaning/",
            "/small-pet-flea-treatments/",
            "/small-pet-health-supplements/",
            "/small-pets-supplements/"
            "/small-pet-houses-and-hideaways/",
            "/small-pet-litter-trays/",
            "/small-pets-litter/",
            "/small-pet-brushes-combs/",
            "/chinchilla-dust/",
            '/small-pet-clippers-trimmers/',
            '/small-pet-shampoo/',
            "/small-animal-leads-and-harnesses/",
            "/small-animals-toys-and-exercise/",
            "/fish-food/",
            "/fish-disease-treatments/",
            "/water-conditioners/",
            '/other-aquarium-treatments/',
            "/cloudy-water-treatments/"
            "/air-pumps/",
            '/air-lines-accessories/',
            "/cleaning-equipment/",
            "/other-aquarium-accessories/"
            "/fish-tank-filters/",
            "/filter-foams-pads-sponges/",
            "/fish-tank-gravel/",
            "/aquarium-lamps-and-tubes/",
            "/aquarium-light-control-units/",
            "/aquarium-ornaments/",
            "/fish-tanks-aquariums/",
            "/pond-treatments/",
            "/wild-bird-food/",
            "/feeders/",
            "/hedgehog-other-wild-animals/",
            "/bird-nest-boxes/",
            "/squirrel-feeding/",
        ]

    def get_links(self, category: str) -> pd.DataFrame:
        if category not in self.CATEGORIES:
            raise ValueError(
                f"Invalid category. Value must be in {self.CATEGORIES}")

        url = self.BASE_URL+category
        init_soup = self.extract_from_url("GET", url)

        product_count = int(init_soup.find(
            'div', class_="pagination--count").get_text().replace(' products', ''))
        real_soup = self.extract_from_url(
            "GET", f"{url}?limit={product_count}")
        urls = [self.BASE_URL + links.find('a').get('href')
                for links in real_soup.find_all('div', class_="category-page")]

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find(
                'div', class_="page-header").find('h1').get_text()
            product_description = None

            if soup.find('div', id="reviews"):
                product_rating = soup.find('div', id="reviews").find(
                    'span', class_="average_stars").get_text(strip=True)

            product_url = url.replace(self.BASE_URL, "")
            product_rating = '0/5'

            if soup.find('div', id="reviews"):
                product_rating = soup.find('div', id="reviews").find(
                    'span', class_="average_stars").get_text(strip=True)

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []

            if soup.find('div', class_="in_page_options_option"):

                for variant in soup.find('div', class_="in_page_options_option").find_all('div', class_="sub-options"):
                    variants.append(variant.find(
                        'div', class_="inpage_option_title").get_text())

                    if variant.find('span', class_="inpage_option_rrp"):
                        price = float(variant.find(
                            'span', class_="inpage_option_rrp").get_text().replace('RRP: £', ''))
                        discount_price = float(variant.find(
                            'div', class_="ajax-price").get_text().replace('£', ''))
                        discount_percentage = round(
                            (price - float(discount_price)) / price, 2)

                        prices.append(price)
                        discounted_prices.append(discount_price)
                        discount_percentages.append(discount_percentage)

                    else:
                        prices.append(
                            float(variant.find('div', class_="ajax-price").get_text().replace('£', '')))
                        discounted_prices.append(None)
                        discount_percentages.append(None)

            else:
                variants.append(None)

                is_price_same = soup.find('span', class_="ajax-price-vat").get_text().replace(
                    '£', '') == soup.find('span', class_="ajax-rrp").get_text().replace('£', '')

                if is_price_same or soup.find('span', class_="ajax-rrp").get_text() == "£0.00":
                    prices.append(
                        float(soup.find('span', class_="ajax-price-vat").get_text().replace('£', '')))
                    discounted_prices.append(None)
                    discount_percentages.append(None)

                else:
                    price = float(
                        soup.find('span', class_="ajax-rrp").get_text().replace('£', ''))
                    discount_price = float(
                        soup.find('span', class_="ajax-price-vat").get_text().replace('£', ''))
                    discount_percentage = round(
                        (price - float(discount_price)) / price, 2)

                    prices.append(price)
                    discounted_prices.append(discount_price)
                    discount_percentages.append(discount_percentage)

            df = pd.DataFrame({"variant": variants, "price": prices,
                               "discounted_price": discounted_prices, "discount_percentage": discount_percentages})
            df.insert(0, "url", product_url)
            df.insert(0, "description", product_description)
            df.insert(0, "rating", product_rating)
            df.insert(0, "name", product_name)
            df.insert(0, "shop", self.SHOP)

            return df
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
