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
            "/puppy-essentials/",
            "/dog-beds/",
            "/dog-feeding/",
            "/coats-and-clothing/",
            "/dog-collars-and-leads/",
            "/dog-crates/",
            "/dog-collars-id-accessories/",
            "/dog-flea-tick-wormers/",
            "/dog-grooming/",
            "/dog-health-and-hygiene/",
            "/dog-poop-bags-scoops/",
            "/stain-odour-removal/",
            "/dog-subscription-boxes/",
            "/dog-toys/",
            "/dog-toileting-training/",
            "/travel-home/",
            "/cat-food/",
            "/kitten-essentials/",
            "/outdoor-cat-kennels-shelters/",
            "/outdoor-cat-kennels-shelters/",
            "/cat-beds/",
            "/cat-feeding/",
            "/cat-plastic-carriers/",
            "/cat-collars-and-leads/",
            "/cat-flaps-and-accessories/",
            "/cat-flea-tick-wormers/",
            "/birds-food/",
            "/bird-harnesses-leads/",
            "/bird-cage-accessories/",
            "/bird-cage-cleaning/",
            "/birds-health-supplements/",
            "/birds-cages/",
            "/birds-parrot-toys/",
            "/pigeon-supplies/",
            "/reptile-food/",
            "/reptile-vivarium-starter-kits/",
            "/reptile-vivarium-starter-kits/",
            "/vivariums-and-cabinets/",
            "/reptiles-living-heating/",
            "/reptile-lighting/",
            "/vivarium-humidity/",
            "/reptiles-living-accessories/",
            "/reptile-substrates/",
            "/reptile-health-care/",
            "/small-animals-food/",
            "/bedding/",
            "/small-animals-feeding/",
            "/living-spaces/",
            "/small-pet-carriers/",
            "/hammocks-hanging-beds/",
            "/small-animals-health-and-hygiene/",
            "/small-pet-houses-and-hideaways/",
            "/small-pet-toileting/",
            "/grooming/",
            "/small-animal-leads-and-harnesses/",
            "/small-animals-toys-and-exercise/",
            "/small-animal-starter-kits/",
            "/fish-food/",
            "/health-treatments/",
            "/fish-living-accessories/",
            "/filtration/",
            "/aquarium-substrates/",
            "/aquarium-lighting/",
            "/aquarium-ornaments/",
            "/fish-living-spaces/",
            "/ponds/",
            "/wild-bird-food/",
            "/birds-feeding/",
            "/hedgehog-other-wild-animals/",
            "/bird-nest-boxes/",
            "/squirrel-feeding/",
        ]
    
    def transform(self, soup: BeautifulSoup, url: str):
        pass

    def get_links(self, category: str) -> pd.DataFrame:
        pass