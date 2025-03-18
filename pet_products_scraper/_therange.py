import re
import json
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

class TheRangeETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "TheRange"
        self.BASE_URL = "https://www.therange.co.uk"
        self.CATEGORIES = [
            "/pets/dogs/",
            "/pets/cats/",
            "/pets/small-pets/",
            "/pets/aquatics/aguarium-food/",
            "/pets/aquatics/aguarium-ornaments/",
            "/pets/aquatics/aguarium-plants/",
            "/pets/fish/garden-ponds/",
            "/pets/fish/aquarium-filters/",
            "/pets/fish/aquarium-treatments-and-fish-healthcare/",
            "/pets/pet-food/",
            "/pets/pet-beds/",
        ]
    
    def transform(self, soup: BeautifulSoup, url: str):
        pass

    def get_links(self, category: str) -> pd.DataFrame:
        pass