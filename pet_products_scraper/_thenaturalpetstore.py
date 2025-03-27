import re
import json
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

class TheNaturalPetStoreETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "TheNaturalPetStore"
        self.BASE_URL = "https://www.thenaturalpetstore.co.uk"
        self.CATEGORIES = [
            "/collections/dog-food",
            "/collections/dog-treats",
            "/collections/dog-toys",
            "/collections/dog-accessories",
            "/collections/dog-health",
            "/collections/cat-food",
            "/collections/cat-treats",
            "/collections/cat-toys",
            "/collections/cat-accessories",
            "/collections/cat-health",
            "/collections/cat-litter",
            "/collections/small-pet-food",
            "/collections/small-pet-treats",
            "/collections/small-pet-accessories",
            "/collections/bird-food",
            "/collections/bird-toys",
            "/collections/bird-treats",
        ]
    
    def transform(self, soup: BeautifulSoup, url: str):
        pass

    def get_links(self, category: str) -> pd.DataFrame:
        pass