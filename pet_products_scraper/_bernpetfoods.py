import re
import json
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

class BernPetFoodsETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "BernPetFoods"
        self.BASE_URL = "https://www.bernpetfoods.co.uk"
        self.CATEGORIES = []
    
    def transform(self, soup: BeautifulSoup, url: str):
        pass

    def get_links(self, category: str) -> pd.DataFrame:
        pass