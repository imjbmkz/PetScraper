import re
import json
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

class AsdaETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "ASDAGroceries"
        self.BASE_URL = "https://groceries.asda.com"
        self.CATEGORIES = [
            "/dept/pet-food-accessories/dog-food-accessories/1215662103573-1215680107518",
            "/dept/pet-food-accessories/cat-food-accessories/1215662103573-1215680108192",
            "/aisle/pet-food-accessories/dog-food-accessories/dog-treats-chews-biscuits/1215662103573-1215680107518-1215680108312",
            "/aisle/pet-food-accessories/cat-food-accessories/cat-treats-milk/1215662103573-1215680108192-1215680109365",
            "/dept/pet-food-accessories/small-pets-fish-bird/1215662103573-1215662122191",
            "/shelf/pet-food-accessories/dog-food-accessories/dog-healthcare-accessories/dog-accessories-grooming/1215662103573-1215680107518-1215680109181-1215680182582",
        ]
    
    def transform(self, soup: BeautifulSoup, url: str):
        pass

    def get_links(self, category: str) -> pd.DataFrame:
        pass