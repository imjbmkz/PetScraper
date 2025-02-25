import re
import json
import math
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

class VetUKETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "VetUK"
        self.BASE_URL = "https://www.vetuk.co.uk"
        self.CATEGORIES = ["/Dog", "/Cat", "/Other-Pets", "/Other-Pets/Bird", "/Type/Own-Brand", "/Raw"]

    def get_links(self, category: str) -> pd.DataFrame:
        pass
        
    def transform(self, soup: BeautifulSoup, url: str):
        pass