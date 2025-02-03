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
    
    def transform(self, soup: BeautifulSoup, url: str):
        pass

    def get_links(self, category: str) -> pd.DataFrame:
        pass

    def run(self, db_conn: Engine, table_name: str):
        pass

    def refresh_links(self, db_conn: Engine, table_name: str):
        pass