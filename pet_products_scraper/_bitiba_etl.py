from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger
from sqlalchemy import Engine

from ._pet_products_etl import PetProductsETL
from .utils import execute_query, get_sql_from_file, update_url_scrape_status

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
