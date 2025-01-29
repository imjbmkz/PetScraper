import requests
import pandas as pd
from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from loguru import logger

from .utils import execute_query

class PetProductsETL(ABC):
    
    def extract(self, url: str) -> BeautifulSoup:

        self.url = url

        try:
            # Parse request response
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            logger.info(f"Successfully extracted data from {url} {response.status_code}")
            return soup
        
        # Log and raise exceptions
        except Exception as e:
            logger.error(e)
            raise e

    @abstractmethod
    def transform(self, source: str, soup: BeautifulSoup) -> pd.DataFrame:
        pass

    def load(self, product_data: pd.DataFrame, db_conn: Engine, table_name: str):
        try:
            n = product_data.shape[0]
            product_data.to_sql(table_name, db_conn, if_exists="append", index=False)
            logger.info(f"Successfully loaded {n} records to the database.")

        except Exception as e:
            logger.error(e)
            raise e
    
    def run(self, source: str, url: str, db_conn: Engine, table_name: str):
        soup = self.extract(url)
        df = self.transform(source, soup)
        self.load(df, db_conn, table_name)

        with open("sql/insert_into_pet_products.sql") as f:
            query = f.read()
            execute_query(db_conn, query)

        with open("sql/insert_into_pet_product_variants.sql") as f:
            query = f.read()
            execute_query(db_conn, query)