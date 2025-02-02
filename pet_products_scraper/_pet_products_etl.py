import requests
import pandas as pd
from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from loguru import logger

from .utils import execute_query, get_sql_from_file

class PetProductsETL(ABC):
    
    def extract_from_url(self, url: str) -> BeautifulSoup:

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

    def extract_from_sql(self, db_conn: Engine, sql: str) -> pd.DataFrame:
        try:
            return pd.read_sql(sql, db_conn)
        
        except Exception as e:
            logger.error(e)
            raise e

    @abstractmethod
    def transform(self, soup: BeautifulSoup, url: str) -> pd.DataFrame:
        pass

    def load(self, data: pd.DataFrame, db_conn: Engine, table_name: str):
        try:
            n = data.shape[0]
            data.to_sql(table_name, db_conn, if_exists="append", index=False)
            logger.info(f"Successfully loaded {n} records to the {table_name}.")

        except Exception as e:
            logger.error(e)
            raise e
    
    def run(self, url: str, db_conn: Engine, table_name: str):
        soup = self.extract_from_url(url)
        df = self.transform(soup, url)
        self.load(df, db_conn, table_name)

        sql = get_sql_from_file("insert_into_pet_products.sql")
        execute_query(db_conn, sql)

        sql = get_sql_from_file("insert_into_pet_product_variants.sql")
        execute_query(db_conn, sql)

    @abstractmethod
    def get_links(self) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def refresh_links(self, db_conn: Engine, table_name: str):
        pass