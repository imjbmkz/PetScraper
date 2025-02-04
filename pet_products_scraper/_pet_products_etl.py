import requests
import pandas as pd
from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from loguru import logger
import undetected_chromedriver as uc
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random,
)
import random

from .utils import execute_query, get_sql_from_file

MAX_RETRIES = 10
MAX_WAIT_BETWEEN_REQ = 2
MIN_WAIT_BETWEEN_REQ = 0
REQUEST_TIMEOUT = 30


class PetProductsETL(ABC):
    def __init__(self):
        self.session = requests.Session()

    def extract_from_driver(self, url: str) -> uc.Chrome:

        try:
            driver = uc.Chrome(headless=True, use_subprocess=False)
            driver.get(url)
            logger.info(f"Initialized web driver at {url}")
            return driver

        except Exception as e:
            logger.error(e)
            raise e
    
    @retry(
        wait=wait_random(min=MIN_WAIT_BETWEEN_REQ, max=MAX_WAIT_BETWEEN_REQ),
        stop=stop_after_attempt(MAX_RETRIES),
        retry=retry_if_exception_type(requests.RequestException),
        before_sleep=before_sleep_log(logger, "WARNING"),
        reraise=True,
    )
    def extract_from_url(self, url: str, params: dict = None, headers: dict = None) -> BeautifulSoup:
        # Parse request response
        response = self.session.get(url=url, params=params, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        logger.info(
            f"Successfully extracted data from {url} {response.status_code}"
        )
        sleep_time = random.uniform(MIN_WAIT_BETWEEN_REQ, MAX_WAIT_BETWEEN_REQ)
        logger.info(f"Sleeping for {sleep_time} seconds...")
        return soup

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