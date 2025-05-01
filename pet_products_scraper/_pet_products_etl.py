import requests
import pandas as pd
from datetime import datetime as dt
from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from sqlalchemy.engine import Engine
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

from .utils import execute_query, get_sql_from_file, update_url_scrape_status

MAX_RETRIES = 10
MAX_WAIT_BETWEEN_REQ = 2
MIN_WAIT_BETWEEN_REQ = 0
REQUEST_TIMEOUT = 30


class PetProductsETL(ABC):
    def __init__(self):
        self.session = requests.Session()
        self.SHOP = ""
        self.BASE_URL = ""
        self.CATEGORIES = []

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
    def extract_from_url(self, method: str, url: str, params: dict = None, data: dict = None, headers: dict = None, verify: bool = True) -> BeautifulSoup:
        try:
            # Parse request response
            response = self.session.request(
                method=method, url=url, params=params, data=data, headers=headers, verify=verify)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            logger.info(
                f"Successfully extracted data from {url} {response.status_code}"
            )
            sleep_time = random.uniform(
                MIN_WAIT_BETWEEN_REQ, MAX_WAIT_BETWEEN_REQ)
            logger.info(f"Sleeping for {sleep_time} seconds...")
            return soup

        except Exception as e:
            logger.error(f"Error in parsing {url}: {e}")

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
            logger.info(
                f"Successfully loaded {n} records to the {table_name}.")

        except Exception as e:
            logger.error(e)
            raise e

    def run(self, db_conn: Engine, table_name: str):
        sql = get_sql_from_file("select_unscraped_urls.sql")
        sql = sql.format(shop=self.SHOP)
        df_urls = self.extract_from_sql(db_conn, sql)

        for i, row in df_urls.iterrows():

            pkey = row["id"]
            url = row["url"]

            now = dt.now().strftime("%Y-%m-%d %H:%M:%S")

            soup = self.extract_from_url("GET", url)
            df = self.transform(soup, url)

            if df is not None:
                self.load(df, db_conn, table_name)
                update_url_scrape_status(db_conn, pkey, "DONE", now)

            else:
                update_url_scrape_status(db_conn, pkey, "FAILED", now)

    @abstractmethod
    def get_links(self) -> pd.DataFrame:
        pass

    def refresh_links(self, db_conn: Engine, table_name: str):
        execute_query(db_conn, f"TRUNCATE TABLE {table_name};")

        for category in self.CATEGORIES:
            df = self.get_links(category)
            if df is not None:
                self.load(df, db_conn, table_name)

        sql = get_sql_from_file("insert_into_urls.sql")
        execute_query(db_conn, sql)
