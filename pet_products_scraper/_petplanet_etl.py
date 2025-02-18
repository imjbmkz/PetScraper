import re
import time
import requests
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine

from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

class PetPlanetETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "PetPlanet"
        self.BASE_URL = "https://www.petplanet.co.uk"
        self.CATEGORIES = [
            "/d7/dog_food", 
            "/d2/dog_products", 
            "/d34/cat_food", 
            "/d3/cat_products", 
            "/d298/other_small_furries", 
            "/d2709/pet_health"
        ]
    
    def transform(self, soup: BeautifulSoup, url: str):
        pass

    def get_links(self, category: str) -> pd.DataFrame:
        # Data validation on category
        cleaned_category = category.lower()
        if cleaned_category not in self.CATEGORIES:
            raise ValueError(f"Invalid category. Value must be in {self.CATEGORIES}")
        
        # Construct URL
        url = f"{self.BASE_URL}{cleaned_category}"

        # Request headers 
        headers = {
            "Referer": self.BASE_URL,
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }

        # Initial request
        soup = self.extract_from_url("GET", url, headers=headers, verify=False)
        if soup:
        
            # Get number of product items
            num_items_pattern = r"Showing (\d+) items"
            num_items = int(re.search(num_items_pattern, soup.text).group(1))
            
            urls = []

            idx = 0
            step = 20
            while len(urls) < num_items:
                if soup:

                    for product in soup.find_all(class_="product-name"):
                        p_url = self.BASE_URL + product["href"]
                        p_url = urls.append(p_url)
                
                    _viewstate = soup.find("input", {"name": '__VIEWSTATE'})
                    _viewstate_value = _viewstate.get('value') if _viewstate else None
                
                    _eventtarget = soup.find("input", {"name": '__EVENTTARGET'})
                    _eventtarget_value = _eventtarget.get('value') if _eventtarget else None
                
                    _eventargument = soup.find("input", {"name": '__EVENTARGUMENT'})
                    _eventargument_value = _eventargument.get('value') if _eventargument else None
                
                    _lastfocus = soup.find("input", {"name": '__LASTFOCUS'})
                    _lastfocus_value = _lastfocus.get('value') if _lastfocus else None
                
                    _viewgenerator = soup.find("input", {"name": '__VIEWSTATEGENERATOR'})
                    _viewgenerator_value = _viewgenerator.get('value') if _viewgenerator else None
                
                    _eventvalidation = soup.find("input", {"name": '__EVENTVALIDATION'})
                    _eventvalidation_value = _eventvalidation.get('value') if _eventvalidation else None
                
                
                    params = {
                        'ctl00$Header1$SearchBox1$search_text': '',
                        'ctl00$Header1$SearchBox1$search_text_mobile': '',
                        'ctl00$ContentPlaceHolder1$ctl00$Shop1$ProdMenu1$menu_sort_list_mobile': 'sales',
                        'ctl00$ContentPlaceHolder1$ctl00$Shop1$ProdMenu1$HiddenTextClickedFilterValue': '',
                        'ctl00$ContentPlaceHolder1$ctl00$Shop1$ProdMenu1$menu_sort_list': 'price',
                        'ctl00$ContentPlaceHolder1$ctl00$Shop1$ProdMenu1$LoadMoreFlag1': '1',
                        'ctl00$ContentPlaceHolder1$ctl00$Shop1$ProdMenu1$LoadStopFlag1': '0',
                        'ctl00$ContentPlaceHolder1$ctl00$Shop1$ProdMenu1$PageSize1': idx + step,
                        'ctl00$ContentPlaceHolder1$ctl00$Shop1$ProdMenu1$PageSizeStep1': step,
                        'ctl00$Footer1$FooterSubscribePanel$subscribeEmailTextbox': '',
                        '__EVENTTARGET': _eventtarget_value,
                        '__EVENTARGUMENT': _eventargument_value,
                        '__LASTFOCUS': _lastfocus_value,
                        '__VIEWSTATE': _viewstate_value,
                        '__VIEWSTATEGENERATOR': _viewgenerator_value,
                        '__EVENTVALIDATION': _eventvalidation_value,
                        '__ASYNCPOST': 'true',
                        'ctl00$ContentPlaceHolder1$ctl00$Shop1$ProdMenu1$LoadMoreBtn1': 'Show More',
                        'popup-ctrl-ouibounce-email': ''
                    }
                    headers["Referer"] = url
                    soup = self.extract_from_url("POST", url, headers=headers, data=params, verify=False)

                    idx += step

                else:
                    break
            
            if urls:
                df = pd.DataFrame({"url": urls})
                df.drop_duplicates(inplace=True)
                df.insert(0, "shop", self.SHOP)

                logger.info(f"Scraped: {url}")

                return df

    def run(self, db_conn: Engine, table_name: str):
        pass