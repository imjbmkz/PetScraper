import pandas as pd
from dotenv import load_dotenv
from os import getenv
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file

load_dotenv()

class FishKeeperETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "FishKeeper"
        self.BASE_URL = "https://www.fishkeeper.co.uk"
        self.CATEGORIES = [
            "/aquarium-products",
            "/pond-products",
            "/marine",
        ]
        self.API_URLS = {
            "/aquarium-products":"https://43kmjyqoga-2.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.13.1)%3B%20Browser%3B%20instantsearch.js%20(4.41.0)%3B%20Magento2%20integration%20(3.8.0)%3B%20JS%20Helper%20(3.8.2)",
            "/pond-products": "https://43kmjyqoga-2.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.13.1)%3B%20Browser%3B%20instantsearch.js%20(4.41.0)%3B%20Magento2%20integration%20(3.8.0)%3B%20JS%20Helper%20(3.8.2)",
            "/marine": "https://43kmjyqoga-3.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.13.1)%3B%20Browser%3B%20instantsearch.js%20(4.41.0)%3B%20Magento2%20integration%20(3.8.0)%3B%20JS%20Helper%20(3.8.2)"
        }
        self.PAYLOADS = {
            "/aquarium-products": '{"requests":[{"indexName":"magento2_livedefault_products","params":"highlightPreTag=__ais-highlight__&highlightPostTag=__%2Fais-highlight__&page=__page__&ruleContexts=%5B%22magento_filters%22%2C%22magento-category-2682%22%5D&hitsPerPage=36&clickAnalytics=true&enablePersonalization=true&userToken=anonymous-2f5a3ebc-8310-465f-aa6c-8df08982e912&maxValuesPerFacet=16&facets=%5B%22item_includes%22%2C%22decor%22%2C%22pond_plant_type%22%2C%22product_types%22%2C%22brand%22%2C%22available_online%22%2C%22filter_type%22%2C%22volume_range%22%2C%22length_range%22%2C%22flow_rate%22%2C%22fish_type%22%2C%22food_type%22%2C%22lighting_type%22%2C%22plant_type%22%2C%22wattage%22%2C%22test_type%22%2C%22price.GBP.default%22%5D&tagFilters=&facetFilters=%5B%22categories.level0%3AAquarium%20Products%22%5D&numericFilters=%5B%22visibility_catalog%3D1%22%5D"}]}',
            "/pond-products": '{"requests":[{"indexName":"magento2_livedefault_products","params":"highlightPreTag=__ais-highlight__&highlightPostTag=__%2Fais-highlight__&page=__page__&ruleContexts=%5B%22magento_filters%22%2C%22magento-category-2704%22%5D&hitsPerPage=36&clickAnalytics=true&enablePersonalization=true&userToken=anonymous-a44320cb-7e97-4b2d-9152-f56a91c97057&maxValuesPerFacet=16&facets=%5B%22item_includes%22%2C%22decor%22%2C%22pond_plant_type%22%2C%22product_types%22%2C%22brand%22%2C%22available_online%22%2C%22filter_type%22%2C%22volume_range%22%2C%22length_range%22%2C%22flow_rate%22%2C%22fish_type%22%2C%22food_type%22%2C%22lighting_type%22%2C%22plant_type%22%2C%22wattage%22%2C%22test_type%22%2C%22price.GBP.default%22%5D&tagFilters=&facetFilters=%5B%22categories.level0%3APond%20Products%22%5D&numericFilters=%5B%22visibility_catalog%3D1%22%5D"}]}',
            "/marine": '{"requests":[{"indexName":"magento2_livedefault_products","params":"highlightPreTag=__ais-highlight__&highlightPostTag=__%2Fais-highlight__&page=__page__&ruleContexts=%5B%22magento_filters%22%2C%22magento-category-3746%22%5D&hitsPerPage=36&clickAnalytics=true&enablePersonalization=true&userToken=anonymous-a44320cb-7e97-4b2d-9152-f56a91c97057&maxValuesPerFacet=16&facets=%5B%22item_includes%22%2C%22decor%22%2C%22pond_plant_type%22%2C%22product_types%22%2C%22brand%22%2C%22available_online%22%2C%22filter_type%22%2C%22volume_range%22%2C%22length_range%22%2C%22flow_rate%22%2C%22fish_type%22%2C%22food_type%22%2C%22lighting_type%22%2C%22plant_type%22%2C%22wattage%22%2C%22test_type%22%2C%22price.GBP.default%22%5D&tagFilters=&facetFilters=%5B%22categories.level0%3AMarine%20Products%22%5D&numericFilters=%5B%22visibility_catalog%3D1%22%5D"}]}'
        }

        X_ALGOLIA_API_KEY = getenv("X-ALGOLIA-API-KEY")

        self.HEADERS = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.7',
            'Connection': 'keep-alive',
            'Origin': 'https://www.fishkeeper.co.uk',
            'Referer': 'https://www.fishkeeper.co.uk/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site',
            'Sec-GPC': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
            'content-type': 'application/x-www-form-urlencoded',
            'sec-ch-ua': '"Not(A:Brand";v="99", "Brave";v="133", "Chromium";v="133"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'x-algolia-api-key': X_ALGOLIA_API_KEY,
            'x-algolia-application-id': '43KMJYQOGA',
        }
    
    def transform(self, soup: BeautifulSoup, url: str):
        pass

    def get_links(self, category: str) -> pd.DataFrame:
        
        # Construct category link and get base payload
        category_link = self.BASE_URL + category
        category_payload = self.PAYLOADS[category]
        category_api = self.API_URLS[category]
        soup = self.extract_from_url("GET", category_link)

        # Get estimated number of pages
        n_products = int(soup.select_one("span[class*='toolbar-number']").text)
        n_pages = int(round(n_products / 36,0))

        urls = []

        for i in range(n_pages):
            category_payload_parsed = category_payload.replace("__page__", str(i))

            response = self.session.post(category_api, headers=self.HEADERS, data=category_payload_parsed)

            try:
                response.raise_for_status()
                new_urls = pd.DataFrame(response.json()["results"][0]["hits"])["url"].to_list()
                urls.extend(new_urls)

            except Exception as e:
                logger.error(f"Error scraping {category_api} page {i}: {e}")

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df