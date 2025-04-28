import random
import time
import pandas as pd
from loguru import logger

from pet_products_scraper import (
    ZooplusETL,
    PetsAtHomeETL,
    JollyesETL,
    LilysKitchenETL,
    BitibaETL,
    PetSupermarketETL,
    PetPlanetETL,
    PurinaETL,
    DirectVetETL,
    FishKeeperETL,
    PetDrugsOnlineETL,
    ViovetETL,
    PetShopETL,
    VetShopETL,
    VetUKETL,
    BurnsPetETL,
    AsdaETL,
    TheRangeETL,
    OcadoETL,
    HarringtonsETL,
    BernPetFoodsETL,
    PetsCornerETL,
    OrijenETL,
    ThePetExpressETL,
    PetShopOnlineETL,
    TaylorPetFoodsETL,
    TheNaturalPetStoreETL,
    HealthyPetStoreETL,
    FarmAndPetPlaceETL,
    NaturesMenuETL,
)

factory = {
    "Zooplus": ZooplusETL(),
    "PetsAtHome": PetsAtHomeETL(),
    "Jollyes": JollyesETL(),
    "LilysKitchen": LilysKitchenETL(),
    "Bitiba": BitibaETL(),
    "PetSupermarket": PetSupermarketETL(),
    "PetPlanet": PetPlanetETL(),
    "Purina": PurinaETL(),
    "DirectVet": DirectVetETL(),
    "FishKeeper": FishKeeperETL(),
    "PetDrugsOnline": PetDrugsOnlineETL(),
    "Viovet": ViovetETL(),
    "PetShop": PetShopETL(),
    "VetShop": VetShopETL(),
    "VetUK": VetUKETL(),
    "BurnsPet": BurnsPetETL(),
    "ASDAGroceries": AsdaETL(),
    "TheRange": TheRangeETL(),
    "Ocado": OcadoETL(),
    "Harringtons": HarringtonsETL(),
    "BernPetFoods": BernPetFoodsETL(),
    "PetsCorner": PetsCornerETL(),
    "Orijen": OrijenETL(),
    "ThePetExpress": ThePetExpressETL(),
    "PetShopOnline": PetShopOnlineETL(),
    "TaylorPetFoods": TaylorPetFoodsETL(),
    "TheNaturalPetStore": TheNaturalPetStoreETL(),
    "HealthyPetStore": HealthyPetStoreETL(),
    "FarmAndPetPlace": FarmAndPetPlaceETL(),
    "NaturesMenu": NaturesMenuETL(),
}


class PetImage():

    def __init__(self, file):
        self.df = pd.read_csv(file)
        self.df['full_url'] = self.df['base_url'].str[:-1] + self.df['url']
        self.valid_companies = [
            company for company in self.df['shop_name'].unique()
            if company in factory.keys()
        ]

    def run_etl(self, shop: str):
        if shop in factory:
            return factory[shop]
        else:
            raise ValueError(
                f"Shop {shop} is not supported. Please pass a valid shop.")

    def extract(self, companies: str, min_sec, max_sec):
        hard_scrape_companies = ['Zooplus', 'Bitiba']
        for c in companies:
            logger.info(f"Scraping images for {c}...")

            sample_df = self.df[self.df['shop_name'] == c]
            scrape_links = sample_df['full_url'].drop_duplicates().tolist()
            scraper = self.run_etl(c)
            scrape_payload = []
            for link in scrape_links:
                try:
                    scrape_df = scraper.image_scrape_product(link)
                    if scrape_df is not None:
                        scrape_payload.append(scrape_df)

                    if c in hard_scrape_companies:
                        sleep_time = random.uniform(60, 120)
                        logger.info(
                            f"Sleeping for {sleep_time:.2f} seconds...")
                        time.sleep(sleep_time)
                    else:
                        sleep_time = random.uniform(min_sec, max_sec)
                        logger.info(
                            f"Sleeping for {sleep_time:.2f} seconds...")
                        time.sleep(sleep_time)

                except Exception as e:
                    logger.error(f"Error scraping {link}: {e}")

            self.transform(c, scrape_payload)

    def transform(self, company: str, df: pd.DataFrame):
        df_scrape = pd.DataFrame(df)
        df_products = self.df[self.df['shop_name'] == company]

        df_merge = df_products.merge(df_scrape, how="inner",
                                     left_on="full_url", right_on="url")

        df_merge = df_merge.rename(columns={'url_x': 'url'})
        self.load(
            df_merge[['id', 'shop', 'base_url', 'url', 'variant', 'image_urls']], company)

    def load(self, df: pd.DataFrame, company: str):
        output_path = f'./csv/{company}.csv'
        df.to_csv(output_path, index=False)
        logger.success(f"Saved {company} images to {output_path}")
