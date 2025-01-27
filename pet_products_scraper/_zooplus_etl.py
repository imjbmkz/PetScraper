import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL

class ZooplusETL(PetProductsETL):
    
    def transform(self, source: str, soup: BeautifulSoup):
        
        # Get product wrappers. Each wrapper may have varying content.
        product_wrappers = soup.select('div[class*="ProductListItem_productWrapper"]')

        # Placeholder for consolidated data frames
        consolidated_data = []

        # Iterate through the wrappers
        for wrapper in product_wrappers:

            # Get the product title, rating, and description
            product_title = wrapper.select_one('a[class*="ProductListItem_productInfoTitleLink"]').text
            rating = wrapper.select_one('span[class*="pp-visually-hidden"]').text
            description = wrapper.select_one('p[class*="ProductListItem_productInfoDescription"]').text
            product_url = wrapper.select_one('a[class*="ProductListItem_productInfoTitleLink"]')["href"]

            # Get product variants. Each variant has their own price.
            product_variants = wrapper.select('div[class*="ProductListItemVariant_variantWrapper"]')

            # Placeholder for variant details
            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []

            # Get the variant name, price, and reference price
            for variant in product_variants:
                variants.append(variant.select_one('span[class*="ProductListItemVariant_variantDescription"]').text)

                price = float(variant.select_one('span[class*="z-price__amount"]').text.replace("£", ""))

                # Not all products are discounted. Sometimes, there are no reference prices
                reference_price_span = variant.select_one('span[data-zta*="productReducedPriceRefPriceAmount"]')
                if reference_price_span:

                    # Reference price is the original price before discount
                    reference_price = float(reference_price_span.text.replace("£", ""))
                    prices.append(reference_price)
                    discounted_prices.append(price)

                    # Calculate the discount percentage
                    discount_percentage = ((reference_price - price) / reference_price) 
                    discount_percentages.append(discount_percentage)

                else:
                    # If there is no reference price, then the product is not discounted
                    prices.append(price)
                    discounted_prices.append(None)
                    discount_percentages.append(None)

            
            # Compile the data acquired into dataframe
            df = pd.DataFrame(
                {
                    "variant": variants, 
                    "price": prices,
                    "discounted_price": discounted_prices,
                    "discount_percentage": discount_percentages
                }
            )
            df.insert(0, "url", product_url)
            df.insert(0, "description", description)
            df.insert(0, "rating", rating)
            df.insert(0, "name", product_title)

            consolidated_data.append(df)
        
        try:
            df_consolidated = pd.concat(consolidated_data, ignore_index=True)
            df_consolidated.insert(0, "shop", source)
            df_consolidated.insert(0, "inserted_date", datetime.now())

            return df_consolidated
        
        except Exception as e:
            logger.error(e)

    def run(self, source: str, url: str, db_conn: Engine):

        while True:

            soup = self.extract(url)
            df = self.transform(source, soup)

            if df is not None:
                self.load(df, db_conn)

            # Repeate the process if there are new pages
            next_page_a = soup.find("a", attrs={"data-zta": "paginationNext"})
            if next_page_a:
                url = next_page_a["href"]
            else:
                break