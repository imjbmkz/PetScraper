import requests
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup

BASE_URL = "https://www.zooplus.co.uk"
CATEGORIES = ["dogs", "cats", "small_pets", "birds"]

def get_sublinks(category: str, tag_name: str = "a", class_name: str = "ProductGroupCard_productGroupLink", attribute: str = "data-pg-link"):
    
    # Data validation on category
    cleaned_category = category.lower()
    if cleaned_category not in CATEGORIES:
        raise ValueError(f"Invalid category. Value must be in {CATEGORIES}")

    # Construct link
    category_link = f"{BASE_URL}/shop/{category}"

    # Parse request response 
    response = requests.get(category_link)
    soup = BeautifulSoup(response.content, "html.parser")

    # Get all tags with matching class name
    tags = soup.select(f'{tag_name}[class*="{class_name}"]')

    # Get all links; filter out specials
    links = [f"https://www.zooplus.co.uk{tag[attribute]}" for tag in tags]
    links = [link for link in links if category_link in link]

    # Save the links to text file
    file_path = Path(f"links/{cleaned_category}.txt")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w") as file:
        for link in links:
            file.write(link + "\n")

def get_products(link: str) -> BeautifulSoup:
    # Parse request response 
    response = requests.get(link)
    soup = BeautifulSoup(response.content, "html.parser")
    
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
        reference_prices = []
        prices = []

        # Get the variant name, price, and reference price
        for variant in product_variants:
            variants.append(variant.select_one('span[class*="ProductListItemVariant_variantDescription"]').text)
            prices.append(variant.select_one('span[class*="z-price__amount"]').text.replace("£", ""))

            # Not all products are discounted, so sometimes there are no reference prices
            try:
                reference_prices.append(variant.select_one('span[data-zta*="productReducedPriceRefPriceAmount"]').text.replace("£", ""))
            except:
                reference_prices.append(None)
        
        # Compile the data acquired into dataframe
        df = pd.DataFrame(
            {
                "variant": variants, 
                "price": prices,
                "reference_price": reference_prices,
            }
        )
        df.insert(0, "product_url", product_url)
        df.insert(0, "description", description)
        df.insert(0, "rating", rating)
        df.insert(0, "product_title", product_title)

        consolidated_data.append(df)
    
    df_consolidated = pd.concat(consolidated_data, ignore_index=True)
    file_name = Path("data" + link.replace("https://www.zooplus.co.uk/shop", "").replace("?p=", "_") + ".csv")
    file_name.parent.mkdir(parents=True, exist_ok=True)
    df_consolidated.to_csv(file_name, index=False)

    return soup

def get_next_link(soup: BeautifulSoup) -> str:
    try:
        next_url = soup.find("a", attrs={"data-zta": "paginationNext"})["href"]
        return next_url
    
    except:
        return None