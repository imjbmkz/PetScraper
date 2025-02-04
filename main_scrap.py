import argparse

from pet_products_scraper import scraper

parser = argparse.ArgumentParser(
    prog="Pet Products Scraper",
    description="Scrape the Zooplus website for pet products.",
)

parser.add_argument(
    "task",
    choices=["refresh", "scrape"],
    help="Identify the task to be executed. refresh=refresh the links; scrape=scraper products.",
)
parser.add_argument(
    "-c",
    "--category",
    nargs="?",
    default="all",
    help="Refresh the link of a category. Default: all categories.",
)
args = parser.parse_args()

if __name__ == "__main__":
    task = args.task
    category = args.category
    categories_to_refresh = scraper.CATEGORIES if category == "all" else [category]

    print(f"{task} task has started.")

    if args.task == "refresh":
        for cat in categories_to_refresh:
            scraper.get_sublinks(cat)

    if args.task == "scrape":
        for cat in categories_to_refresh:
            file_path = f"links/{cat}.txt"
            with open(file_path, "r") as file:
                links = file.readlines()
            for link in links:
                scraper.get_products(link.rstrip())

    print(f"{task} task has ended.")
