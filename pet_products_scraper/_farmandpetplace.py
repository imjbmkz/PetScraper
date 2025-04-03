import re
import requests
import math
import pandas as pd
from datetime import datetime
from loguru import logger
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from ._pet_products_etl import PetProductsETL
from .utils import execute_query, update_url_scrape_status, get_sql_from_file


class FarmAndPetPlaceETL(PetProductsETL):

    def __init__(self):
        super().__init__()
        self.SHOP = "FarmAndPetPlace"
        self.BASE_URL = "https://www.farmandpetplace.co.uk"
        self.CATEGORIES = [
            '/shop/products/pet/dog/dog-food/dry-dog-food/dr-green-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/diamond-naturals/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/canagan-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/tribal-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/carnilove-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/fish4dogs/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/go-native-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/leader-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/orijen-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/yora-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/acana/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/chudleys/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/dr-john-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/arden-grange/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/burns-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/eukanuba/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/beta/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/gelert/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/autarky-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/chappie/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/harringtons/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/iams/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/james-wellbeloved/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/lilys-kitchen/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/natures-menu/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/natures-variety-dry-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/pero/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/pro-plan/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/red-mills/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/royal-canin-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/skinners-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/symply-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/truline-dog/page-1.html',
            '/shop/products/pet/dog/dog-food/dry-dog-food/vitalin/page-1.html',
            '/shop/products/pet/dog/dog-food/cold-pressed-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/natural-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/grain-free-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/small-breed-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/large-breed-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/working-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/wet-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/raw-dog-food/naked-dog-raw/page-1.html',
            '/shop/products/pet/dog/dog-food/raw-dog-food/naturaw-raw-feed/page-1.html',
            '/shop/products/pet/dog/dog-food/raw-dog-food/natures-menu-raw/page-1.html',
            '/shop/products/pet/dog/dog-food/raw-dog-food/raw-supplement/page-1.html',
            '/shop/products/pet/dog/dog-food/tinned-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/puppy-food/page-1.html',
            '/shop/products/pet/dog/dog-food/medium-breed-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/senior-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-food/mixer-dog-food/page-1.html',
            '/shop/products/pet/dog/dog-beds/morris-and-co/page-1.html',
            '/shop/products/pet/dog/dog-beds/soft-dog-beds/page-1.html',
            '/shop/products/pet/dog/dog-beds/vet-bed/page-1.html',
            '/shop/products/pet/dog/dog-beds/plastic-dog-beds/page-1.html',
            '/shop/products/pet/dog/dog-beds/dog-mattress-and-duvet/page-1.html',
            '/shop/products/pet/dog/dog-beds/waterproof-dog-beds/page-1.html',
            '/shop/products/pet/dog/dog-beds/dog-blankets/page-1.html',
            '/shop/products/pet/dog/dog-leads-and-collars/dog-leads/leather-dog-leads/page-1.html',
            '/shop/products/pet/dog/dog-leads-and-collars/dog-leads/nylon-dog-leads/page-1.html',
            '/shop/products/pet/dog/dog-leads-and-collars/dog-leads/rope-dog-lead/page-1.html',
            '/shop/products/pet/dog/dog-leads-and-collars/dog-collars/leather-dog-collars/page-1.html',
            '/shop/products/pet/dog/dog-leads-and-collars/dog-collars/nylon-dog-collars/page-1.html',
            '/shop/products/pet/dog/dog-leads-and-collars/dog-collars/safety-collars/page-1.html',
            '/shop/products/pet/dog/dog-leads-and-collars/dog-harnesses/page-1.html',
            '/shop/products/pet/dog/dog-leads-and-collars/retractable-dog-leads/page-1.html',
            '/shop/products/pet/dog/dog-leads-and-collars/training-and-muzzles/page-1.html',
            '/shop/products/pet/dog/dog-leads-and-collars/puppy-sets/page-1.html',
            '/shop/products/pet/dog/dog-leads-and-collars/accessories/page-1.html',
            '/shop/products/pet/dog/dog-leads-and-collars/tie-out/page-1.html',
            '/shop/products/pet/dog/dog-grooming-and-shampoos/animology/page-1.html',
            '/shop/products/pet/dog/dog-grooming-and-shampoos/scissors-blades-and-clippers/page-1.html',
            '/shop/products/pet/dog/dog-grooming-and-shampoos/brushes-and-combs/page-1.html',
            '/shop/products/pet/dog/dog-grooming-and-shampoos/towels/page-1.html',
            '/shop/products/pet/dog/dog-grooming-and-shampoos/shampoos-and-sprays/page-1.html',
            '/shop/products/pet/dog/dog-grooming-and-shampoos/luxury-spa/page-1.html',
            '/shop/products/pet/dog/dog-high-visibility-and-reflective-wear/page-1.html',
            '/shop/products/pet/dog/dog-medication-and-health/dog-calming/page-1.html',
            '/shop/products/pet/dog/dog-medication-and-health/dental-care/page-1.html',
            '/shop/products/pet/dog/dog-medication-and-health/skin/page-1.html',
            '/shop/products/pet/dog/dog-medication-and-health/ear/page-1.html'
            '/shop/products/pet/dog/dog-medication-and-health/eye/page-1.html',
            '/shop/products/pet/dog/dog-medication-and-health/flea/page-1.html',
            '/shop/products/pet/dog/dog-medication-and-health/dog-wormers/page-1.html',
            '/shop/products/pet/dog/dog-medication-and-health/health-and-supplements/page-1.html',
            '/shop/products/pet/dog/dog-medication-and-health/hygiene-and-training/page-1.html',
            '/shop/products/pet/dog/dog-coats-and-clothing/dog-coats/waterproof-dog-coats/page-1.html',
            '/shop/products/pet/dog/dog-coats-and-clothing/dog-coats/reflective-dog-coats/page-1.html',
            '/shop/products/pet/dog/dog-coats-and-clothing/dog-coats/standard-dog-coats/page-1.html',
            '/shop/products/pet/dog/dog-coats-and-clothing/dog-coats/sweaters/page-1.html',
            '/shop/products/pet/dog/dog-coats-and-clothing/dog-coats/cooling-and-calming-dog-coats/page-1.html',
            '/shop/products/pet/dog/dog-feeders/dog-bowls/page-1.html',
            '/shop/products/pet/dog/dog-feeders/storage-and-accessories/page-1.html',
            '/shop/products/pet/dog/dog-feeders/lick-mats/page-1.html',
            '/shop/products/pet/dog/dog-feeders/placemats/page-1.html',
            '/shop/products/pet/dog/dog-feeders/scruffs-dog-bowls/page-1.html',
            '/shop/products/pet/dog/dog-feeders/stainless-steel-dog-bowls/page-1.html',
            '/shop/products/pet/dog/dog-feeders/travel-dog-feeders/page-1.html',
            '/shop/products/pet/dog/dog-chews-and-dog-treats/natural-dog-treats/page-1.html',
            '/shop/products/pet/dog/dog-chews-and-dog-treats/hide-dog-chews/page-1.html',
            '/shop/products/pet/dog/dog-chews-and-dog-treats/dental-dog-chews/page-1.html',
            '/shop/products/pet/dog/dog-chews-and-dog-treats/packet-dog-treats/page-1.html',
            '/shop/products/pet/dog/dog-chews-and-dog-treats/puppy-treats/page-1.html',
            '/shop/products/pet/dog/dog-chews-and-dog-treats/chocolate-dog-treats/page-1.html',
            '/shop/products/pet/dog/dog-chews-and-dog-treats/grain-free-dog-treats/page-1.html',
            '/shop/products/pet/dog/dog-chews-and-dog-treats/dog-bones/page-1.html',
            '/shop/products/pet/dog/dog-chews-and-dog-treats/dog-biscuits/page-1.html',
            '/shop/products/pet/dog/dog-chews-and-dog-treats/munchy-dog-chews/page-1.html',
            '/shop/products/pet/dog/dog-chews-and-dog-treats/health-joint-and-mobility/page-1.html',
            '/shop/products/pet/dog/dog-chews-and-dog-treats/novelty-dog-treats/page-1.html',
            '/shop/products/pet/dog/dog-summer/cooling/page-1.html',
            '/shop/products/pet/dog/dog-training/berts-bows/page-1.html',
            '/shop/products/pet/dog/dog-training/toilet-training/page-1.html',
            '/shop/products/pet/dog/dog-training/dog-muzzles/page-1.html',
            '/shop/products/pet/dog/dog-training/other-dog-training/page-1.html',
            '/shop/products/pet/dog/dog-training/anxiety-solutions/page-1.html',
            '/shop/products/pet/dog/dog-travel/page-1.html',
            '/shop/products/pet/dog/dog-carriers-and-kennels/carriers-and-crates/page-1.html',
            '/shop/products/pet/dog/dog-carriers-and-kennels/runs-and-pens/page-1.html',
            '/shop/products/pet/dog/scruffs-pet-range/page-1.html',
            '/shop/products/pet/dog/dog-toys/fetch-dog-toys/page-1.html',
            '/shop/products/pet/dog/dog-toys/kong-dog-toys/page-1.html',
            '/shop/products/pet/dog/dog-toys/latex-rubber-and-vinyl-dog-toys/page-1.html',
            '/shop/products/pet/dog/dog-toys/no-fill-dog-toys/page-1.html',
            '/shop/products/pet/dog/dog-toys/novelty-dog-toys/page-1.html',
            '/shop/products/pet/dog/dog-toys/nylabone-toys/page-1.html',
            '/shop/products/pet/dog/dog-toys/puppy-toys/page-1.html',
            '/shop/products/pet/dog/dog-toys/rope-dog-toys/page-1.html',
            '/shop/products/pet/dog/dog-toys/soft-dog-toys/page-1.html',
            '/shop/products/pet/dog/dog-toys/tough-dog-toys/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/canagan-brand-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/carnilove-brand-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/fish-4-cats-brand-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/royal-canin-brand-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/wellness-core-cat/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/diamond-naturals-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/felix-brand-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/whiskas-brand-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/dr-green-brand-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/breederpack-brand-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/burgess-brand-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/duchess-brand-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/catit-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/go-cat-brand-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/gourmet-brand-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/harringtons-brand-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/iams-brand-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/james-wellbeloved-brand-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/lilys-kitchen-brand-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/natures-menu-brand-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/natures-variety-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/purina-one-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/pro-plan-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/cat-food-by-brand/sheba-brand-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/tinned-cat-food/animonda-tinned-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/tinned-cat-food/breederpack-tinned-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/tinned-cat-food/canagan-tinned-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/tinned-cat-food/carnilove-tinned-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/tinned-cat-food/duchess-tinned-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/tinned-cat-food/fish-4-cats-tinned-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/tinned-cat-food/natures-menu-tinned-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/senior-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/kitten-food/page-1.html',
            '/shop/products/pet/cat/cat-food/dry-cat-food/acana-dry-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/dry-cat-food/breederpack-dry-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/dry-cat-food/burgess-dry-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/dry-cat-food/canagan-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/dry-cat-food/carnilove-dry-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/dry-cat-food/duchess-dry-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/dry-cat-food/extra-select-dry-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/dry-cat-food/go-cat-dry-food/page-1.html',
            '/shop/products/pet/cat/cat-food/dry-cat-food/harringtons-dry-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/dry-cat-food/iams-dry-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/dry-cat-food/james-wellbeloved-dry-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/dry-cat-food/purina-one-dry-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/dry-cat-food/pro-plan-dry-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/dry-cat-food/royal-canin-dry-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/dry-cat-food/whiskas-dry-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/wet-cat-food/canagan-wet-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/wet-cat-food/carnilove-wet-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/wet-cat-food/felix-wet-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/wet-cat-food/fish-4-cats-wet-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/wet-cat-food/gourmet-wet-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/wet-cat-food/iams-wet-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/wet-cat-food/lilys-kitchen-wet-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/wet-cat-food/natures-menu-wet-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/wet-cat-food/pro-plan-wet-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/wet-cat-food/purina-one-wet-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/wet-cat-food/royal-canin-wet-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/wet-cat-food/sheba-wet-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-food/wet-cat-food/whiskas-wet-cat-food/page-1.html',
            '/shop/products/pet/cat/cat-health-and-hygiene/cat-calming-solutions/page-1.html',
            '/shop/products/pet/cat/cat-health-and-hygiene/cat-grooming/page-1.html',
            '/shop/products/pet/cat/cat-health-and-hygiene/cat-wormers/page-1.html',
            '/shop/products/pet/cat/cat-health-and-hygiene/cat-flea-treatment/page-1.html',
            '/shop/products/pet/cat/cat-health-and-hygiene/cat-health-and-supplements/page-1.html',
            '/shop/products/pet/cat/cat-toys/cat-scratchers/page-1.html',
            '/shop/products/pet/cat/cat-toys/toys/page-1.html',
            '/shop/products/pet/cat/cat-toys/kong-cat-toys/page-1.html',
            '/shop/products/pet/cat/cat-feeders/cat-bowls/page-1.html',
            '/shop/products/pet/cat/cat-treats/healthcare-treats/page-1.html',
            '/shop/products/pet/cat/cat-treats/meaty-and-fish-treats/page-1.html',
            '/shop/products/pet/cat/cat-treats/biscuit-treats/page-1.html',
            '/shop/products/pet/cat/cat-treats/kitten-milk-and-treats/page-1.html',
            '/shop/products/pet/cat/cat-treats/cat-milk-and-treats/page-1.html',
            '/shop/products/pet/cat/cat-treats/soft-treats/page-1.html',
            '/shop/products/pet/cat/cat-treats/catnip-and-grass/page-1.html',
            '/shop/products/pet/cat/cat-litter-and-trays/cat-litter/page-1.html',
            '/shop/products/pet/cat/cat-litter-and-trays/cat-litter-accessories/page-1.html',
            '/shop/products/pet/cat/cat-litter-and-trays/litter-trays/page-1.html',
            '/shop/products/pet/cat/cat-leads-and-collars/cat-collars/page-1.html',
            '/shop/products/pet/cat/cat-leads-and-collars/cat-harnesses/page-1.html',
            '/shop/products/pet/cat/cat-leads-and-collars/hi-vis-cat-collars/page-1.html',
            '/shop/products/pet/cat/cat-beds/page-1.html',
            '/shop/products/pet/cat/cat-carriers-and-flaps/cat-carriers/page-1.html',
            '/shop/products/pet/small-pet/food-and-treats/rabbit/page-1.html',
            '/shop/products/pet/small-pet/food-and-treats/hamster/page-1.html',
            '/shop/products/pet/small-pet/food-and-treats/mouse-and-rat/page-1.html',
            '/shop/products/pet/small-pet/food-and-treats/gerbil/page-1.html',
            '/shop/products/pet/small-pet/food-and-treats/mixed-animal-treats/rosewood-naturals/page-1.html',
            '/shop/products/pet/small-pet/food-and-treats/mixed-animal-treats/tiny-friends-selective/page-1.html',
            '/shop/products/pet/small-pet/food-and-treats/mixed-animal-treats/vitakraft-nibblots-johnsons/page-1.html',
            '/shop/products/pet/small-pet/food-and-treats/guinea-pig/page-1.html',
            '/shop/products/pet/small-pet/food-and-treats/ferret/page-1.html',
            '/shop/products/pet/small-pet/food-and-treats/chinchilla/page-1.html',
            '/shop/products/pet/small-pet/food-and-treats/grasses-and-hay/page-1.html',
            '/shop/products/pet/small-pet/food-and-treats/grasses-and-hay/page-1.html',
            '/shop/products/pet/small-pet/medication-and-health/health-and-hygiene/page-1.html',
            '/shop/products/pet/small-pet/eating-and-drinking-accessories/page-1.html',
            '/shop/products/pet/small-pet/leads-and-harnesses/page-1.html',
            '/shop/products/pet/small-pet/housing-and-carriers/cages/page-1.html',
            '/shop/products/pet/small-pet/housing-and-carriers/carriers-for-small-animal/page-1.html',
            '/shop/products/pet/small-pet/housing-and-carriers/hutches-and-runs/page-1.html',
            '/shop/products/pet/small-pet/bedding-and-litters/page-1.html',
            '/shop/products/pet/small-pet/small-pet-toys/page-1.html',
            '/shop/products/pet/small-pet/grooming-and-shampoo/page-1.html',
            '/shop/products/pet/bird/food-and-treats-for-birds/budgie/page-1.html',
            '/shop/products/pet/bird/food-and-treats-for-birds/canary/page-1.html',
            '/shop/products/pet/bird/food-and-treats-for-birds/cockatiel-and-parrot/page-1.html',
            '/shop/products/pet/bird/food-and-treats-for-birds/finch/page-1.html',
            '/shop/products/pet/bird/food-and-treats-for-birds/other-food-and-treats/page-1.html',
            '/shop/products/pet/bird/nesting/page-1.html',
            '/shop/products/pet/bird/sand-sheets-and-grits/page-1.html',
            '/shop/products/pet/bird/feeders-and-drinkers/page-1.html',
            '/shop/products/pet/bird/bird-cages-and-stands/page-1.html',
            '/shop/products/pet/bird/perches/page-1.html',
            '/shop/products/pet/bird/toys-for-birds/page-1.html',
            '/shop/products/pet/bird/medication-and-health-for-birds/hygiene-for-birds/page-1.html',
            '/shop/products/pet/bird/medication-and-health-for-birds/supplements-for-birds/page-1.html',
            '/shop/products/pet/bird/medication-and-health-for-birds/hygiene-for-birds/page-1.html',
            '/shop/products/pet/bird/medication-and-health-for-birds/supplements-for-birds/page-1.html',
            '/shop/products/pet/pigeon/food-and-supplements/pigeon-food/page-1.html',
            '/shop/products/pet/pigeon/food-and-supplements/supplements-for-pigeons/page-1.html',
            '/shop/products/pet/reptile/feed-and-supplements/reptile-food/page-1.html',
            '/shop/products/pet/reptile/feed-and-supplements/live-reptile-food/page-1.html',
            '/shop/products/pet/reptile/feed-and-supplements/live-food-care/page-1.html',
            '/shop/products/pet/reptile/feed-and-supplements/frozen-reptile-food/page-1.html',
            '/shop/products/pet/reptile/feeders-and-hides/page-1.html',
            '/shop/products/pet/reptile/decoration/page-1.html',
            '/shop/products/pet/reptile/heating-and-controls/page-1.html',
            '/shop/products/pet/reptile/lighting/page-1.html',
            '/shop/products/pet/reptile/substrates/page-1.html',
            '/shop/products/pet/reptile/vivariums-and-terrariums/page-1.html',
            '/shop/products/pet/fish/aquatic/treatments/page-1.html',
            '/shop/products/pet/fish/aquatic/decorations/page-1.html',
            '/shop/products/pet/fish/aquatic/fish-food/coldwater/page-1.html',
            '/shop/products/pet/fish/aquatic/fish-food/tropical/page-1.html',
            '/shop/products/pet/fish/aquatic/fish-food/holiday/page-1.html',
            '/shop/products/pet/fish/aquatic/gravel/page-1.html',
            '/shop/products/pet/fish/pond/food/page-1.html',
            '/shop/products/pet/fish/pond/pond-treatments/page-1.html',
            '/shop/products/pet/fish/aquatic/airation/page-1.html',
            '/shop/products/pet/fish/aquatic/filtration/page-1.html',
            '/shop/products/pet/fish/aquatic/maintenance/page-1.html',
            '/shop/products/pet/fish/aquatic/heating/page-1.html',
            '/shop/products/equine/horse-feed/chaffs/page-1.html',
            '/shop/products/equine/horse-feed/stud-and-young-stock-feed/page-1.html',
            '/shop/products/equine/horse-feed/senior/page-1.html',
            '/shop/products/equine/horse-feed/fibre-beet/page-1.html',
            '/shop/products/equine/horse-feed/competition-feed/page-1.html',
            '/shop/products/equine/horse-feed/leisure/page-1.html',
            '/shop/products/equine/horse-feed/conditioning-and-performance-feed/page-1.html',
            '/shop/products/equine/horse-feed/haylage/page-1.html',
            '/shop/products/equine/horse-feed/straights/page-1.html',
            '/shop/products/equine/horse-feed/balancer-feed/page-1.html',
            '/shop/products/equine/tack/safety-and-training/page-1.html',
            '/shop/products/equine/tack/saddle-pads-and-numahs/page-1.html',
            '/shop/products/equine/tack/leather-care/page-1.html',
            '/shop/products/equine/tack/headcollars-and-lead-ropes/page-1.html',
            '/shop/products/equine/horse-rugs/summer-rugs/page-1.html',
            '/shop/products/equine/horse-rugs/weatherbeeta-winter-turnouts/page-1.html',
            '/shop/products/equine/horse-rugs/weatherbeeta-winter-combo/page-1.html',
            '/shop/products/equine/horse-rugs/fly-rugs/page-1.html',
            '/shop/products/equine/horse-rugs/stable-rugs-fleeces-and-coolers/page-1.html',
            '/shop/products/equine/health/page-1.html',
            '/shop/products/equine/horse-fencing/electric-fencing-for-horses/tape-and-wires/page-1.html',
            '/shop/products/equine/horse-fencing/electric-fencing-for-horses/batteries-and-energizers/page-1.html',
            '/shop/products/equine/horse-fencing/electric-fencing-for-horses/accessories-and-insulators/page-1.html',
            '/shop/products/equine/grooming/combs-and-brushes/page-1.html',
            '/shop/products/equine/grooming/hoof-care/page-1.html',
            '/shop/products/equine/grooming/plaiting/page-1.html',
            '/shop/products/equine/grooming/scissors-blades-and-clippers-for-horses/page-1.html',
            '/shop/products/equine/grooming/tack-and-grooming-boxes/page-1.html',
            '/shop/products/equine/stable-equipment/buckets/page-1.html',
            '/shop/products/equine/stable-equipment/forks-tidees-and-shovels/page-1.html',
            '/shop/products/equine/stable-equipment/haynets-and-hay-bags/page-1.html',
            '/shop/products/equine/stable-equipment/yard-equipment/stable-and-yard-accessories/page-1.html',
            '/shop/products/equine/stable-equipment/yard-equipment/stable-wheelbarrows/page-1.html',
            '/shop/products/equine/shampoos-and-conditioners/horse-care/page-1.html',
            '/shop/products/equine/shampoos-and-conditioners/conditioner/page-1.html',
            '/shop/products/equine/shampoos-and-conditioners/shampoo/page-1.html',
            '/shop/products/equine/riding-clothes-and-footwear/yard-boots/page-1.html',
            '/shop/products/equine/riding-clothes-and-footwear/wellies/page-1.html,'
            '/shop/products/equine/riding-clothes-and-footwear/riding-footwear/jodphur-boot-adult/page-1.html',
            '/shop/products/equine/riding-clothes-and-footwear/riding-footwear/childrens-riding-boots/page-1.html',
            '/shop/products/equine/horse-bedding/rubber-matting/page-1.html',
            '/shop/products/equine/horse-bedding/shavings/page-1.html',
            '/shop/products/equine/horse-bedding/straw/page-1.html',
            '/shop/products/equine/horse-bedding/wood-pulp-pellets/page-1.html',
            '/shop/products/equine/fly-products/fly-sprays-and-creams/page-1.html',
            '/shop/products/equine/fly-products/fly-masks-and-fringes/page-1.html',
            '/shop/products/equine/supplements-and-treats/horse-supplements/calming/page-1.html',
            '/shop/products/equine/supplements-and-treats/horse-supplements/competition-and-performance/page-1.html',
            '/shop/products/equine/supplements-and-treats/horse-supplements/conditioning-horse-feed/page-1.html',
            '/shop/products/equine/supplements-and-treats/horse-supplements/digestion/page-1.html',
            '/shop/products/equine/supplements-and-treats/horse-supplements/garlic/page-1.html',
            '/shop/products/equine/supplements-and-treats/horse-supplements/general-health/page-1.html',
            '/shop/products/equine/supplements-and-treats/horse-supplements/hoof-and-legs/page-1.html',
            '/shop/products/equine/supplements-and-treats/horse-supplements/joints-and-mobility/page-1.html',
            '/shop/products/equine/supplements-and-treats/horse-supplements/stud-and-young-stock-supplements/page-1.html',
            '/shop/products/equine/supplements-and-treats/treats-and-licks/page-1.html',
            '/shop/products/poultry/poultry-feed/poultry-feed-by-brand/dr-green-poultry-feed/page-1.html',
            '/shop/products/poultry/poultry-feed/poultry-feed-by-brand/fancy-feeds-poultry-feed/page-1.html',
            '/shop/products/poultry/poultry-feed/poultry-feed-by-brand/allen-and-page-poultry-feed/page-1.html',
            '/shop/products/poultry/poultry-feed/poultry-feed-by-brand/argo-poultry-feed/page-1.html',
            '/shop/products/poultry/poultry-feed/poultry-feed-by-brand/badminton-poultry-feed/page-1.html',
            '/shop/products/poultry/poultry-feed/poultry-feed-by-brand/verm-x-poultry-feed/page-1.html',
            '/shop/products/poultry/poultry-feed/goose-duck-and-quail-feed/page-1.html',
            '/shop/products/poultry/poultry-feed/chicken-feed/page-1.html',
            '/shop/products/poultry/poultry-feed/organic-chicken-feed/page-1.html',
            '/shop/products/poultry/poultry-bedding/hay-bedding/page-1.html',
            '/shop/products/poultry/poultry-bedding/straw-bedding/page-1.html',
            '/shop/products/poultry/poultry-bedding/wood-shavings/page-1.html',
            '/shop/products/poultry/chicken-housing/coops-runs-and-sheds/page-1.html',
            '/shop/products/poultry/feeders/page-1.html',
            '/shop/products/poultry/chicken-drinkers/page-1.html',
            '/shop/products/poultry/treats/page-1.html',
            '/shop/products/poultry/poultry-grit/page-1.html',
            '/shop/products/poultry/poultry-accessories/gaun/gaun-feeders/page-1.html',
            '/shop/products/poultry/poultry-accessories/gaun/gaun-drinkers/page-1.html',
            '/shop/products/poultry/poultry-accessories/gaun/gaun-accessories/page-1.html',
            '/shop/products/poultry/poultry-accessories/egg-boxes/page-1.html',
            '/shop/products/poultry/poultry-accessories/miscellaneous/page-1.html',
            '/shop/products/poultry/poultry-health-and-hygiene/page-1.html',
            '/shop/products/wild-bird/wild-bird-food-and-treats/wild-bird-seed/page-1.html',
            '/shop/products/wild-bird/wild-bird-food-and-treats/peanuts/page-1.html',
            '/shop/products/wild-bird/wild-bird-food-and-treats/fat-and-suet/suet-balls/page-1.html',
            '/shop/products/wild-bird/wild-bird-food-and-treats/fat-and-suet/suet-cakes-and-pellets/page-1.html',
            '/shop/products/wild-bird/wild-bird-food-and-treats/mealworm/page-1.html',
            '/shop/products/wild-bird/wild-bird-feeders/peanut-bird-feeders/page-1.html',
            '/shop/products/wild-bird/wild-bird-feeders/seed-feeders/page-1.html',
            '/shop/products/wild-bird/wild-bird-feeders/fat-feeders/page-1.html',
            '/shop/products/wild-bird/wild-bird-feeders/specialist-bird-feeders/page-1.html',
            '/shop/products/wild-bird/wild-bird-feeders/nyjer-seed-feeders/page-1.html',
            '/shop/products/wild-bird/wild-bird-feeders/squirrel-proof-feeders/page-1.html',
            '/shop/products/wild-bird/wild-bird-accessories/page-1.html'
            '/shop/products/wild-bird/bird-baths-tables-and-feeding-stations/page-1.html',
            '/shop/products/wild-bird/other-wildlife/hedgehogs/page-1.html',
            '/shop/products/wild-bird/other-wildlife/swans-and-ducks/page-1.html'
            '/shop/products/wild-bird/other-wildlife/squirrels/page-1.html'
        ]

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find(
                'h1', attrs={'itemprop': 'name'}).get_text()
            product_description = None

            if soup.find('div', class_="short-description"):
                product_description = soup.find(
                    'div', class_="short-description").get_text(strip=True)

            product_url = url.replace(self.BASE_URL, "")
            product_id = soup.find(
                'div', class_="ruk_rating_snippet").get('data-sku')

            rating_wrapper = requests.get(
                f"https://api.feefo.com/api/10/reviews/summary/product?since_period=ALL&parent_product_sku={product_id}&merchant_identifier=farm-pet-place&origin=www.farmandpetplace.co.uk")
            rating = float(rating_wrapper.json()['rating']['rating'])
            product_rating = f'{rating}/5'

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []

            if soup.find('select', id="attribute"):
                variants.append(soup.find('select', id="attribute").find_all(
                    'option')[0].get('value'))
                if soup.find('div', class_="price").find('span', class_="rrp"):
                    price = float(soup.find('div', class_="price").find(
                        'span', class_="rrp").find('strong').get_text().replace('£', ''))
                    discounted_price = float(soup.find('div', class_="price").find(
                        'span', class_="current").find('strong').get_text().replace('£', ''))
                    discount_percentage = "{:.2f}".format(
                        (price - discounted_price) / price)

                    prices.append(price)
                    discounted_prices.append(discounted_price)
                    discount_percentages.append(discount_percentage)

                else:
                    prices.append(float(soup.find('div', class_="price").find(
                        'span', class_="current").find('strong').get_text().replace('£', '')))
                    discounted_prices.append(None)
                    discount_percentages.append(None)

            else:
                variants.append(None)
                if soup.find('div', class_="price").find('span', class_="rrp"):
                    price = float(soup.find('div', class_="price").find(
                        'span', class_="rrp").find('strong').get_text().replace('£', ''))
                    discounted_price = float(soup.find('div', class_="price").find(
                        'span', class_="current").find('strong').get_text().replace('£', ''))
                    discount_percentage = "{:.2f}".format(
                        (price - discounted_price) / price)

                    prices.append(price)
                    discounted_prices.append(discounted_price)
                    discount_percentages.append(discount_percentage)

                else:
                    prices.append(float(soup.find('div', class_="price").find(
                        'span', class_="current").find('strong').get_text().replace('£', '')))
                    discounted_prices.append(None)
                    discount_percentages.append(None)

            df = pd.DataFrame({"variant": variants, "price": prices,
                               "discounted_price": discounted_prices, "discount_percentage": discount_percentages})
            df.insert(0, "url", product_url)
            df.insert(0, "description", product_description)
            df.insert(0, "rating", product_rating)
            df.insert(0, "name", product_name)
            df.insert(0, "shop", self.SHOP)

            return df
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")

    def get_links(self, category: str) -> pd.DataFrame:
        if category not in self.CATEGORIES:
            raise ValueError(
                f"Invalid category. Value must be in {self.CATEGORIES}")

        url = self.BASE_URL+category
        soup = self.extract_from_url("GET", url)
        n_product = [int(word) for word in soup.find(
            'p', class_="woocommerce-result-count").get_text().split() if word.isdigit()][0]
        pagination_length = math.ceil(n_product / 24)

        urls = []
        if pagination_length == 1:
            urls.extend([self.BASE_URL + product.find('a').get('href') for product in soup.find(
                'div', class_="shop-filters-area").find_all('div', class_="product")])
        else:
            for i in range(1, pagination_length + 1):
                base = url.split("page-")[0]
                new_url = f"{base}page-{i}.html"
                soup_pagination = self.extract_from_url("GET", new_url)

                urls.extend([self.BASE_URL + product.find('a').get('href') for product in soup_pagination.find(
                    'div', class_="shop-filters-area").find_all('div', class_="product")])

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df
