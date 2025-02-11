INSERT INTO pet_product_variant_prices (
    product_variant_id
    ,shop_id
    ,price
    ,discounted_price
    ,discount_percentage
)
SELECT DISTINCT 
    b.id
    ,b.shop_id
    ,a.price
    ,a.discounted_price
    ,a.discount_percentage
FROM stg_pet_products a 
LEFT JOIN pet_product_variants b
    ON CONCAT(b.url,IFNULL(b.variant,'')) = CONCAT(a.url,IFNULL(a.variant,''))

-- Insert only product variants that are not yet available in the table
WHERE b.id NOT IN (
    SELECT product_variant_id
    FROM pet_product_variant_prices
)