INSERT INTO pet_product_variants (
    product_id
    ,inserted_date
    ,url
    ,variant
    ,price
    ,discounted_price
    ,discount_percentage
)
SELECT DISTINCT 
    b.id
    ,a.inserted_date
    ,a.url
    ,a.variant
    ,a.price
    ,a.discounted_price
    ,a.discount_percentage
FROM stg_pet_products a 
LEFT JOIN pet_products b
    ON b.url = a.url
WHERE CONCAT(a.url,a.variant) NOT IN (
    SELECT CONCAT(url,variant)
    FROM pet_product_variants
)