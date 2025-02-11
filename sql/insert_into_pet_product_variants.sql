INSERT INTO pet_product_variants (
    product_id
    ,shop_id
    ,url
    ,variant
)
SELECT DISTINCT 
    b.id
    ,b.shop_id
    ,a.url
    ,a.variant
FROM stg_pet_products a 
LEFT JOIN pet_products b
    ON b.url = a.url

-- Replace nulls with empty string to avoid issues with concatenating
WHERE CONCAT(a.url,IFNULL(a.variant,'')) NOT IN (
    SELECT CONCAT(url,IFNULL(variant,''))
    FROM pet_product_variants
)