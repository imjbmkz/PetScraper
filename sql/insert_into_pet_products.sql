INSERT INTO pet_products (
    shop_id
    ,name
    ,rating
    ,description
    ,url
)
SELECT DISTINCT
	b.id
    ,a.name 
    ,a.rating
    ,a.description
    ,a.url
FROM stg_pet_products a 
LEFT JOIN shops b ON b.name=a.shop
WHERE a.url NOT IN (
    SELECT DISTINCT url
    FROM pet_products
); 