INSERT INTO pet_products (
    inserted_date
    ,shop
    ,name
    ,rating
    ,description
    ,url
)
SELECT DISTINCT
	inserted_date
    ,shop
    ,name 
    ,rating
    ,description
    ,url
FROM stg_pet_products
WHERE url NOT IN (
    SELECT DISTINCT url
    FROM pet_products
); 