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
LEFT JOIN pet_products b ON b.url=a.url
LEFT JOIN pet_product_variants c 
	ON c.url=a.url 
    	-- Replace nulls with empty string to avoid issues with concatenating
    	AND IFNULL(c.variant,'')=IFNULL(a.variant,'')
		AND c.shop_id=b.shop_id
WHERE c.id IS NULL 