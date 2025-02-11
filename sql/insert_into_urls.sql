INSERT INTO urls (
    shop
    ,url
    ,updated_date
)
SELECT DISTINCT
	shop
    ,url
    ,updated_date
FROM stg_urls a 
WHERE url NOT IN (
    SELECT DISTINCT url
    FROM urls
); 