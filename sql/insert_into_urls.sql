INSERT INTO urls (
    inserted_date
    ,shop
    ,url
    ,scrape_status
    ,updated_date
)
SELECT DISTINCT
	inserted_date
    ,shop
    ,url
    ,scrape_status
    ,updated_date
FROM stg_urls
WHERE url NOT IN (
    SELECT DISTINCT url
    FROM urls
); 