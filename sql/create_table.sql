DROP TABLE IF EXISTS shops;
CREATE TABLE shops (
    id int NOT NULL AUTO_INCREMENT PRIMARY KEY
    ,inserted_date datetime DEFAULT now()
    ,name varchar(50) CHARACTER SET utf8mb4
    ,base_url varchar(100) CHARACTER SET utf8mb4
);

INSERT INTO shops (name, base_url)
VALUES 
    ('Zooplus','https://www.zooplus.co.uk/')
    ,('Pets at Home','https://www.petsathome.com/')
    ,('Petplanet','https://www.petplanet.co.uk/')
    ,('Pet Supermarket','https://www.pet-supermarket.co.uk/')
    ,('Pet Drugs Online','https://www.petdrugsonline.co.uk/')
    ,('Jollyes','https://www.jollyes.co.uk/')
    ,('Lily''s Kitchen','https://www.lilyskitchen.co.uk/')
    ,('Viovet','https://www.viovet.co.uk/')
    ,('Bitiba','https://www.bitiba.co.uk/')
    ,('Direct Vet','https://www.direct-vet.co.uk/')
    ,('Fishkeeper','https://www.fishkeeper.co.uk/')
    ,('Purina','https://www.purina.co.uk/');

DROP TABLE IF EXISTS stg_urls;
CREATE TABLE stg_urls (
    shop varchar(50) CHARACTER SET utf8mb4
    ,url varchar(255) CHARACTER SET utf8mb4
    ,updated_date datetime
);

DROP TABLE IF EXISTS urls;
CREATE TABLE urls (
    id int NOT NULL AUTO_INCREMENT PRIMARY KEY
    ,inserted_date datetime DEFAULT now()
    ,shop varchar(50) CHARACTER SET utf8mb4
    ,url varchar(255) CHARACTER SET utf8mb4
    ,scrape_status varchar(25) CHARACTER SET utf8mb4 DEFAULT 'NOT STARTED'
    ,updated_date datetime
);

DROP TABLE IF EXISTS stg_pet_products;
CREATE TABLE stg_pet_products (
    shop varchar(50) CHARACTER SET utf8mb4
    ,name varchar(255) CHARACTER SET utf8mb4
    ,rating varchar(50) CHARACTER SET utf8mb4
    ,description varchar(1000) CHARACTER SET utf8mb4
    ,url varchar(255) CHARACTER SET utf8mb4
    ,variant varchar(255) CHARACTER SET utf8mb4
    ,price decimal(10, 4)
    ,discounted_price decimal(10, 4)
    ,discount_percentage decimal(10, 4)
);

DROP TABLE IF EXISTS pet_product_variants;
CREATE TABLE pet_product_variants (
    id int NOT NULL AUTO_INCREMENT PRIMARY KEY
    ,product_id int NOT NULL
    ,shop_id int NOT NULL
    ,inserted_date datetime DEFAULT now()
    ,url varchar(255) CHARACTER SET utf8mb4
    ,variant varchar(255) CHARACTER SET utf8mb4
);

DROP TABLE IF EXISTS pet_product_variant_prices;
CREATE TABLE pet_product_variant_prices (
    id int NOT NULL AUTO_INCREMENT PRIMARY KEY
    ,product_variant_id int NOT NULL
    ,shop_id int NOT NULL
    ,inserted_date datetime DEFAULT now()
    ,price decimal(10, 4)
    ,discounted_price decimal(10, 4)
    ,discount_percentage decimal(10, 4)
);

DROP TABLE IF EXISTS pet_products;
CREATE TABLE pet_products (
    id int NOT NULL AUTO_INCREMENT PRIMARY KEY
    ,inserted_date datetime DEFAULT now()
    ,shop_id int NOT NULL
    ,name varchar(255) CHARACTER SET utf8mb4
    ,rating varchar(50) CHARACTER SET utf8mb4
    ,description varchar(1000) CHARACTER SET utf8mb4
    ,url varchar(255) CHARACTER SET utf8mb4
);

ALTER TABLE pet_product_variants 
ADD FOREIGN KEY (product_id) REFERENCES pet_products(id);