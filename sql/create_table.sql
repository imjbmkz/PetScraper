DROP TABLE IF EXISTS stg_pet_products;
CREATE TABLE stg_pet_products (
    inserted_date datetime
    ,shop varchar(50) CHARACTER SET utf8mb4
    ,name varchar(255) CHARACTER SET utf8mb4
    ,rating varchar(50) CHARACTER SET utf8mb4
    ,description varchar(1000) CHARACTER SET utf8mb4
    ,url varchar(255) CHARACTER SET utf8mb4
    ,variant varchar(255) CHARACTER SET utf8mb4
    ,price decimal(10, 4)
    ,discounted_price decimal(10, 4)
    ,discount_percentage decimal(10, 4)
);

DROP TABLE IF EXISTS pet_products;
CREATE TABLE pet_products (
    id int NOT NULL AUTO_INCREMENT PRIMARY KEY
    ,inserted_date datetime
    ,shop varchar(50) CHARACTER SET utf8mb4
    ,name varchar(255) CHARACTER SET utf8mb4
    ,rating varchar(50) CHARACTER SET utf8mb4
    ,description varchar(1000) CHARACTER SET utf8mb4
    ,url varchar(255) CHARACTER SET utf8mb4
);

DROP TABLE IF EXISTS pet_product_variants;
CREATE TABLE pet_product_variants (
    id int NOT NULL AUTO_INCREMENT PRIMARY KEY
    ,product_id int NOT NULL
    ,inserted_date datetime
    ,url varchar(255) CHARACTER SET utf8mb4
    ,variant varchar(255) CHARACTER SET utf8mb4
    ,price decimal(10, 4)
    ,discounted_price decimal(10, 4)
    ,discount_percentage decimal(10, 4)
    ,CONSTRAINT FK_Id FOREIGN KEY (product_id) 
    REFERENCES pet_products(id)
);