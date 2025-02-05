CREATE TABLE stg_urls (
    inserted_date TIMESTAMP DEFAULT now(),
    shop VARCHAR(50),
    url VARCHAR(255),
    -- scrape_status VARCHAR(25) DEFAULT 'NOT STARTED',
    updated_date TIMESTAMP
);

CREATE TABLE urls (
    id SERIAL PRIMARY KEY,
    inserted_date TIMESTAMP DEFAULT now(),
    shop VARCHAR(50),
    url VARCHAR(255),
    scrape_status VARCHAR(25) DEFAULT 'NOT STARTED',
    updated_date TIMESTAMP
);

CREATE TABLE stg_pet_products (
    inserted_date TIMESTAMP DEFAULT now(),
    shop VARCHAR(50),
    name VARCHAR(255),
    rating VARCHAR(50),
    description VARCHAR(1000),
    url VARCHAR(255),
    variant VARCHAR(255),
    price DECIMAL(10, 4),
    discounted_price DECIMAL(10, 4),
    discount_percentage DECIMAL(10, 4)
);

CREATE TABLE pet_product_variants (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    inserted_date TIMESTAMP DEFAULT now(),
    url VARCHAR(255),
    variant VARCHAR(255),
    price DECIMAL(10, 4),
    discounted_price DECIMAL(10, 4),
    discount_percentage DECIMAL(10, 4)
);

CREATE TABLE pet_products (
    id SERIAL PRIMARY KEY,
    inserted_date TIMESTAMP DEFAULT now(),
    shop VARCHAR(50),
    name VARCHAR(255),
    rating VARCHAR(50),
    description VARCHAR(1000),
    url VARCHAR(255)
);

ALTER TABLE pet_product_variants 
ADD CONSTRAINT fk_pet_product_variants_product_id 
FOREIGN KEY (product_id) REFERENCES pet_products(id) ON DELETE CASCADE;
