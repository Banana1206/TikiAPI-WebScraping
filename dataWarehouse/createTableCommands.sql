-- Drop the fact_category table if it exists
DROP TABLE IF EXISTS fact_category;

-- Drop the dim_product table if it exists
DROP TABLE IF EXISTS dim_product;

-- Drop the dim_level1_category table if it exists
DROP TABLE IF EXISTS dim_level1_category;

-- Drop the dim_level2_category table if it exists
DROP TABLE IF EXISTS dim_level2_category;

-- Drop the dim_level3_category table if it exists
DROP TABLE IF EXISTS dim_level3_category;

-- Create the fact_category table
CREATE TABLE fact_category (
    productId BIGINT,
    categoryId BIGINT,
    PRIMARY KEY (productId, categoryId),
    FOREIGN KEY (productId) REFERENCES dim_product (productId),
    FOREIGN KEY (categoryId) REFERENCES dim_level3_category (categoryId)
);

-- Create the dim_product table
CREATE TABLE dim_product (
    productId BIGINT,
    sellerId BIGINT,
    name VARCHAR(500),
    brandName VARCHAR(255),
    originalPrice FLOAT,
    price FLOAT,
    discount FLOAT,
    discountRate FLOAT,
    quantitySold INT,
    ratingAverage FLOAT,
    PRIMARY KEY (productId)
);

-- Create the dim_level1_category table
CREATE TABLE dim_level1_category (
    categoryId BIGINT,
    categoryName VARCHAR(255),
    PRIMARY KEY (categoryId)
);

-- Create the dim_level2_category table
CREATE TABLE dim_level2_category (
    level2CategoryId BIGINT,
    level1CategoryId BIGINT,
    categoryName VARCHAR(255),
    PRIMARY KEY (level2CategoryId),
    FOREIGN KEY (level1CategoryId) REFERENCES dim_level1_category (categoryId)
);

-- Create the dim_level3_category table
CREATE TABLE dim_level3_category (
    level3CategoryId BIGINT,
    level2CategoryId BIGINT,
    level1CategoryId BIGINT,
    categoryName VARCHAR(255),
    PRIMARY KEY (level3CategoryId),
    FOREIGN KEY (level2CategoryId) REFERENCES dim_level2_category (level2CategoryId),
    FOREIGN KEY (level1CategoryId) REFERENCES dim_level1_category (categoryId)
);
