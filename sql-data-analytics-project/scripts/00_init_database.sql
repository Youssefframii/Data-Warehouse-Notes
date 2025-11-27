/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'datawarehouseanalytics'.
    If the database already exists, it is dropped and recreated.

WARNING:
    Running this script will drop the entire 'datawarehouseanalytics'
    database if it exists. All data will be permanently deleted.
    Ensure proper backups before executing.
*/

-- Remove database if present
DROP DATABASE IF EXISTS datawarehouseanalytics;

-- Create new database
CREATE DATABASE datawarehouseanalytics;

-- IMPORTANT:
-- PostgreSQL does not support USE <db>. 
-- Connect manually via psql:     \c datawarehouseanalytics
-- Or configure your client to run the remaining commands inside this DB.


/*
=============================================================
Create Schema
=============================================================
*/

CREATE SCHEMA IF NOT EXISTS gold;


/*
=============================================================
Create Tables
=============================================================
*/

-- Customer Dimension
CREATE TABLE gold.dim_customers (
    customer_key INTEGER,
    customer_id INTEGER,
    customer_number VARCHAR(50),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    country VARCHAR(50),
    marital_status VARCHAR(50),
    gender VARCHAR(50),
    birthdate DATE,
    create_date DATE
);

-- Product Dimension
CREATE TABLE gold.dim_products (
    product_key INTEGER,
    product_id INTEGER,
    product_number VARCHAR(50),
    product_name VARCHAR(50),
    category_id VARCHAR(50),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    maintenance VARCHAR(50),
    cost INTEGER,
    product_line VARCHAR(50),
    start_date DATE
);

-- Sales Fact
CREATE TABLE gold.fact_sales (
    order_number VARCHAR(50),
    product_key INTEGER,
    customer_key INTEGER,
    order_date DATE,
    shipping_date DATE,
    due_date DATE,
    sales_amount INTEGER,
    quantity SMALLINT,
    price INTEGER
);


/*
=============================================================
Truncate Tables (Data Reset)
=============================================================
*/

TRUNCATE TABLE gold.dim_customers;
TRUNCATE TABLE gold.dim_products;
TRUNCATE TABLE gold.fact_sales;


/*
=============================================================
Data Import from CSV
=============================================================
PostgreSQL COPY requires:
- The file must be accessible by the PostgreSQL server
OR
- Use \copy to import from the local machine (recommended)

Your dataset directory:
C:\EDA_csv_files\datasets
=============================================================
*/

-- Import customers
COPY gold.dim_customers
FROM 'C:/EDA_csv_files/datasets/gold.dim_customers.csv'
WITH (FORMAT csv, HEADER true);

-- Import products
COPY gold.dim_products
FROM 'C:/EDA_csv_files/datasets/gold.dim_products.csv'
WITH (FORMAT csv, HEADER true);

-- Import sales
COPY gold.fact_sales
FROM 'C:/EDA_csv_files/datasets/gold.fact_sales.csv'
WITH (FORMAT csv, HEADER true);


/*
Alternative for psql (client-side, recommended):
\copy gold.dim_customers FROM 'C:/EDA_csv_files/datasets/gold.dim_customers.csv' CSV HEADER;
\copy gold.dim_products FROM 'C:/EDA_csv_files/datasets/gold.dim_products.csv' CSV HEADER;
\copy gold.fact_sales FROM 'C:/EDA_csv_files/datasets/gold.fact_sales.csv' CSV HEADER;
*/
