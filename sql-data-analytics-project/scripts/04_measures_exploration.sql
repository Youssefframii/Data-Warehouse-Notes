/*
===============================================================================
Measures Exploration (Key Metrics)
===============================================================================
Purpose:
    - Calculate aggregated business metrics (totals, averages, counts).
    - Identify trends, performance indicators, and potential anomalies.

SQL Functions Used:
    - COUNT()
    - SUM()
    - AVG()
===============================================================================
*/

-- Find the Total Sales
SELECT SUM(sales_amount) AS total_sales 
FROM gold.fact_sales;

-- Find how many items are sold (total quantity)
SELECT SUM(quantity) AS total_quantity 
FROM gold.fact_sales;

-- Find the average selling price
SELECT AVG(price) AS avg_price 
FROM gold.fact_sales;

-- Find the total number of orders (raw count)
SELECT COUNT(order_number) AS total_orders 
FROM gold.fact_sales;

-- Find the total number of unique orders
SELECT COUNT(DISTINCT order_number) AS total_unique_orders 
FROM gold.fact_sales;

-- Find the total number of products
SELECT COUNT(product_name) AS total_products 
FROM gold.dim_products;

-- Find the total number of customers
SELECT COUNT(customer_key) AS total_customers 
FROM gold.dim_customers;

-- Find the total number of customers who have placed an order
SELECT COUNT(DISTINCT customer_key) AS active_customers 
FROM gold.fact_sales;

-- Generate a unified KPI (Key Metrics) Report
SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value 
FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) 
FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', AVG(price) 
FROM gold.fact_sales
UNION ALL
SELECT 'Total Unique Orders', COUNT(DISTINCT order_number) 
FROM gold.fact_sales
UNION ALL
SELECT 'Total Products', COUNT(DISTINCT product_name) 
FROM gold.dim_products
UNION ALL
SELECT 'Total Customers', COUNT(customer_key) 
FROM gold.dim_customers;
