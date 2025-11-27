/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- 👤 Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results (customer_key is generated using ROW_NUMBER() and should be unique)
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;


---

-- 🛒 Checking 'gold.dim_products'
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results (product_key is generated using ROW_NUMBER() and should be unique)
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;


---

-- 🔗 Checking 'gold.fact_sales' Connectivity
-- Check the data model connectivity between fact and dimensions
-- Expectation: No results (indicates that all foreign keys in the fact table successfully map to the dimension tables)
SELECT 
    f.order_number,
    f.product_key AS fact_product_key,
    f.customer_key AS fact_customer_key,
    p.product_key AS dim_product_key,
    c.customer_key AS dim_customer_key
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
-- Records returned here represent data integrity failures (missing dimension records for a given fact key)
WHERE p.product_key IS NULL OR c.customer_key IS NULL;