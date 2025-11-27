/*
===============================================================================
Dimensions Exploration
===============================================================================
Purpose:
    - Explore the structure of dimension tables and understand 
      the categorical distributions inside the dataset.

SQL Functions Used:
    - DISTINCT   → For retrieving unique values
    - ORDER BY   → For sorting results alphabetically
===============================================================================
*/

-- Retrieve a list of unique countries from which customers originate
SELECT DISTINCT 
    country
FROM gold.dim_customers
ORDER BY country;

-- Retrieve a list of unique categories, subcategories, and products
SELECT DISTINCT 
    category,
    subcategory,
    product_name
FROM gold.dim_products
ORDER BY category, subcategory, product_name;
