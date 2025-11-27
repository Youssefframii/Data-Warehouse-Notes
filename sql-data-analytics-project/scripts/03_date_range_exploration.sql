/*
===============================================================================
Date Range Exploration 
===============================================================================
Purpose:
    - Determine the temporal boundaries of key data points.
    - Understand the historical span covered by the dataset.

SQL Functions Used:
    - MIN(), MAX()
    - AGE()          → PostgreSQL function for date differences
    - EXTRACT()      → Used to compute differences in months or years
===============================================================================
*/

-- Determine the first and last order date and the total duration in months
SELECT 
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,
    
    /* 
       In PostgreSQL, DATEDIFF does not exist.
       We calculate month difference using EXTRACT from AGE().
    */
    EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12 
      + EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) 
        AS order_range_months

FROM gold.fact_sales;

-- Find the youngest and oldest customer based on birthdate
SELECT
    MIN(birthdate) AS oldest_birthdate,
    EXTRACT(YEAR FROM AGE(NOW(), MIN(birthdate))) AS oldest_age,

    MAX(birthdate) AS youngest_birthdate,
    EXTRACT(YEAR FROM AGE(NOW(), MAX(birthdate))) AS youngest_age

FROM gold.dim_customers;
