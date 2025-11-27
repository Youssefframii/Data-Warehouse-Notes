/*
===============================================================================
Change Over Time Analysis
===============================================================================
Purpose:
    - Track trends, growth, and changes in key metrics over time.
    - Perform time-series analysis and identify seasonality.
    - Measure growth or decline over specific periods.

SQL Functions Used:
    - Date Functions: EXTRACT(), DATE_TRUNC(), TO_CHAR()
    - Aggregate Functions: SUM(), COUNT()
===============================================================================
*/

-- Analyse sales performance over time (Year & Month)
SELECT
    EXTRACT(YEAR FROM order_date) AS order_year,
    EXTRACT(MONTH FROM order_date) AS order_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date)
ORDER BY order_year, order_month;

-- Analyse sales performance by month using DATE_TRUNC
SELECT
    DATE_TRUNC('month', order_date) AS order_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY order_month;

-- Analyse sales with formatted month labels (YYYY-MMM)
SELECT
    TO_CHAR(order_date, 'YYYY-Mon') AS order_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY TO_CHAR(order_date, 'YYYY-Mon')
ORDER BY TO_CHAR(order_date, 'YYYY-Mon');
