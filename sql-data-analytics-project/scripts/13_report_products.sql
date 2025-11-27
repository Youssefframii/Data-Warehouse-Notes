/*
===============================================================================
Product Report
===============================================================================
Purpose:
    - Consolidates key product metrics and behaviors.
    - Segments products by revenue into High-Performers, Mid-Range, or Low-Performers.
    - Aggregates product-level KPIs: total orders, sales, quantity, customers, and lifespan.
    - Computes recency, average order revenue (AOR), and average monthly revenue.

SQL Functions Used:
    - CASE: For product segmentation.
    - Aggregate Functions: COUNT(), SUM(), AVG(), MAX(), MIN().
    - Window Functions: Optional for cumulative calculations.
===============================================================================
*/

-- Drop view if exists
DROP VIEW IF EXISTS gold.report_products;

-- Create Product Report View
CREATE VIEW gold.report_products AS

WITH base_query AS (
    -- Retrieve core product and sales data
    SELECT
        f.order_number,
        f.order_date,
        f.customer_key,
        f.sales_amount,
        f.quantity,
        p.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.cost
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
),

product_aggregations AS (
    -- Aggregate metrics at the product level
    SELECT
        product_key,
        product_name,
        category,
        subcategory,
        cost,
        EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan,
        MAX(order_date) AS last_sale_date,
        COUNT(DISTINCT order_number) AS total_orders,
        COUNT(DISTINCT customer_key) AS total_customers,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        ROUND(AVG(CASE WHEN quantity = 0 THEN 0 ELSE sales_amount::FLOAT / quantity END), 1) AS avg_selling_price
    FROM base_query
    GROUP BY product_key, product_name, category, subcategory, cost
)

SELECT 
    product_key,
    product_name,
    category,
    subcategory,
    cost,
    last_sale_date,
    EXTRACT(MONTH FROM AGE(CURRENT_DATE, last_sale_date)) AS recency_in_months,
    CASE
        WHEN total_sales > 50000 THEN 'High-Performer'
        WHEN total_sales >= 10000 THEN 'Mid-Range'
        ELSE 'Low-Performer'
    END AS product_segment,
    lifespan,
    total_orders,
    total_sales,
    total_quantity,
    total_customers,
    avg_selling_price,
    -- Average Order Revenue (AOR)
    CASE 
        WHEN total_orders = 0 THEN 0
        ELSE total_sales / total_orders
    END AS avg_order_revenue,
    -- Average Monthly Revenue
    CASE
        WHEN lifespan = 0 THEN total_sales
        ELSE total_sales / lifespan
    END AS avg_monthly_revenue
FROM product_aggregations;
