/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL silver.load_silver();
===============================================================================
*/

/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
*/
CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql AS $$
DECLARE
    start_time TIMESTAMP;
    end_time   TIMESTAMP;
BEGIN
    RAISE notice '================================================';
    RAISE notice 'Starting Silver Layer Load';
    RAISE notice '================================================';

    ---------------------------------------------------------------------------
    -- silver.crm_cust_info  (clean + transform + dedupe)
    ---------------------------------------------------------------------------
    start_time := clock_timestamp();

    RAISE notice '>> Truncating Table: silver.crm_cust_info'; -- clear table
    TRUNCATE TABLE silver.crm_cust_info;

    RAISE notice '>> Loading: silver.crm_cust_info'; -- load + clean data
    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY cst_id ORDER BY cst_create_date DESC
               ) AS flag_last   -- pick latest record
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;

    end_time := clock_timestamp();
    RAISE notice '>> Load Duration: % seconds',
    EXTRACT(epoch FROM end_time - start_time); -- duration

    ---------------------------------------------------------------------------
    -- silver.crm_prd_info  (clean + derive keys + slowly changing dates)
    ---------------------------------------------------------------------------
    start_time := clock_timestamp();

    RAISE notice '>> Truncating Table: silver.crm_prd_info'; -- clear table
    TRUNCATE TABLE silver.crm_prd_info;

    RAISE notice '>> Loading: silver.crm_prd_info'; -- load + transform
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_') AS cat_id, -- derive category
        SUBSTRING(prd_key FROM 7),                                   -- extract key
        prd_nm,
        COALESCE(prd_cost, 0),
        CASE
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        prd_start_dt::DATE,
        LEAD(prd_start_dt) OVER (
            PARTITION BY prd_key ORDER BY prd_start_dt
        )::DATE - INTERVAL '1 day' AS prd_end_dt -- build validity range
    FROM bronze.crm_prd_info;

    end_time := clock_timestamp();
    RAISE notice '>> Load Duration: % seconds',
    EXTRACT(epoch FROM end_time - start_time);

    ---------------------------------------------------------------------------
    -- silver.crm_sales_details  (clean dates + fix sales/price integrity)
    ---------------------------------------------------------------------------
    start_time := clock_timestamp();

    RAISE notice '>> Truncating Table: silver.crm_sales_details'; -- clear table
    TRUNCATE TABLE silver.crm_sales_details;

    RAISE notice '>> Loading: silver.crm_sales_details'; -- load + validate metrics
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,

        -- convert YYYYMMDD integers to dates
        CASE WHEN sls_order_dt IS NULL OR sls_order_dt < 10000000
             THEN NULL ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD') END,
        CASE WHEN sls_ship_dt IS NULL OR sls_ship_dt < 10000000
             THEN NULL ELSE TO_DATE(sls_ship_dt::text, 'YYYYMMDD') END,
        CASE WHEN sls_due_dt IS NULL OR sls_due_dt < 10000000
             THEN NULL ELSE TO_DATE(sls_due_dt::text, 'YYYYMMDD') END,

        -- verify sales = quantity * price
        CASE
            WHEN sls_sales IS NULL
             OR sls_sales <= 0
             OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END,

        sls_quantity,

        -- fix missing/invalid price
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END
    FROM bronze.crm_sales_details;

    end_time := clock_timestamp();
    RAISE notice '>> Load Duration: % seconds',
    EXTRACT(epoch FROM end_time - start_time);

    ---------------------------------------------------------------------------
    -- silver.erp_cust_az12 (clean IDs + validate birthdate + normalize gender)
    ---------------------------------------------------------------------------
    start_time := clock_timestamp();

    RAISE notice '>> Truncating Table: silver.erp_cust_az12'; -- clear table
    TRUNCATE TABLE silver.erp_cust_az12;

    RAISE notice '>> Loading: silver.erp_cust_az12'; -- load + normalize
    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4) -- strip NAS prefix
            ELSE cid
        END,
        CASE WHEN bdate > CURRENT_DATE THEN NULL ELSE bdate END, -- avoid future dates
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;

    end_time := clock_timestamp();
    RAISE notice '>> Load Duration: % seconds',
    EXTRACT(epoch FROM end_time - start_time);

    ---------------------------------------------------------------------------
    -- silver.erp_loc_a101 (clean IDs + standardize countries)
    ---------------------------------------------------------------------------
    start_time := clock_timestamp();

    RAISE notice '>> Truncating Table: silver.erp_loc_a101'; -- clear table
    TRUNCATE TABLE silver.erp_loc_a101;

    RAISE notice '>> Loading: silver.erp_loc_a101'; -- load + standardize
    INSERT INTO silver.erp_loc_a101 (cid, cntry)
    SELECT
        REPLACE(cid, '-', '') AS cid, -- strip hyphens
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;

    end_time := clock_timestamp();
    RAISE notice '>> Load Duration: % seconds',
    EXTRACT(epoch FROM end_time - start_time);

    ---------------------------------------------------------------------------
    -- silver.erp_px_cat_g1v2 (simple passthrough)
    ---------------------------------------------------------------------------
    start_time := clock_timestamp();

    RAISE notice '>> Truncating Table: silver.erp_px_cat_g1v2'; -- clear table
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    RAISE notice '>> Loading: silver.erp_px_cat_g1v2'; -- load directly
    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT id, cat, subcat, maintenance
    FROM bronze.erp_px_cat_g1v2;

    end_time := clock_timestamp();
    RAISE notice '>> Load Duration: % seconds',
    EXTRACT(epoch FROM end_time - start_time);

    RAISE notice '================================================';
    RAISE notice 'Silver Layer Load Completed';
    RAISE notice '================================================';

END; $$;

CALL silver.load_silver();

