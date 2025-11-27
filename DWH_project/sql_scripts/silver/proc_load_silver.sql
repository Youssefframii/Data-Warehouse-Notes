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
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE
OR REPLACE PROCEDURE silver.load_silver() AS $$
DECLARE
    start_time TIMESTAMP;
end_time TIMESTAMP;
batch_start_time TIMESTAMP;
batch_end_time TIMESTAMP;
load_duration_sec INT;
total_duration_sec INT;
error_msg text;
error_detail text;
BEGIN
    batch_start_time:= clock_timestamp();
RAISE notice '================================================';
RAISE notice 'Loading Silver Layer';
RAISE notice '================================================';
RAISE notice '------------------------------------------------';
RAISE notice 'Loading CRM Tables';
RAISE notice '------------------------------------------------';
-- Loading silver.crm_cust_info
    start_time:= clock_timestamp();
RAISE notice '>> Truncating Table: silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info;
RAISE notice '>> Inserting Data Into: silver.crm_cust_info';
INSERT INTO
    silver.crm_cust_info (
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
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'END AS cst_marital_status,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'END AS cst_gndr,
            cst_create_date
            FROM
                (
                    SELECT
                        *,
                        -- PostgreSQL uses "window_name" as the alias after OVER()
                        ROW_NUMBER() over (
                            PARTITION BY cst_id
                            ORDER BY
                                cst_create_date DESC
                        ) AS flag_last
                    FROM
                        bronze.crm_cust_info
                    WHERE
                        cst_id IS NOT NULL
                ) t
            WHERE
                flag_last = 1;
end_time:= clock_timestamp();
load_duration_sec:= EXTRACT(
                    epoch
                    FROM
                        (
                            end_time - start_time
                        )
                );
RAISE notice '>> Load Duration: % seconds',
                load_duration_sec;
RAISE notice '>> -------------';
-- Loading silver.crm_prd_info (Handling Slowly Changing Dimension type 2 logic)
                start_time:= clock_timestamp();
RAISE notice '>> Truncating Table: silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info;
RAISE notice '>> Inserting Data Into: silver.crm_prd_info';
            INSERT INTO
                silver.crm_prd_info (
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
                REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
                SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
                -- LEN() changed to LENGTH()
                prd_nm,
                COALESCE(
                    prd_cost,
                    0
                ) AS prd_cost,
                -- ISNULL() changed to COALESCE()
                CASE
                    WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                    WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                    WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                    WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                    ELSE 'n/a'END AS prd_line,
                    prd_start_dt :: DATE AS prd_start_dt,
                    -- CAST(col AS DATE) changed to col::DATE
                    (LEAD(prd_start_dt) over (PARTITION BY prd_key
                    ORDER BY
                        prd_start_dt) - INTERVAL '1 day') :: DATE AS prd_end_dt -- DATETIME math (col - 1) changed to (col - INTERVAL '1 day')
                    FROM
                        bronze.crm_prd_info;
end_time:= clock_timestamp();
load_duration_sec:= EXTRACT(
                            epoch
                            FROM
                                (
                                    end_time - start_time
                                )
                        );
RAISE notice '>> Load Duration: % seconds',
                        load_duration_sec;
RAISE notice '>> -------------';
-- Loading silver.crm_sales_details (Data Type Conversion and Cleansing)
                        start_time:= clock_timestamp();
RAISE notice '>> Truncating Table: silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;
RAISE notice '>> Inserting Data Into: silver.crm_sales_details';
                    INSERT INTO
                        silver.crm_sales_details (
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
                        CASE
                            WHEN sls_order_dt = 0
                            OR LENGTH(
                                sls_order_dt :: VARCHAR
                            ) != 8 THEN NULL
                            ELSE TO_DATE(
                                sls_order_dt :: VARCHAR,
                                'YYYYMMDD'
                            ) -- CAST(CAST(INT AS VARCHAR) AS DATE) changed to TO_DATE(INT::VARCHAR, 'YYYYMMDD')
                        END AS sls_order_dt,
                        CASE
                            WHEN sls_ship_dt = 0
                            OR LENGTH(
                                sls_ship_dt :: VARCHAR
                            ) != 8 THEN NULL
                            ELSE TO_DATE(
                                sls_ship_dt :: VARCHAR,
                                'YYYYMMDD'
                            )
                        END AS sls_ship_dt,
                        CASE
                            WHEN sls_due_dt = 0
                            OR LENGTH(
                                sls_due_dt :: VARCHAR
                            ) != 8 THEN NULL
                            ELSE TO_DATE(
                                sls_due_dt :: VARCHAR,
                                'YYYYMMDD'
                            )
                        END AS sls_due_dt,
                        CASE
                            -- Reversing the calculation of sales (Sales = Quantity * Price)
                            WHEN sls_sales IS NULL
                            OR sls_sales <= 0
                            OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
                            ELSE sls_sales
                        END AS sls_sales,
                        sls_quantity,
                        CASE
                            -- Derive price if original value is invalid
                            WHEN sls_price IS NULL
                            OR sls_price <= 0 THEN sls_sales :: numeric / NULLIF(
                                sls_quantity,
                                0
                            ) -- Added ::NUMERIC to prevent integer division
                            ELSE sls_price
                        END AS sls_price
                    FROM
                        bronze.crm_sales_details;
end_time:= clock_timestamp();
load_duration_sec:= EXTRACT(
                            epoch
                            FROM
                                (
                                    end_time - start_time
                                )
                        );
RAISE notice '>> Load Duration: % seconds',
                        load_duration_sec;
RAISE notice '>> -------------';
--------------------------------------------------------------------------------------------------------------------
                        -- Loading ERP Tables
                        --------------------------------------------------------------------------------------------------------------------
                        RAISE notice '------------------------------------------------';
RAISE notice 'Loading ERP Tables';
RAISE notice '------------------------------------------------';
-- Loading silver.erp_cust_az12 (Data Cleansing and Normalization)
                        start_time:= clock_timestamp();
RAISE notice '>> Truncating Table: silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;
RAISE notice '>> Inserting Data Into: silver.erp_cust_az12';
                    INSERT INTO
                        silver.erp_cust_az12 (
                            cid,
                            bdate,
                            gen
                        )
                    SELECT
                        CASE
                            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
                            ELSE cidEND AS cid,
                            CASE
                                WHEN bdate > CURRENT_DATE THEN NULL -- GETDATE() changed to CURRENT_DATE
                                ELSE bdate
                            END AS bdate,
                            CASE
                                WHEN UPPER(TRIM(gen)) IN (
                                    'F',
                                    'FEMALE'
                                ) THEN 'Female'
                                WHEN UPPER(TRIM(gen)) IN (
                                    'M',
                                    'MALE'
                                ) THEN 'Male'
                                ELSE 'n/a'
                            END AS gen
                            FROM
                                bronze.erp_cust_az12;
end_time:= clock_timestamp();
load_duration_sec:= EXTRACT(
                                    epoch
                                    FROM
                                        (
                                            end_time - start_time
                                        )
                                );
RAISE notice '>> Load Duration: % seconds',
                                load_duration_sec;
RAISE notice '>> -------------';
-- Loading silver.erp_loc_a101 (Data Cleansing and Normalization)
                                start_time:= clock_timestamp();
RAISE notice '>> Truncating Table: silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101;
RAISE notice '>> Inserting Data Into: silver.erp_loc_a101';
                            INSERT INTO
                                silver.erp_loc_a101 (
                                    cid,
                                    cntry
                                )
                            SELECT
                                REPLACE(
                                    cid,
                                    '-',
                                    ''
                                ) AS cid,
                                CASE
                                    WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                                    WHEN TRIM(cntry) IN (
                                        'US',
                                        'USA'
                                    ) THEN 'United States'
                                    WHEN TRIM(cntry) = ''
                                    OR cntry IS NULL THEN 'n/a'
                                    ELSE TRIM(cntry)
                                END AS cntry
                            FROM
                                bronze.erp_loc_a101;
end_time:= clock_timestamp();
load_duration_sec:= EXTRACT(
                                    epoch
                                    FROM
                                        (
                                            end_time - start_time
                                        )
                                );
RAISE notice '>> Load Duration: % seconds',
                                load_duration_sec;
RAISE notice '>> -------------';
-- Loading silver.erp_px_cat_g1v2 (Direct Load)
                                start_time:= clock_timestamp();
RAISE notice '>> Truncating Table: silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2;
RAISE notice '>> Inserting Data
