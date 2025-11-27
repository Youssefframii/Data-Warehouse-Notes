/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze_simple();
===============================================================================
*/

/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
*/
CREATE OR REPLACE PROCEDURE bronze.load_bronze_simple()
LANGUAGE plpgsql AS $$
DECLARE
    start_time TIMESTAMP;
    end_time   TIMESTAMP;
BEGIN
    RAISE notice '================================================';
    RAISE notice 'Starting Bronze Layer Load';
    RAISE notice '================================================';

    ---------------------------------------------------------------------------
    -- bronze.crm_cust_info  (truncate + load)
    ---------------------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE notice '>> Truncating Table: bronze.crm_cust_info'; -- clear table
    TRUNCATE TABLE bronze.crm_cust_info;

    RAISE notice '>> Loading: bronze.crm_cust_info'; -- load CSV
    EXECUTE 'COPY bronze.crm_cust_info 
             FROM ''C:/dwh_csv_files/datasets/source_CRM/cust_info.csv''
             WITH (FORMAT csv, HEADER TRUE, DELIMITER '','')';

    end_time := clock_timestamp();
    RAISE notice '>> Load Duration: % seconds',
        EXTRACT(epoch FROM end_time - start_time); -- duration

    ---------------------------------------------------------------------------
    -- bronze.crm_prd_info (truncate + load)
    ---------------------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE notice '>> Truncating Table: bronze.crm_prd_info'; -- clear table
    TRUNCATE TABLE bronze.crm_prd_info;

    RAISE notice '>> Loading: bronze.crm_prd_info'; -- load CSV
    EXECUTE 'COPY bronze.crm_prd_info 
             FROM ''C:/dwh_csv_files/datasets/source_CRM/prd_info.csv''
             WITH (FORMAT csv, HEADER TRUE, DELIMITER '','')';

    end_time := clock_timestamp();
    RAISE notice '>> Load Duration: % seconds',
        EXTRACT(epoch FROM end_time - start_time);

    ---------------------------------------------------------------------------
    -- bronze.crm_sales_details (truncate + load)
    ---------------------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE notice '>> Truncating Table: bronze.crm_sales_details'; -- clear table
    TRUNCATE TABLE bronze.crm_sales_details;

    RAISE notice '>> Loading: bronze.crm_sales_details'; -- load CSV
    EXECUTE 'COPY bronze.crm_sales_details 
             FROM ''C:/dwh_csv_files/datasets/source_CRM/sales_details.csv''
             WITH (FORMAT csv, HEADER TRUE, DELIMITER '','')';

    end_time := clock_timestamp();
    RAISE notice '>> Load Duration: % seconds',
        EXTRACT(epoch FROM end_time - start_time);

    ---------------------------------------------------------------------------
    -- bronze.erp_loc_a101 (truncate + load)
    ---------------------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE notice '>> Truncating Table: bronze.erp_loc_a101'; -- clear table
    TRUNCATE TABLE bronze.erp_loc_a101;

    RAISE notice '>> Loading: bronze.erp_loc_a101'; -- load CSV
    EXECUTE 'COPY bronze.erp_loc_a101 
             FROM ''C:/dwh_csv_files/datasets/source_ERP/LOC_A101.csv''
             WITH (FORMAT csv, HEADER TRUE, DELIMITER '','')';

    end_time := clock_timestamp();
    RAISE notice '>> Load Duration: % seconds',
        EXTRACT(epoch FROM end_time - start_time);

    ---------------------------------------------------------------------------
    -- bronze.erp_cust_az12 (truncate + load)
    ---------------------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE notice '>> Truncating Table: bronze.erp_cust_az12'; -- clear table
    TRUNCATE TABLE bronze.erp_cust_az12;

    RAISE notice '>> Loading: bronze.erp_cust_az12'; -- load CSV
    EXECUTE 'COPY bronze.erp_cust_az12 
             FROM ''C:/dwh_csv_files/datasets/source_ERP/CUST_AZ12.csv''
             WITH (FORMAT csv, HEADER TRUE, DELIMITER '','')';

    end_time := clock_timestamp();
    RAISE notice '>> Load Duration: % seconds',
        EXTRACT(epoch FROM end_time - start_time);

    ---------------------------------------------------------------------------
    -- bronze.erp_px_cat_g1v2 (truncate + load)
    ---------------------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE notice '>> Truncating Table: bronze.erp_px_cat_g1v2'; -- clear table
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    RAISE notice '>> Loading: bronze.erp_px_cat_g1v2'; -- load CSV
    EXECUTE 'COPY bronze.erp_px_cat_g1v2 
             FROM ''C:/dwh_csv_files/datasets/source_ERP/PX_CAT_G1V2.csv''
             WITH (FORMAT csv, HEADER TRUE, DELIMITER '','')';

    end_time := clock_timestamp();
    RAISE notice '>> Load Duration: % seconds',
        EXTRACT(epoch FROM end_time - start_time);

    RAISE notice '================================================';
    RAISE notice 'Bronze Layer Load Completed';
    RAISE notice '================================================';
END; $$;

-- RUN the stored procedure
CALL bronze.load_bronze_simple();

