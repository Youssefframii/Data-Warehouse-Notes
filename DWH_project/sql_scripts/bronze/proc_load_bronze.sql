/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
*/
CREATE OR REPLACE PROCEDURE bronze.load_bronze() AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    load_duration_sec INT;
    total_duration_sec INT;
    error_msg text;
    error_state text;
BEGIN
    -- Start a transaction (implicit in a PROCEDURE call)
    batch_start_time := clock_timestamp();
    
    RAISE notice '================================================';
    RAISE notice 'Loading Bronze Layer';
    RAISE notice '================================================';

    --------------------------------------------------------------------------------------------------------------------
    -- Loading CRM Tables
    --------------------------------------------------------------------------------------------------------------------
    RAISE notice '------------------------------------------------';
    RAISE notice 'Loading CRM Tables';
    RAISE notice '------------------------------------------------';

    -- bronze.crm_cust_info
    start_time := clock_timestamp();
    RAISE notice '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    RAISE notice '>> Inserting Data Into: bronze.crm_cust_info (Using COPY)';
    
    -- The COPY command is PostgreSQL's highly efficient bulk loading tool.
    -- Using dollar-quoting ($$) for the inner COPY string to avoid quoting confusion.
    EXECUTE format(
        'COPY bronze.crm_cust_info FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','', ENCODING ''UTF8'')',
        'datasets/source_crm/cust_info.csv'
    );

    end_time := clock_timestamp();
    load_duration_sec := EXTRACT(epoch FROM (end_time - start_time));
    RAISE notice '>> Load Duration: % seconds', load_duration_sec;
    RAISE notice '>> -------------';

    -- bronze.crm_prd_info
    start_time := clock_timestamp();
    RAISE notice '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    RAISE notice '>> Inserting Data Into: bronze.crm_prd_info (Using COPY)';
    
    EXECUTE format(
        'COPY bronze.crm_prd_info FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','', ENCODING ''UTF8'')',
        'datasets/source_crm/prd_info.csv'
    );
    
    end_time := clock_timestamp();
    load_duration_sec := EXTRACT(epoch FROM (end_time - start_time));
    RAISE notice '>> Load Duration: % seconds', load_duration_sec;
    RAISE notice '>> -------------';

    -- bronze.crm_sales_details
    start_time := clock_timestamp();
    RAISE notice '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    RAISE notice '>> Inserting Data Into: bronze.crm_sales_details (Using COPY)';
    
    EXECUTE format(
        'COPY bronze.crm_sales_details FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','', ENCODING ''UTF8'')',
        'datasets/source_crm/sales_details.csv'
    );
    
    end_time := clock_timestamp();
    load_duration_sec := EXTRACT(epoch FROM (end_time - start_time));
    RAISE notice '>> Load Duration: % seconds', load_duration_sec;
    RAISE notice '>> -------------';

    --------------------------------------------------------------------------------------------------------------------
    -- Loading ERP Tables
    --------------------------------------------------------------------------------------------------------------------
    RAISE notice '------------------------------------------------';
    RAISE notice 'Loading ERP Tables';
    RAISE notice '------------------------------------------------';

    -- bronze.erp_loc_a101
    start_time := clock_timestamp();
    RAISE notice '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    RAISE notice '>> Inserting Data Into: bronze.erp_loc_a101 (Using COPY)';
    
    EXECUTE format(
        'COPY bronze.erp_loc_a101 FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','', ENCODING ''UTF8'')',
        'datasets/source_erp/loc_a101.csv'
    );
    
    end_time := clock_timestamp();
    load_duration_sec := EXTRACT(epoch FROM (end_time - start_time));
    RAISE notice '>> Load Duration: % seconds', load_duration_sec;
    RAISE notice '>> -------------';

    -- bronze.erp_cust_az12
    start_time := clock_timestamp();
    RAISE notice '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    RAISE notice '>> Inserting Data Into: bronze.erp_cust_az12 (Using COPY)';
    
    EXECUTE format(
        'COPY bronze.erp_cust_az12 FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','', ENCODING ''UTF8'')',
        'datasets/source_erp/cust_az12.csv'
    );
    
    end_time := clock_timestamp();
    load_duration_sec := EXTRACT(epoch FROM (end_time - start_time));
    RAISE notice '>> Load Duration: % seconds', load_duration_sec;
    RAISE notice '>> -------------';

    -- bronze.erp_px_cat_g1v2
    start_time := clock_timestamp();
    RAISE notice '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    RAISE notice '>> Inserting Data Into: bronze.erp_px_cat_g1v2 (Using COPY)';
    
    EXECUTE format(
        'COPY bronze.erp_px_cat_g1v2 FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','', ENCODING ''UTF8'')',
        'datasets/source_erp/px_cat_g1v2.csv'
    );
    
    end_time := clock_timestamp();
    load_duration_sec := EXTRACT(epoch FROM (end_time - start_time));
    RAISE notice '>> Load Duration: % seconds', load_duration_sec;
    RAISE notice '>> -------------';

    batch_end_time := clock_timestamp();
    total_duration_sec := EXTRACT(epoch FROM (batch_end_time - batch_start_time));

    RAISE notice '==========================================';
    RAISE notice 'Loading Bronze Layer is Completed';
    RAISE notice ' - Total Load Duration: % seconds', total_duration_sec;
    RAISE notice '==========================================';

EXCEPTION
    WHEN OTHERS THEN 
        GET STACKED DIAGNOSTICS 
            error_msg = MESSAGE_TEXT,
            error_state = PG_EXCEPTION_DETAIL; -- Sometimes PG_EXCEPTION_HINT is more useful
        
        RAISE notice '==========================================';
        RAISE notice 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        RAISE notice 'Error Message: %', error_msg;
        RAISE notice 'Error Details: %', error_state;
        RAISE notice '==========================================';
        -- The current transaction will be aborted by default if not caught.
        -- Uncomment the next line to explicitly re-raise:
        -- RAISE;
END;
$$ LANGUAGE plpgsql;

-- To execute the procedure:
-- CALL bronze.load_bronze();