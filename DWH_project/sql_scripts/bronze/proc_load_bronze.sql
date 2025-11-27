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
this script IS A SQL server STORED PROCEDURE for loading DATA
FROM
    csv files INTO THE bronze layer.converting it TO postgresql requires replacing SQL server 's procedural language (T-SQL) elements, stored procedure syntax, and bulk load commands with PostgreSQL' s equivalents,
    primarily pl / pgSQL
    AND THE copy command.since file paths are specific TO THE server 's OS and user permissions, the paths have been kept as placeholders but the syntax for the COPY command is shown. 🐘 PostgreSQL Function Equivalent In PostgreSQL, stored procedures for ETL logic are typically implemented as FUNCTIONS or PROCEDURES using the PL/pgSQL language. Here is the converted PostgreSQL script: SQL -- PostgreSQL does not use CREATE OR ALTER PROCEDURE, it uses CREATE OR REPLACE FUNCTION/PROCEDURE. -- We will use a PROCEDURE here as it' s closer TO THE SQL server STORED PROCEDURE concept,
    -- but a simple FUNCTION could also be used if no transactions are needed.
    CREATE
    OR REPLACE PROCEDURE bronze.load_bronze() AS $$ -- DECLARE block for variables
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
    -- Start a transaction to ensure atomicity
    batch_start_time:= clock_timestamp();
-- Use clock_timestamp() for current time
    RAISE notice '================================================';
RAISE notice 'Loading Bronze Layer';
RAISE notice '================================================';
-- Note: PostgreSQL error handling (EXCEPTION block) is at the end of the procedure.
    --------------------------------------------------------------------------------------------------------------------
    -- Loading CRM Tables
    --------------------------------------------------------------------------------------------------------------------
    RAISE notice '------------------------------------------------';
RAISE notice 'Loading CRM Tables';
RAISE notice '------------------------------------------------';
-- bronze.crm_cust_info
    start_time:= clock_timestamp();
RAISE notice '>> Truncating Table: bronze.crm_cust_info';
TRUNCATE TABLE bronze.crm_cust_info;
RAISE notice '>> Inserting Data Into: bronze.crm_cust_info (Using COPY)';
-- The COPY command is PostgreSQL's highly efficient bulk loading tool.
    -- The file path must be accessible by the PostgreSQL server process.
    EXECUTE format(
        'COPY bronze.crm_cust_info FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','', ENCODING ''UTF8'')',
        'datasets/source_crm/cust_info.csv'
    );
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
-- bronze.crm_prd_info
    start_time:= clock_timestamp();
RAISE notice '>> Truncating Table: bronze.crm_prd_info';
TRUNCATE TABLE bronze.crm_prd_info;
RAISE notice '>> Inserting Data Into: bronze.crm_prd_info (Using COPY)';
EXECUTE format(
        'COPY bronze.crm_prd_info FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','', ENCODING ''UTF8'')',
        'datasets/source_crm/prd_info.csv'
    );
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
-- bronze.crm_sales_details
    start_time:= clock_timestamp();
RAISE notice '>> Truncating Table: bronze.crm_sales_details';
TRUNCATE TABLE bronze.crm_sales_details;
RAISE notice '>> Inserting Data Into: bronze.crm_sales_details (Using COPY)';
EXECUTE format(
        'COPY bronze.crm_sales_details FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','', ENCODING ''UTF8'')',
        'datasets/source_crm/sales_details.csv'
    );
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
-- bronze.erp_loc_a101
    start_time:= clock_timestamp();
RAISE notice '>> Truncating Table: bronze.erp_loc_a101';
TRUNCATE TABLE bronze.erp_loc_a101;
RAISE notice '>> Inserting Data Into: bronze.erp_loc_a101 (Using COPY)';
EXECUTE format(
        'COPY bronze.erp_loc_a101 FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','', ENCODING ''UTF8'')',
        'datasets/source_erp/loc_a101.csv'
    );
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
-- bronze.erp_cust_az12
    start_time:= clock_timestamp();
RAISE notice '>> Truncating Table: bronze.erp_cust_az12';
TRUNCATE TABLE bronze.erp_cust_az12;
RAISE notice '>> Inserting Data Into: bronze.erp_cust_az12 (Using COPY)';
EXECUTE format(
        'COPY bronze.erp_cust_az12 FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','', ENCODING ''UTF8'')',
        'datasets/source_erp/cust_az12.csv'
    );
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
-- bronze.erp_px_cat_g1v2
    start_time:= clock_timestamp();
RAISE notice '>> Truncating Table: bronze.erp_px_cat_g1v2';
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
RAISE notice '>> Inserting Data Into: bronze.erp_px_cat_g1v2 (Using COPY)';
EXECUTE format(
        'COPY bronze.erp_px_cat_g1v2 FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','', ENCODING ''UTF8'')',
        'datasets/source_erp/px_cat_g1v2.csv'
    );
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
batch_end_time:= clock_timestamp();
total_duration_sec:= EXTRACT(
        epoch
        FROM
            (
                batch_end_time - batch_start_time
            )
    );
RAISE notice '==========================================';
RAISE notice 'Loading Bronze Layer is Completed';
RAISE notice ' - Total Load Duration: % seconds',
    total_duration_sec;
RAISE notice '==========================================';
-- PostgreSQL's EXCEPTION block handles errors
EXCEPTION
    WHEN OTHERS THEN GET stacked diagnostics error_msg = message_text,
    error_state = pg_exception_detail;
RAISE notice '==========================================';
RAISE notice 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
RAISE notice 'Error Message: %',
    error_msg;
RAISE notice 'Error Details: %',
    error_state;
RAISE notice '==========================================';
-- Optionally re-raise the error to stop execution:
    -- RAISE;
END;$$ LANGUAGE plpgsql;
-- To execute the procedure:
-- CALL bronze.load_bronze();
