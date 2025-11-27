/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Terminate existing connections to the target database
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'datawarehouse'
  AND pid <> pg_backend_pid();

-- Drop the database if it exists
DROP DATABASE IF EXISTS datawarehouse;

-- Create the database
-- Note: This command must be run while connected to a different database (e.g., 'postgres')
CREATE DATABASE datawarehouse;

-- Reconnect to the new database
\c datawarehouse

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS gold;
