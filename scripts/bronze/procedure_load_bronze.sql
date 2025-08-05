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
    CALL bronze.load_bronze();
===============================================================================
Other ways to load data you can import the data directly on the pg admin Interface or...
by terminal using \copy if you have not the permission required
-first command: psql -U postgres -d datawarehouse -h localhost
-second_command: \copy FROM 'chemin ou path' DELIMITER ',' HEADER CSV;
*/

-- FOR CRM SOURCES

-- FOR CRM SOURCES

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN

    batch_start_time := now(); -- total time to load all the file
	
	start_time = now();        -- start time to load file crm_prd_info
	TRUNCATE TABLE bronze.crm_prd_info;
	COPY bronze.crm_prd_info
	FROM '/tmp/prd_info.csv'
	DELIMITER ','
	CSV HEADER;
	end_time = now();          -- end time to load file crm_prd_info
    RAISE NOTICE 'time to load file crm_prd_info : % secondes', EXTRACT(EPOCH FROM (end_time - start_time));

	start_time = now();        -- start time to load file crm_sales_details
	TRUNCATE TABLE bronze.crm_sales_details;
	COPY bronze.crm_sales_details
	FROM '/tmp/sales_details.csv'
	DELIMITER ','
	CSV HEADER;
	end_time = now();          -- end time to load file crm_sales_details
    RAISE NOTICE 'time to load file crm_sales_details : % secondes', EXTRACT(EPOCH FROM (end_time - start_time));

	start_time = now();        -- start time to load file crm_prd_info
	TRUNCATE TABLE bronze.crm_cust_info;
	COPY bronze.crm_cust_info
	FROM '/tmp/cust_info.csv'
	DELIMITER ','
	CSV HEADER;
	end_time = now();          -- end time to load file crm_cust_info
    RAISE NOTICE 'time to load file crm_cust_info : % secondes', EXTRACT(EPOCH FROM (end_time - start_time));
	-- FOR ERP SOURCES
	start_time = now();        -- start time to load all ERP file

	TRUNCATE TABLE bronze.erp_loc_a101;
	COPY bronze.erp_loc_a101
	FROM '/tmp/LOC_A101.csv'
	DELIMITER ','
	CSV HEADER;
	
	TRUNCATE TABLE bronze.erp_cust_az12;
	COPY bronze.erp_cust_az12
	FROM '/tmp/CUST_AZ12.csv'
	DELIMITER ','
	CSV HEADER;
	
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	COPY bronze.erp_px_cat_g1v2
	FROM '/tmp/PX_CAT_G1V2.csv'
	DELIMITER ','
	CSV HEADER;
	end_time = now();          -- end time to load all ERP files
    RAISE NOTICE 'time to load all ERP files: % secondes', EXTRACT(EPOCH FROM (end_time - start_time));
	
    batch_end_time := now();
    RAISE NOTICE 'Durée totale du chargement : % secondes', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
END;
$$;
