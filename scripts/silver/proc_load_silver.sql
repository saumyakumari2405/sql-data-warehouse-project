/*
=========================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=========================================================================================
Script Purpose:
	  This stored procedure performs the ETL (Extract, Transform, Load) process to populate the 
    'silver' schema tables from the 'bronze' schema.
  Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from bronze into silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values

Usage Example:
    EXEC silver.load_silver;

==============================================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '==========================================';
		PRINT 'Loading Silver Layer';
		PRINT '==========================================';
		
		PRINT '-------------------------------------------';
		PRINT 'Loading CRM Tables'
		PRINT '-------------------------------------------';
		
		
		SET @start_time = GETDATE();
			/*
			=================================crm_cust_info==============================================
			*/
			PRINT '>> Truncating Table: silver.crm_cust_info';
			TRUNCATE TABLE silver.crm_cust_info;
			PRINT '>> Inserting Data into: silver.crm_cust_info';
			INSERT INTO silver.crm_cust_info (
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date
			)
			Select cst_id, cst_key, 
			TRIM(cst_firstname) as cst_firstname,
			TRIM(cst_lastname) as cst_lastname,
			CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
				ELSE 'N/A' 
			END AS cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'N/A' 
			END AS cst_gndr,
			cst_create_date
			from (
			SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info WHERE cst_id is not null )t
			where flag_last = 1;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: crm_cust_info table: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';



		SET @start_time = GETDATE();
			/*
			=================================crm_prd_info==============================================
			*/
			PRINT '>> Truncating Table: silver.crm_prd_info';
			TRUNCATE TABLE silver.crm_prd_info;
			PRINT '>> Inserting Data into: silver.crm_prd_info';
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
			Select prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
			SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
			prd_nm,
			ISNULL(prd_cost,0) as prd_cost,
			CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
			END as prd_line,
			CAST(prd_start_dt as date),
			cast(DATEADD(DAY,-1,LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) as date) as prd_end_dt
			from bronze.crm_prd_info;
			
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: crm_prd_info table: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';



		
		SET @start_time = GETDATE();
			/*
			=================================crm_sales_details==============================================
			*/
			PRINT '>> Truncating Table: silver.crm_sales_details';
			TRUNCATE TABLE silver.crm_sales_details;
			PRINT '>> Inserting Data into: silver.crm_sales_details';
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

			Select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity*abs(sls_price)
						THEN sls_quantity*abs(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity,0)
				ELSE sls_price
			END AS sls_price
			from bronze.crm_sales_details;
			
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: crm_sales_details table: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';



		
		SET @start_time = GETDATE();
	
			/*
			=================================erp_loc_a101==============================================
			*/
			PRINT '>> Truncating Table: silver.erp_loc_a101';
			TRUNCATE TABLE silver.erp_loc_a101;
			PRINT '>> Inserting Data into: silver.erp_loc_a101';
			INSERT INTO silver.erp_loc_a101 (cid, cntry)
			Select 
			REPLACE(cid, '-', '') as cid,
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
				WHEN TRIM(cntry) = '' or cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry
			from bronze.erp_loc_a101;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: erp_loc_a101 table: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';



		SET @start_time = GETDATE();
			/*
			=================================erp_px_cat_g1v2==============================================
			*/
			PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
			TRUNCATE TABLE silver.erp_px_cat_g1v2;
			PRINT '>> Inserting Data into: silver.erp_px_cat_g1v2';
			INSERT INTO silver.erp_px_cat_g1v2
			(id, cat, subcat, maintenance)
			Select
			id, cat, subcat, maintenance
			from bronze.erp_px_cat_g1v2;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: erp_px_cat_g1v2 table: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';



		SET @start_time = GETDATE();
			/*
			=================================erp_cust_az12==============================================
			*/
			PRINT '>> Truncating Table: silver.erp_cust_az12';
			TRUNCATE TABLE silver.erp_cust_az12;
			PRINT '>> Inserting Data into: silver.erp_cust_az12';
			INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
			Select
			CASE
				WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,len(cid))
				ELSE cid
			END AS cid,
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate,
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen
			from bronze.erp_cust_az12;
			
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: erp_cust_az12 table: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
		
	
	SET @batch_end_time = GETDATE();
	PRINT '====================================================';
	PRINT 'Loading Silver Layer is completed';
	PRINT ' - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) as NVARCHAR) + ' seconds';
	
	END TRY
	
	
	BEGIN CATCH
			--We can do multiple things int this catch block
			--1. Create a logging table and add the messages inside this table.
			--2. Add some nice messaging
			PRINT '===================================================================================='
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
			PRINT '===================================================================================='
		END CATCH

END;


/*to execute use the below command
EXEC silver.load_silver;
*/
