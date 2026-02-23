/*
==========================================================================================================================
Quality Checks
==========================================================================================================================

Script Purpose:
		This script performs various quality checks for data consistency, accuracy, and standardization across the 'silver'
		schema. It includes checks for:
		- Null or duplicate primary keys
		- Unwanted spaces in string fields
		- Data standardization and consistency
		- Invalid date ranges and orders
		- Data consistency between related fields

Usage Notes:
		- Run these checks after data loading silver Layer.
		- Investigate and resolve any discrepencies found during the checks.
==========================================================================================================================
*/

IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);


BULK INSERT DataWarehouse.bronze.crm_sales_details
			FROM 'C:\Users\Saumya Kumari\Desktop\STUDY\Data_Warehouse\sales_details_original.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);

Select * from bronze.crm_sales_details
/*Data Quality check - for unwanted spaces*/
Select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_ord_num != TRIM(sls_ord_num)

/*Data Quality check - for sls_prd_key*/
Select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_prd_key NOT IN (Select prd_key from silver.crm_prd_info);

/*Data Quality check - for sls_cust_key*/
Select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_cust_id NOT IN (Select cst_id from silver.crm_cust_info);

/*Data Quality check - for date column sls_ordr_dt*/
Select
sls_order_dt as int
from bronze.crm_sales_details
where sls_order_dt <= 0;

/*Data Quality check - Resolution for date column sls_ordr_dt*/
Select
NULLIF(sls_order_dt,0) as sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 or len(sls_order_dt) != 8 or sls_order_dt > 20500101 
or sls_order_dt < 19000101;

/*Fixing the sls_order_dt issue*/
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
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details

/*Check for invalid date orders
that sls_order_dt cannot be greater than sls_ship_date
*/
Select * from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt
or sls_order_dt > sls_ship_dt or sls_ship_dt > sls_due_dt;

/*Data Quality check - sls_sales, sls_quantity, sls_price
We have a business rule that says
sales = quantity * price
Negative, zeros, null are not allowed*/
Select distinct sls_sales, sls_quantity, sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null 
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales,sls_quantity, sls_price;

/*We check with experts and we get few solutions
1. Data Issues will be fixed direct in source system
2. Data Issues has to be fixed in data warehouse

If option 2 then rules are:
a) if sales is -ve, 0 or null derive it using quantity and price
b) if price is zero or null, calculate it using sales and quantity
c) if price is negative, convert it to a positive value
Lets build the transaformation based on the above rules
*/
Select distinct sls_sales as old_sls_sales,
sls_quantity as old_sls_quantity, 
sls_price as old_sls_price,
CASE 
	WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity*abs(sls_price)
			THEN sls_quantity*abs(sls_price)
	ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price
END AS sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null 
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales,sls_quantity, sls_price;


/*
fixing sls_sales,sls_quantity,sls_price data
*/
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
from bronze.crm_sales_details

/*After fixing the data our silver table structure might not match 
with the data after transformations
so check the structure of the silver table once and perform the 
DDL again if necessary
*/
IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

/*Inserting the sales_details data into our silver table*/
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


/*Check the health of our silver table*/
Select * from silver.crm_sales_details;
