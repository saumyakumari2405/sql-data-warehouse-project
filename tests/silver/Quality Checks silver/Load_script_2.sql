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

USE DataWarehouse;


--Check for Nulls or duplicates in primary key
--Expectation: No result
Select
prd_id, count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;


--prd_key has lots of information we will split this column
Select prd_id,
prd_key, SUBSTRING(prd_key,1,5) AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info;

--check in category table for distinct cat_id's in bronze layer
Select distinct id from bronze.erp_px_cat_g1v2;

/* In category table (bronze.erp_px_cat_g1v2) id column separator used is '_' while when we derive the cat_id 
from bronze.crm_prd_info table it uses '-' as separator
So we need to have matching information in order to join the table
For this we will use replace function with substring function when deriving cat_id from 
bronze.crm_prd_info table.
*/
Select prd_id,
prd_key, REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info;


/*Check if the cat_id derived after replace substring function matches with the id in the
category table*/
Select prd_id,
prd_key, REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
Where REPLACE(SUBSTRING(prd_key,1,5),'-','_') NOT IN 
(Select distinct id from bronze.erp_px_cat_g1v2);


/*
We will extract the other part of prd_key as prd_key
this new column will help us join this table with another table sales_details
*/
Select prd_id,
prd_key, REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info;

--checking the sales_details table
Select sls_prd_key from bronze.crm_sales_details;

/*
We will check if all the values after providing transformation are present in sales_details table
*/
Select prd_id,
prd_key, REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
where SUBSTRING(prd_key,7,len(prd_key)) not in (Select sls_prd_key from bronze.crm_sales_details);

/*since the above query gave many results so we are checking by providing one of the returned 
values as like function to check the data individualy for sample.
*/
Select prd_id,
prd_key, REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
where SUBSTRING(prd_key,7,len(prd_key)) not in (Select sls_prd_key from bronze.crm_sales_details
where sls_prd_key like 'FK%');


/*Quality check for string column - Check for any unwanted spaces for prd_nm column
Expectation: No Results
Actual Result: No Results*/
Select prd_nm
from bronze.crm_prd_info
where prd_nm != TRIM(prd_nm);

/*Quality check for number column - Check for nulls or negative numbers for prd_cost column
Expectation: No Results
Actual Result: We find 2 nulls
To handle the null values we will replace null with 0 if business allows that.*/
Select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null;

/*handling the null values we will replace null with 0 if business allows that.*/
Select prd_id,
prd_key, REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
prd_nm,
ISNULL(prd_cost,0) as prd_cost, --We can use coalesce function as well in place of isnull function
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info;


/*We check if we still have any null or negative values in prd_cost column*/
Select prd_id,
prd_key, REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
prd_nm,
ISNULL(prd_cost,0) as prd_cost, --We can use coalesce function as well in place of isnull function
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
where ISNULL(prd_cost,0) < 0 or ISNULL(prd_cost,0) is null;


/*Data Quality check - prd_line column
since its a abbreviation and we do not understand it
We will check all the possible values for this column
*/
Select distinct prd_line
from bronze.crm_prd_info;

/*Data Quality check - prd_line column
We will write the actual values using case statements after talking with data experts.
In this case we will write as below
*/
Select prd_id,
prd_key, REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
prd_nm,
ISNULL(prd_cost,0) as prd_cost, --We can use coalesce function as well in place of isnull function
CASE UPPER(TRIM(prd_line))
WHEN 'M' THEN 'Mountain'
WHEN 'R' THEN 'Road'
WHEN 'S' THEN 'other Sales'
WHEN 'T' THEN 'Touring'
ELSE 'n/a'
END as prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info;


/*Data Quality check for invalid date orders 
we check if we have any start date that is greater than end_dt
Since we get a lot of results by this query
For complex transformations like these in SQL, we typically need to narrow it down
to a specific example and brainstorm multiple solution approaches.
*/
Select * 
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt


/*
Record sampling for the above issue
*/
Select prd_id,
prd_key, REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
prd_nm,
ISNULL(prd_cost,0) as prd_cost, --We can use coalesce function as well in place of isnull function
CASE UPPER(TRIM(prd_line))
WHEN 'M' THEN 'Mountain'
WHEN 'R' THEN 'Road'
WHEN 'S' THEN 'other Sales'
WHEN 'T' THEN 'Touring'
ELSE 'n/a'
END as prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
where
REPLACE(SUBSTRING(prd_key,1,5),'-','_') like 'AC_HE'

/*After observing the above sample records we can have multiple solution
1. Switch the end date and start date
But if you do this you will observe that
when for prd_id 212 prd_start_dt = '2007-12-28' and prd_end_dt = '2011-07-01'
and prd_cost is 12 and for prd_id 213
we should also see end_dt of previous record should be smaller than the start_dt of next record for a 
particular product
Also each record should have a start date

2. Derive the end date of the record from the start date for the next record
*/


/*for solution 2 we will build logic by using sample records and then implement it*/
Select
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
DATEADD(DAY,-1,LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) as prd_end_dt_test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R','AC-HE-HL-U509')


/* Now we will put this prd_end_dt_test logic into our query that has other transformations*/
Select prd_id,
prd_key, REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
prd_nm,
ISNULL(prd_cost,0) as prd_cost, --We can use coalesce function as well in place of isnull function
CASE UPPER(TRIM(prd_line))
WHEN 'M' THEN 'Mountain'
WHEN 'R' THEN 'Road'
WHEN 'S' THEN 'other Sales'
WHEN 'T' THEN 'Touring'
ELSE 'n/a'
END as prd_line,
prd_start_dt,
DATEADD(DAY,-1,LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) as prd_end_dt
from bronze.crm_prd_info;

/*Since our date columns do not have time information
it makes no sense to have that extra data 
hence we will put some transformation and take only the date part of that column*/
Select prd_id,
prd_key, REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
prd_nm,
ISNULL(prd_cost,0) as prd_cost, --We can use coalesce function as well in place of isnull function
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

/*Since we have transformed few column datatypes and few derived columns
hence we will run this query to change the structure of the silver table that we created earlier
*/
IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

/*We will insert the date into silver table with our transformations*/
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

/*We need to cehck the data quality - primary key duplicate check*/
Select
prd_id,
count(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;

/*We need to cehck the data quality - unwanted space check*/
Select prd_cost
from silver.crm_prd_info
where prd_cost < 0 OR prd_cost IS NULL;

/*We need to cehck the data quality - Data standardization & Consistency check*/
Select distinct prd_line
from silver.crm_prd_info;

/*We need to cehck the data quality - Invalid date orders check*/
Select *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt;
