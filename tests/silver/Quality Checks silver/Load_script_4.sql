USE DataWarehouse;

/*
Checking the data in erp_cust_az12 table
*/
Select
cid,
bdate,
gen
from bronze.erp_cust_az12;

/*
Checking the data in crm_cust_info table in silver layer
*/
Select * from silver.crm_cust_info;

/*
cst_key in silver.crm_cust_info --> AW00011000
cid in bronze.erp_cust_az12 --> NASAW00011000
We see that cid in bronze.erp_cust_az12 has a prefix of NAS for some cid's
While for other data we dont have that prefix.
So we need to cleanup the data of cid in bronze.erp_cust_az12 and remove that NAS prefix
before loading to silver layer.
*/
Select
cid,
CASE
	WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,len(cid))
	ELSE cid
END AS cid,
bdate,
gen
from bronze.erp_cust_az12;


/* We can check if the transformation that we applied is working or not 
by applying the transformation in where condition
*/
Select
cid,
CASE
	WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,len(cid))
	ELSE cid
END AS cid,
bdate,
gen
from bronze.erp_cust_az12
WHERE 
CASE
	WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,len(cid))
	ELSE cid
END NOT IN (SELECT DISTINCT cst_key from silver.crm_cust_info);


/*
Identify out of range dates 
It will be weird for business to have customers over 100 years of age.
or customers that have a birthdate greater than todays date
So even the data is fine.
Business wise it needs to be transformed. We need to check with source system experts
*/
Select bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' OR bdate > GETDATE();


/*
Data Quality - Will nullnify the data if bdate is greater than todays date
*/
Select
cid,
CASE
	WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,len(cid))
	ELSE cid
END AS cid,
CASE
	WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END AS bdate,
gen
from bronze.erp_cust_az12

/*
Data Quality - Data standardization & Consistency for gender column
*/
Select distinct gen
from bronze.erp_cust_az12;

/*
We need to transform the data of gen column to have only 3 values
Male, Female and N/A
*/
Select
cid,
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


/*
Insert the data into silver layer
*/
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

/*
Data Quality check of silver table
*/
Select bdate
from silver.erp_cust_az12
where bdate < '1924-01-01' OR bdate > GETDATE();

Select distinct gen
from silver.erp_cust_az12;

Select *
from silver.erp_cust_az12;



==============================================================ERP_LOC_101 TABLE ================================================================
/* 
Check the data in erp_loc_a101 table
*/
Select
cid,
cntry
from bronze.erp_loc_a101;


/* 
Check the data in crm_cust_info table
*/
Select cst_key from silver.crm_cust_info;

/*
cid in erp_loc_a101 table --> AW-00011000
cst_key in silver.crm_cust_info --> AW00011000
Data in erp_loc_a101 table is splitted with '-' while in silver.crm_cust_info it is not
*/
Select 
REPLACE(cid, '-', '') as cid,
cntry
from bronze.erp_loc_a101;

/*Check if our transformation is working correctly or not*/
Select 
REPLACE(cid, '-', '') as cid,
cntry
from bronze.erp_loc_a101
where REPLACE(cid, '-', '') NOT IN 
(Select cst_key from silver.crm_cust_info);


/*Data Standardization & Consistency*/
Select distinct cntry
from bronze.erp_loc_a101
order by cntry;

/*
*/
Select 
REPLACE(cid, '-', '') as cid,
CASE
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	WHEN TRIM(cntry) = '' or cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
from bronze.erp_loc_a101


/*
Insert into silver table
*/
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


/*Data Quality check on silver table*/
Select cid, cntry from silver.erp_loc_a101;







===========================================================ERP_PX_CAT_G1V2===========================================================================
/* 
Check the bronze table
*/
Select id, cat, subcat, maintenance from bronze.erp_px_cat_g1v2;

/*
We had created cat_id as a derived column while loading the silver.crm_prd_info
and id column of bronze.erp_px_cat_g1v2 matches with cat_id of silver.crm_prd_info
*/

/*
Check for unwanted spaces in cat column
Expected result: No results
Actual result: It matched and we dont get any result so we dont have any unwanted spaces in this column
*/
Select cat from bronze.erp_px_cat_g1v2
where cat != TRIM(cat);


/*
Check for unwanted spaces in subcat column
Expected result: No results
Actual result: It matched and we dont get any result so we dont have any unwanted spaces in this column
*/
Select subcat from bronze.erp_px_cat_g1v2
where subcat != TRIM(subcat);


/*
Check for unwanted spaces in maintenance column
Expected result: No results
Actual result: It matched and we dont get any result so we dont have any unwanted spaces in this column
*/
Select maintenance from bronze.erp_px_cat_g1v2
where maintenance != TRIM(maintenance);

/*
Data Standardization & Consistency for cat column
*/
Select distinct cat from bronze.erp_px_cat_g1v2;

/*
Data Standardization & Consistency for subcat column
*/
Select distinct subcat from bronze.erp_px_cat_g1v2;

/*
Data Standardization & Consistency for maintenance column
*/
Select distinct maintenance from bronze.erp_px_cat_g1v2;

/*
Inserting data into silver table
*/
INSERT INTO silver.erp_px_cat_g1v2
(id, cat, subcat, maintenance)
Select
id, cat, subcat, maintenance
from bronze.erp_px_cat_g1v2;

/*Check the data quality of silver table*/
Select * from silver.erp_px_cat_g1v2
