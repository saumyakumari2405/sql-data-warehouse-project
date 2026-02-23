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


--=====================================Check for nulls or duplicates in primary Key========

----------------------------------Step1 Find the duplicates or nulls----------------------------------
Select cst_id, count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 OR cst_id IS NULL;
-----------------------------Step1 We get the cst_id that have multiple records or are null----------


----------------Step2 Find how we can pick only one record of a pk that has multiple records-----------
Select * from bronze.crm_cust_info
where cst_id = 29466 --<<29466 is just used as a value for the placeholder>>
-----Step2 cst_create_date we can pick latest record of 29466 cst_id pk that has multiple records-----------


----------------Step3 Assign Rank to each record based on cst_create_date column-----------
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
----------------Step3 Ranks to each record based on cst_create_date column-----------------



---------Step4 We pick only the ecords that have flag_last = 1 (we pick the latest record for each pk-----
Select * from (
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info )t
where flag_last = 1
-------Step4 We picked only the ecords that have flag_last = 1 (we pick the latest record for each pk--


--================================Nulls and dupliacte issue resolved=======================================




--================================Check for unwanted space=======================================

----------------------------------Step1 Detect the cst_firstname that have unwanted space----------------------------------
Select cst_firstname from 
bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname); --like this check for all string columns one by one
----------------------------------Step1 cst_firstname that have unwanted space detected----------------------------------

----------------------------------Step2 Detect the cst_lastname that have unwanted space----------------------------------
Select cst_lastname from 
bronze.crm_cust_info
where cst_lastname != TRIM(cst_lastname); --like this check for all string columns one by one
----------------------------------Step3 cst_lastname that have unwanted space detected----------------------

----------------------------------Step3 Detect the cst_gndr that have unwanted space----------------------------------
Select cst_gndr from 
bronze.crm_cust_info
where cst_gndr != TRIM(cst_gndr); --like this check for all string columns one by one
----------------------------------Step3 cst_gndr that have unwanted space detected----------------------


------------------Step4 we trim the spaces for the columns that gave any result from the abve query---------
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
where flag_last = 1


--================================Unwanted space issue resolved=======================================





--=================================INSERT INTO silver layer============================================
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
where flag_last = 1
--=================================INSERT INTO silver layer============================================


--=========================Checking silver data==================================

---------------------------check duplicate or null values----------------------
Select cst_id, count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 OR cst_id IS NULL;



------------------------------Check unwanted spaces--------------------
Select cst_firstname from 
silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname);
