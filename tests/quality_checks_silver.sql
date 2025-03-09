/*
====================================================================================
Data Quality Checks
====================================================================================
Script Purpose: 
	This script shows some examples of quality checks for data cnsistency, accuracy, 
	and standardization across the 'silver' schemas. It includes checks for:
	- Null or duplicate primary keys.
	- Unwanted spaces in string fields.
	- Data standardization and consistency. 
	- Invalid date ranges and orders.
	- Data consitency between related fields. 

Usuage Notes: 
	- Run tehse checks after loading Silver Layer. 
	- Investigate and resolve any discrepencies found during the checks. 
====================================================================================
*/

-- Checking for duplicated ids
SELECT
	cst_id
	, COUNT(*)
FROM bronze.crm_cust_info

GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Check for unwanted whitespace
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname)

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname <> TRIM(cst_lastname)

-- Data Standardization and Consistency 
SELECT DISTINCT cst_gndr 
FROM bronze.crm_cust_info

SELECT DISTINCT cst_marital_status 
FROM bronze.crm_cust_info

-- Check fo NULLs or Negative Numbers
SELECT prd_cost
FROM bronze.crm_prd_info 
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Check for Invalid Date Orders 
SELECT * 
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

--/-- fixing data using lead()
SELECT
	prd_id
	, prd_key
	, prd_nm
	, prd_start_dt
	, prd_end_dt
	, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt_test -- use start date of next relevant row
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

-- Invalid sales details dates 
SELECT
	NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0
OR LEN(sls_ship_dt) <> 8 
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101

--/-- Check for Invalid Date Orders
SELECT
	* 
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt  
OR sls_order_dt > sls_due_dt


-- Check Data Consitency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price 
-- >> Values must not be NULL, zero, or negative
SELECT DISTINCT
	sls_sales as old_sls_sales
	, sls_quantity
	, sls_price as old_sls_price
	-- new data
	, CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales 
	END AS sls_sales

	, CASE 
		WHEN sls_price IS NULL OR sls_price <= 0 
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price
	
FROM bronze.crm_sales_details
WHERE sls_sales <> sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0

ORDER BY sls_sales, sls_quantity, sls_price
/* 
The above data is cooked. and usually we need to refer back to the source system experts
Here are the rules we've come up with: 
	1. If Sales if negative, zero or null, derive it using Quantity and Price
	2. If Price is zero or null, calculate it using Sales and Quantity 
	3. If price is negative, convert it to a positive value 
*/ 

-- Identity Out-of-Range Dates 
SELECT DISTINCT 
	bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' 
OR bdate > GETDATE()