/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose: 
	This stored procedure peforms the ELT (extract, transform, load) process to 
	populate the 'silver' schema tables from the 'bronze' schema. 

Actions Peformed: 
	- Truncates Silver tables. 
	- Inserts trasnformed and cleansed data from Bronze into Silver tables. 

Parameters:
	None. 
	This stored procedure does not accept any parameters or return any values. 

Usage Example: 
	EXEC silver.load_silver;
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	-- todo: enhance script by adding additional messaging 

	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> Inserting Data Into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info (
		cst_id
		, cst_key
		, cst_firstname
		, cst_lastname
		, cst_marital_status
		, cst_gndr
		, cst_create_date
	)
	SELECT
		cst_id
		, cst_key
		, TRIM(cst_firstname) AS cst_firstname
		, TRIM(cst_lastname) AS cst_lastname
		, CASE UPPER(TRIM(cst_gndr))
			WHEN 'F' THEN 'Female'
			WHEN 'M' THEN 'Male'
			ELSE 'n/a'
		END AS cst_gndr
		, CASE UPPER(TRIM(cst_marital_status))
			WHEN 'S' THEN 'Single'
			WHEN 'M' THEN 'Married'
			ELSE 'n/a'
		END AS cst_marital_status 
		, cst_create_date
	FROM (
		SELECT 
			* ,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
	) t 
		WHERE t.flag_last = 1 
		AND t.cst_id IS NOT NULL

	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>> Inserting Data Into: silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info(
		prd_id
		, cat_id
		, prd_key
		, prd_nm
		, prd_cost
		, prd_line
		, prd_start_dt
		, prd_end_dt
	)
	SELECT
		prd_id
		, REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id
		, SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
		, prd_nm
		, ISNULL(prd_cost, 0) AS prd_cost
		, CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line
		, CAST(prd_start_dt AS DATE) AS prd_start_dt
		, CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE) AS prd_end_dt
	FROM bronze.crm_prd_info

	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>> Inserting Data Into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details (
		sls_ord_num
		, sls_prd_key
		, sls_cust_id
		, sls_order_dt
		, sls_ship_dt
		, sls_due_dt
		, sls_sales
		, sls_quantity
		, sls_price
	)
	SELECT 
		sls_ord_num
		, sls_prd_key
		, sls_cust_id
		, CASE 
			WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt
		, CASE 
			WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt
		, CASE 
			WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt
		, CASE 
			WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales 
		END AS sls_sales
		, sls_quantity
		, CASE 
			WHEN sls_price IS NULL OR sls_price <= 0 
				THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price
		END AS sls_price
	FROM bronze.crm_sales_details

	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>> Inserting Data Into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
	SELECT
		CASE
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
		END AS cid
		, CASE
			WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate
		, CASE
			WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
		END AS gen
	FROM bronze.erp_cust_az12; 

	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '>> Inserting Data Into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101 (cid, cnrty)
	SELECT 
		REPLACE(cid, '-', '') as cid
		, CASE
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry	
	FROM bronze.erp_loc_a101;

	-- erp_px_cat_g1v2
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	INSERT INTO silver.erp_px_cat_g1v2(
		id
		, cat 
		, subcat
		, maintenence
	)
	SELECT 
		id
		, cat 
		, subcat
		, maintenence
	FROM bronze.erp_px_cat_g1v2;

END 