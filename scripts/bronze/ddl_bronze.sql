/*
===========================================
DDL Script: Create DDL for Bronze Layer
===========================================
Script Purpose: 
	This script creates tables in the 'bronze' schema, dropping existing tables
	if they already exist. 
	Run this script to re-define the DDL structure of 'bronze' tables
*/


USE DataWarehouse; 
GO

-- crm source system 
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
	cst_id             INT,
	cst_key            NVARCHAR(50),
	cst_firstname      NVARCHAR(50),
	cst_lastname       NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr		   NVARCHAR(50),
	cst_create_date	   DATE
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
	prd_id       INT, 
	prd_key      NVARCHAR(50),
	prd_nm       NVARCHAR(50),
	prd_cost     INT,
	prd_line     NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt   DATE
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num  NVARCHAR(50),
	sls_prd_key  NVARCHAR(50),
	sls_cust_id  INT,
	sls_order_dt INT,
	sls_ship_dt  INT, 
	sls_due_dt   INT,
	sls_sales    INT,
	sls_quantity INT, 
	sls_price    INT
);

-- erp source system
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
	cid   NVARCHAR(50),
	cnrty NVARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
	cid   NVARCHAR(50),
	bdate DATE,
	gen   NVARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
	id			NVARCHAR(50),
	cat			NVARCHAR(50),
	subcat		NVARCHAR(50),
	maintenence NVARCHAR(50)
);