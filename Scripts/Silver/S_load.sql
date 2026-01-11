/*
Inserting Data into the Silver layer Stored procedure:
==================================================
Function:
	This Stored Procedure inserts data from the Bronze layer
	into the silver layer.

To use the scrtipt run this:
	EXEC Silver.load_silver_layer
*/

CREATE OR ALTER PROCEDURE Silver.load_silver_layer AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @layer_start_time DATETIME, @layer_end_time DATETIME;

	SET @layer_start_time = GETDATE();
	SET @start_time = GETDATE();
	PRINT 'Inserting into Silver.crm_cust_info'
	TRUNCATE TABLE Silver.crm_cust_info
	INSERT INTO Silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)
	SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE
			WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			ELSE 'N/A'
		END AS cst_marital_status,
		CASE
			WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
			ELSE 'N/A'
		END AS cst_gndr,
		cst_create_date
	FROM(
		SELECT
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
		FROM Bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
	) t
	WHERE flag = 1
	SET @end_time = GETDATE();
	PRINT 'Load duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
	PRINT ''

	PRINT'------------------------------------------------------------------------------------------------'
	PRINT ''

	PRINT 'Inserting into crm_prd_info'
	SET @start_time = GETDATE();
	TRUNCATE TABLE Silver.crm_prd_info
	INSERT INTO Silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		COALESCE(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'T' THEN 'Touring'
			WHEN 'S' THEN 'Other sales'
			ELSE 'N/A'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
	FROM Bronze.crm_prd_info
	SET @end_time = GETDATE();
	PRINT 'Load duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
	PRINT ''

	PRINT'------------------------------------------------------------------------------------------------'
	PRINT ''

	PRINT 'Inserting into Silver.crm_sales_details'
	SET @start_time = GETDATE();
	TRUNCATE TABLE Silver.crm_sales_details
	INSERT INTO Silver.crm_sales_details(
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
	SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE
			WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS varchar) AS date)
		END AS sls_order_dt,
		CASE
			WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS varchar) AS date)
		END AS sls_ship_dt,
		CASE
			WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS varchar) AS date)
		END AS sls_due_dt,
		CASE
			WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE
			WHEN sls_price IS NULL OR sls_price <=0
				THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price
		END AS sls_price
	FROM Bronze.crm_sales_details
	SET @end_time = GETDATE();
	PRINT 'Load duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
	PRINT ''

	PRINT'------------------------------------------------------------------------------------------------'
	PRINT ''

	PRINT 'Inserting into Silver.erp_cust_az12'
	SET @start_time = GETDATE();
	TRUNCATE TABLE Silver.erp_cust_az12
	INSERT INTO Silver.erp_cust_az12(
		cid,
		bdate,
		gen
	)
	SELECT
		CASE
			WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
			ELSE CID
		END AS cid,
		CASE
			WHEN BDATE > GETDATE() THEN NULL
			ELSE BDATE
		END AS bdate,
		CASE
			WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Fmale'
			WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'N/A'
		END AS gen
	FROM Bronze.erp_cust_az12
	SET @end_time = GETDATE();
	PRINT 'Load duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
	PRINT ''

	PRINT'------------------------------------------------------------------------------------------------'
	PRINT ''

	PRINT 'Inserting into Silver.erp_loc_a101'
	SET @start_time = GETDATE();
	TRUNCATE TABLE Silver.erp_loc_a101
	INSERT INTO Silver.erp_loc_a101 (cid, cntry)
	SELECT
		REPLACE(CID, '-', '') AS cid,
		CASE
			WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
			WHEN TRIM(CNTRY)  IN('US', 'USA') THEN 'United States'
			WHEN TRIM(CNTRY) = '' or CNTRY IS NULL THEN 'N/A'
			ELSE TRIM(CNTRY)
		END AS cntry
	FROM Bronze.erp_loc_a101
	SET @end_time = GETDATE();
	PRINT 'Load duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
	PRINT ''

	PRINT'------------------------------------------------------------------------------------------------'
	PRINT ''

	PRINT 'Inserting into Silver.erp_px_cat_g1v'
	SET @start_time = GETDATE();
	TRUNCATE TABLE Silver.erp_px_cat_g1v2
	INSERT INTO Silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
	FROM Bronze.erp_px_cat_g1v2
	SET @end_time = GETDATE();
	PRINT 'Load duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
	SET @layer_end_time = GETDATE();
	PRINT ''
	PRINT '--------------------------------------------------------------'
	PRINT ''
	PRINT 'Total loading duration:' + CAST(DATEDIFF(SECOND, @layer_start_time, @layer_end_time) AS NVARCHAR) + ' Seconds'
END
