CREATE OR ALTER PROCEDURE Bronze.load_bronze_layer AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @layer_start_time DATETIME, @layer_end_time DATETIME;

	SET @layer_start_time = GETDATE();
	SET @start_time = GETDATE();
	TRUNCATE TABLE Bronze.crm_cust_info;
	PRINT 'Loading data to Bronze.crm_cust_info:';
	BULK INSERT Bronze.crm_cust_info
	FROM 'D:\Ezz D\SQL Ultimate course (30 Hours)\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
	with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT 'Load duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
	PRINT ''


	PRINT '--------------------------------------------------------------'
	SET @start_time = GETDATE();
	TRUNCATE TABLE Bronze.crm_prd_info
	PRINT 'Loading data to Bronze.crm_prd_info:'
	BULK INSERT Bronze.crm_prd_info
	FROM 'D:\Ezz D\SQL Ultimate course (30 Hours)\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
	with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	)
	SET @end_time = GETDATE();
	PRINT 'Load duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
	PRINT ''
	

	PRINT '--------------------------------------------------------------'
	SET @start_time = GETDATE();
	TRUNCATE TABLE Bronze.crm_sales_details
	PRINT 'Loading data to Bronze.crm_sales_details:'
	BULK INSERT Bronze.crm_sales_details
	FROM 'D:\Ezz D\SQL Ultimate course (30 Hours)\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
	with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	)
	SET @end_time = GETDATE();
	PRINT 'Load duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
	PRINT ''
	

	PRINT '--------------------------------------------------------------'
	SET @start_time = GETDATE();
	TRUNCATE TABLE Bronze.erp_cust_az12
	PRINT 'Loading data to Bronze.erp_cust_az12:'
	BULK INSERT Bronze.erp_cust_az12
	FROM 'D:\Ezz D\SQL Ultimate course (30 Hours)\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
	with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	)
	SET @end_time = GETDATE();
	PRINT 'Load duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
	PRINT ''
	

	PRINT '--------------------------------------------------------------'
	SET @start_time = GETDATE();
	TRUNCATE TABLE Bronze.erp_loc_a101
	PRINT 'Loading data to Bronze.erp_loc_a101:'
	BULK INSERT Bronze.erp_loc_a101
	FROM 'D:\Ezz D\SQL Ultimate course (30 Hours)\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
	with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	)
	SET @end_time = GETDATE();
	PRINT 'Load duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
	PRINT ''
	

	PRINT '--------------------------------------------------------------'
	SET @start_time = GETDATE();
	TRUNCATE TABLE Bronze.erp_px_cat_g1v2
	PRINT 'Loading data to Bronze.erp_px_cat_g1v2:'
	BULK INSERT Bronze.erp_px_cat_g1v2
	FROM 'D:\Ezz D\SQL Ultimate course (30 Hours)\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
	with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	)
	SET @end_time = GETDATE();
	PRINT 'Load duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
	SET @layer_end_time = GETDATE();
	PRINT ''
	PRINT '--------------------------------------------------------------'
	PRINT ''
	PRINT 'Total loading duration:' + CAST(DATEDIFF(SECOND, @layer_start_time, @layer_end_time) AS NVARCHAR) + ' Seconds'
END
