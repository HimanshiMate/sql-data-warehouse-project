 
---------STORED PROCEDURE -store multiple query in DB & query is executed in order anytime by user 
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time  DATETIME, @end_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME---calculate the performance---

	BEGIN TRY  --- ERROR HANDLING for finding is their any error or not (BEGIN TRY/CATCH , END BEGIN/TRY) work same as IF-ELSE
	    SET @batch_start_time = GETDATE();
	    PRINT'====================loading  bronze Layer====================';
        PRINT'-------loading crm data table---------------';

		SET @start_time = GETDATE();
		PRINT'<<Truncating table:bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT'<<Inserting data info:bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\matem\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',' ,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'<<LOAD DURATION:'+CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +'seconds';
		PRINT'<<<<<<<<<<<<<<<<<<<<<--------------------------------------------------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';


		SET @start_time = GETDATE();
        PRINT'<<Truncating table:bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT'<<Inserting data info:bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\matem\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',' ,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'<<LOAD DURATION:'+CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +'seconds';
		PRINT'<<<<<<<<<<<<<<<<<<<<<--------------------------------------------------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';


		SET @start_time = GETDATE();
		PRINT'<<Truncating table:bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT'<<Inserting data info:bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\matem\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',' ,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'<<LOAD DURATION:'+CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +'seconds';
		PRINT'<<<<<<<<<<<<<<<<<<<<<--------------------------------------------------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';


----------------------------------------erp data table -------------------------------------------------------
		PRINT'-------loading erp data table---------------';
		SET @start_time = GETDATE();
		PRINT'<<Truncating table:bronze.erp_cust_aZ12';
		TRUNCATE TABLE bronze.erp_cust_aZ12;
		PRINT'<<Inserting data info:bronze.erp_cust_aZ12';
		BULK INSERT bronze.erp_cust_aZ12
		FROM 'C:\Users\matem\Downloads\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',' ,
			TABLOCK
		);
		--SELECT * FROM bronze.erp_cust_aZ12
		SET @end_time = GETDATE();
	    PRINT'<<LOAD DURATION:'+CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +'seconds';
		PRINT'<<<<<<<<<<<<<<<<<<<<<--------------------------------------------------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';


		SET @start_time = GETDATE();
        PRINT'<<Truncating table:bronze.erp_loc_az12';
		TRUNCATE TABLE bronze.erp_loc_az12;
		PRINT'<<Inserting data info:bronze.erp_loc_az12';
		BULK INSERT bronze.erp_loc_az12
		FROM 'C:\Users\matem\Downloads\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',' ,
			TABLOCK
		);
		--SELECT * FROM bronze.erp_loc_az12
		SET @end_time = GETDATE();
		PRINT'<<LOAD DURATION:'+CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +'seconds';
		PRINT'<<<<<<<<<<<<<<<<<<<<<--------------------------------------------------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';


		SET @start_time = GETDATE();
		PRINT'<<Truncating table:bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT'<<Inserting data info:bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\matem\Downloads\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',' ,
			TABLOCK
		);
		--SELECT * FROM bronze.erp_px_cat_g1v2
        SET @end_time = GETDATE();
		PRINT'<<LOAD DURATION:'+CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +'seconds';
		PRINT'<<<<<<<<<<<<<<<<<<<<<--------------------------------------------------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
	    

		SET @batch_end_time = GETDATE();
		PRINT'<<COMPLETE BATCH LOAD DURATION:'+CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) +'seconds';
		PRINT'<<<<<<<<<<<<<<<<<<<<<----------------------------------------------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
	END TRY
	BEGIN CATCH
	   PRINT'=======================================';
	   PRINT'ERROR OOCCURED DURING LOADING BRONZE LAYER';
	   PRINT'Error Message' + ERROR_MESSAGE();
	   PRINT'Error Message' +CAST (ERROR_NUMBER() AS NVARCHAR);
	   PRINT'Error Message' +CAST (ERROR_STATE() AS NVARCHAR);
	   PRINT'======================================='
	END CATCH

END



EXEC bronze.load_bronze ---for execute the stored procedure-------------------


