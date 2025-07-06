--==========Data inserted into silver layer from bronze layer crm_cust_info
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
     DECLARE @start_time  DATETIME, @end_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME;---calculate the performance---

	 BEGIN TRY  --- ERROR HANDLING for finding is their any error or not (BEGIN TRY/CATCH , END BEGIN/TRY) work same as IF-ELSE
	    SET @batch_start_time = GETDATE();
	    PRINT'====================loading  silver Layer====================';
        PRINT'-------loading crm data table---------------';

		SET @start_time = GETDATE();
		PRINT'>>Truncate Table:silver.crm_cust_info';
		Truncate Table silver.crm_cust_info;
		PRINT'>>Insert data info:silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
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
		TRIM(cst_firstname) AS FirstName,
		TRIM(cst_lastname) AS LastName,
		CASE WHEN  UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
			 WHEN  UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
			 ELSE 'NA'
		END cst_marital_status,   --NORMALIZE the value in readable format

		CASE WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
			 ELSE 'NA'
		END cst_gndr ,--NORMALIZE the value in readable format 
		cst_create_date
		FROM(
			SELECT
			*,
			ROW_NUMBER() OVER(Partition BY cst_id ORDER BY cst_create_date DESC ) AS flag
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t WHERE flag =1; --select the most recent recored per customer
		SET @end_time = GETDATE();
		PRINT'<<LOAD DURATION:'+CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +'seconds';
		PRINT'<<<<<<<<<<<<<<<<<<<<<--------------------------------------------------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';


	  --==========Data inserted into silver layer from bronze layer crm_prd_info
	  SET @start_time = GETDATE();
		PRINT'>>Truncate Table:silver.crm_prd_info';
		Truncate Table silver.crm_prd_info;
		PRINT'>>Insert data info:silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
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
		REPLACE(SUBSTRING(prd_key,1,5),'-' ,'_') AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
			 WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
			 WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
			 WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
			 ELSE 'NA'
		END prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info

        SET @end_time = GETDATE();
		PRINT'<<LOAD DURATION:'+CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +'seconds';
		PRINT'<<<<<<<<<<<<<<<<<<<<<--------------------------------------------------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';


		--==========Data inserted into silver layer from bronze layer crm_sales_dtails
		SET @start_time = GETDATE();
		PRINT'>>Truncate Table:silver.crm_sales_details';
		Truncate Table silver.crm_sales_details;
		PRINT'>>Insert data info:silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt ,
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
		CASE WHEN sls_order_dt =0 OR LEN(sls_order_dt) !=8 THEN NULL -----order_date 
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
		END AS sls_order_dt,

		CASE WHEN sls_ship_dt =0 OR LEN(sls_ship_dt) !=8 THEN NULL  -----shipping_date
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
		END AS sls_ship_dt,

		CASE WHEN sls_due_dt =0 OR LEN(sls_due_dt) !=8 THEN NULL  -----due_date
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
		END AS sls_due_dt,

		CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales != sls_quantity *ABS(sls_price) THEN sls_quantity *ABS(sls_price)
			 ELSE sls_sales
		END sls_sales, -- Recalculate sales if original value is missing or incorrect

		sls_quantity,

		CASE WHEN sls_price IS NULL OR  sls_price <=0 THEN sls_sales/ NULLIF(sls_quantity,0)
			 ELSE sls_price  --Derive price if original value is invalid
		END sls_price
		FROM bronze.crm_sales_details;

		SET @end_time = GETDATE();
		PRINT'<<LOAD DURATION:'+CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +'seconds';
		PRINT'<<<<<<<<<<<<<<<<<<<<<--------------------------------------------------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';



		--==========Data inserted into silver layer from bronze layer erp_cust_aZ12
		SET @start_time = GETDATE();
		PRINT'>>Truncate Table:silver.erp_cust_aZ12';
		Truncate Table silver.erp_cust_aZ12;
		PRINT'>>Insert dat info:silver.erp_cust_aZ12';
		INSERT INTO silver.erp_cust_aZ12(
		cid,
		bdate,
		gen
		)
		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) --Remove 'NAS' prefix if present
			 ELSE cid
		END AS cid,
		CASE WHEN bdate> GETDATE() THEN NULL
			 ELSE bdate
		END AS bdate, -- Set future bd to Null 

		CASE WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
			 WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'
			 ELSE 'NA'
		END as gen --Normalize gender value & handle unknown cases
		FROM bronze.erp_cust_aZ12;

		SET @end_time = GETDATE();
		PRINT'<<LOAD DURATION:'+CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +'seconds';
		PRINT'<<<<<<<<<<<<<<<<<<<<<--------------------------------------------------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';



		--==========Data inserted into silver layer from bronze layer  erp_loc_az12
		SET @start_time = GETDATE();
		PRINT'>>Truncate Table:silver.erp_loc_az12';
		Truncate Table silver.erp_loc_az12;
		PRINT'>>Insert data info:silver.erp_loc_az12';
		INSERT INTO silver.erp_loc_az12(
		cid,
		cntry
		)

		SELECT 
		REPLACE(cid,'-','') AS cid,
		CASE WHEN UPPER(TRIM(cntry)) IN ('USA','US') THEN 'United states'
			 WHEN UPPER(TRIM(cntry))= 'DE' THEN 'Germany'
			 WHEN UPPER(TRIM(cntry))= '' OR cntry IS NULL THEN 'NA'
			 ELSE UPPER(TRIM(cntry))
		END AS cntry
		FROM bronze.erp_loc_az12;

		SET @end_time = GETDATE();
		PRINT'<<LOAD DURATION:'+CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +'seconds';
		PRINT'<<<<<<<<<<<<<<<<<<<<<--------------------------------------------------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';



		--==========Data inserted into silver layer from bronze layer  erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT'>>Truncate Table:silver.erp_px_cat_g1v2';
		Truncate Table silver.erp_px_cat_g1v2;
		PRINT'>>Insert data info:silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
		)

		SELECT 
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2
        
		SET @end_time = GETDATE();
		PRINT'<<LOAD DURATION:'+CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +'seconds';
		PRINT'<<<<<<<<<<<<<<<<<<<<<--------------------------------------------------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
	    

		SET @batch_end_time = GETDATE();
		PRINT'<<COMPLETE LOAD DURATION:'+CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) +'seconds';
		PRINT'<<<<<<<<<<<<<<<<<<<<<----------------------------------------------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
	END TRY
	BEGIN CATCH
	   PRINT'=======================================';
	   PRINT'ERROR OOCCURED DURING LOADING SILVER LAYER';
	   PRINT'Error Message' + ERROR_MESSAGE();
	   PRINT'Error Message' +CAST (ERROR_NUMBER() AS NVARCHAR);
	   PRINT'Error Message' +CAST (ERROR_STATE() AS NVARCHAR);
	   PRINT'======================================='
	END CATCH

END
EXEC silver.load_silver ---for execute the stored procedure-------------------
