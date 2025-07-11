CREATE OR ALTER PROCEDURE silver.load_silver_layer
As
BEGIN
	DECLARE @START_TIME DATETIME,@END_TIME DATETIME,@BATCH_START_TIME DATETIME,@BATCH_END_TIME DATETIME;
	BEGIN TRY
		PRINT'=============';
		PRINT 'LOADING SILVER LAYER';
		PRINT'=============';

		PRINT'--------------';
		PRINT 'LOADING CRM TABLES';
		PRINT'--------------';
		SET @START_TIME=GETDATE();
		SET @BATCH_START_TIME=GETDATE();
		PRINT '>> TRUNCATING TABLES: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info --IF DATA IS ALREADY THERE THEN DELETE IT

		PRINT '>> INSERTING DATA INTO: silver.crm_cust_info';
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
		TRIM(cst_firstname) AS cst_firstname,--remove unwanted spaces
		TRIM(cst_lastname) AS cst_lastname,--remove unwanted spaces
		CASE 
			WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'-- data normalization and standardization(maps coded values to meaningful,user-friendly description
			WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'-- data normalization and standardization(maps coded values to meaningful,user-friendly description
			ELSE 'n/a'--handled missing values
		END cst_marital_status,
		CASE 
			WHEN UPPER(TRIM(cst_gndr))='M' THEN 'MALE'-- data normalization and standardization(maps coded values to meaningful,user-friendly description
			WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'-- data normalization and standardization(maps coded values to meaningful,user-friendly description
			ELSE 'n/a'--handled missing values
		END cst_gndr,
		cst_create_date
		FROM (
		   SELECT *,
		   ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		   FROM bronze.crm_cust_info
		   WHERE cst_id IS NOT NULL
		) t WHERE flag_last = 1-- removed duplicay by using rowNumber

		--SELECT COUNT(*) FROM silver.crm_cust_info;
		SET @END_TIME=GETDATE();
		PRINT '>> LOAD DURATION :'+CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR)+' second';
		print'----------------------------';

		SET @START_TIME=GETDATE();
		PRINT '>> TRUNCATING TABLES: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info --IF DATA IS ALREADY THERE THEN DELETE IT

		PRINT '>> INSERTING DATA INTO: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
		prd_id ,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost ,
		prd_line,
		prd_start_dt,
		prd_end_dt 
		)
		SELECT 
		 prd_id,
		 REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,--derived new column
		  REPLACE(SUBSTRING(prd_key,7,LEN(prd_key)),'-','_') AS prd_key,--derived new column
		 prd_nm,
		 ISNULL(prd_cost,0) AS prd_cost,--handling missing values
		 CASE UPPER(TRIM(prd_line))
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'n/a'
		END
		AS prd_line,--map values
		CAST(prd_start_dt AS DATE) AS prd_start_dt,--data type casting
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt-- data enrichment (adding new data)
		FROM bronze.crm_prd_info
		--SELECT COUNT(*) FROM silver.crm_prd_info;
		SET @END_TIME=GETDATE();
		PRINT '>> LOAD DURATION :'+CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR)+' second';
		print'----------------------------';

		SET @START_TIME=GETDATE();
		PRINT '>> TRUNCATING TABLES: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details --IF DATA IS ALREADY THERE THEN DELETE IT

		PRINT '>> INSERTING DATA INTO: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		sls_order_dt ,
		sls_ship_dt  ,
		sls_due_dt ,
		sls_sales ,
		sls_quantity , 
		sls_price 
		)
		SELECT 
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		CASE 
			WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL 
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END  AS sls_order_dt, 
		CASE 
			WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL 
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END  AS sls_ship_dt,
		CASE 
			WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL 
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END  AS sls_due_dt,
		CASE 
			WHEN sls_sales<=0 OR sls_sales IS NULL OR sls_sales!=sls_quantity*ABS(sls_price) THEN  sls_quantity*ABS(sls_price)
			ELSE sls_sales
		END  AS sls_sales,
		sls_quantity , 
		CASE 
			WHEN sls_price<=0 OR sls_sales IS NULL  THEN  sls_sales/NULLIF(sls_quantity,0)
			ELSE sls_price
		END  AS sls_price
		FROM  bronze.crm_sales_details


		--SELECT COUNT(*) FROM silver.crm_sales_details;
		SET @END_TIME=GETDATE();
		PRINT '>> LOAD DURATION :'+CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR)+' second';
		print'----------------------------';

		PRINT'--------------';
		PRINT 'LOADING ERP TABLES';
		PRINT'--------------';

		SET @START_TIME=GETDATE();
		PRINT '>> TRUNCATING TABLES: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12 --IF DATA IS ALREADY THERE THEN DELETE IT

		PRINT '>> INSERTING DATA INTO: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
		)
		SELECT 
		CASE 
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid
		END AS cid,
		CASE 
			WHEN bdate>GETDATE() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE 
			WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('m','MALE') THEN 'male'
			ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_az12
		--SELECT COUNT(*) FROM silver.erp_cust_az12;
	    SET @END_TIME=GETDATE();
		PRINT '>> LOAD DURATION :'+CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR)+' second';
		print'----------------------------';


	    SET @START_TIME=GETDATE();
		PRINT '>> TRUNCATING TABLES: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101 --IF DATA IS ALREADY THERE THEN DELETE IT

		PRINT '>> INSERTING DATA INTO: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(
		cid,
		cntry
		)
		SELECT 
		REPLACE(cid,'-','')  cid,
		CASE 
			WHEN TRIM(cntry)='DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
			WHEN TRIM(cntry) ='' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101
		
		--SELECT COUNT(*) FROM silver.erp_loc_a101;
		SET @END_TIME=GETDATE();
		PRINT '>> LOAD DURATION :'+CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR)+' second';
		print'----------------------------';


		SET @START_TIME=GETDATE();
		PRINT '>> TRUNCATING TABLES: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2 --IF DATA IS ALREADY THERE THEN DELETE IT

		PRINT '>> INSERTING DATA INTO: silver.erp_px_cat_g1v2';
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

		--SELECT COUNT(*) FROM silver.erp_px_cat_g1v2 ;
		SET @END_TIME=GETDATE();
		SET @BATCH_END_TIME=GETDATE();
		PRINT '>> LOAD DURATION :'+CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR)+' second';
		print'----------------------------';
		PRINT 'TOTAL BATCH TIME:'+CAST(DATEDIFF(SECOND,@BATCH_START_TIME,@BATCH_END_TIME) AS NVARCHAR)+' second';

	END TRY
    BEGIN CATCH
		PRINT '=======================';
		PRINT'ERROR RECORD DURING LOADING BRONZE LAYER';
		PRINT'ERROR MESSAGE'+ERROR_MESSAGE();
		PRINT'ERROR MESSAGE'+CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'=====================';
    END CATCH
END

--EXEC silver.load_silver_layer;
