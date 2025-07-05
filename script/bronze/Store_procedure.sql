/*
created store procedure  in this i am Loading data to tables using BULK INSERT (from a csv file  SOURCE->BRONZE)
calculating and showing time taken to load a file to table,total batch time
this storeprocedure doesnt accept any parameter or return any value

After creating storeprocedure execute it by:
  EXEC bronze.load_bronze_layer;
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze_layer As
BEGIN
	DECLARE @START_TIME DATETIME,@END_TIME DATETIME,@BATCH_START_TIME DATETIME,@BATCH_END_TIME DATETIME;
	BEGIN TRY
		PRINT'=============';
		PRINT 'LOADING BRONZE LAYER';
		PRINT'=============';

		PRINT'--------------';
		PRINT 'LOADING CRM TABLES';
		PRINT'--------------';
		SET @START_TIME=GETDATE();
		SET @BATCH_START_TIME=GETDATE();
		PRINT '>> TRUNCATING TABLES: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info --IF DATA IS ALREADY THERE THEN DELETE IT

		PRINT '>> INSERTING DATA INTO: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\ADMIN\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
		   FIRSTROW = 2, --SAYS FIRST ROW IS FROM 2ND ROW
		   FIELDTERMINATOR =',',-- TELLS THE DELIMITER
		   TABLOCK --TELLS TO LOCK THE TABLE DURING LOADING IT INC PERFORMANCE
		);
		--SELECT COUNT(*) FROM bronze.crm_cust_info;
		SET @END_TIME=GETDATE();
		PRINT '>> LOAD DURATION :'+CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR)+' second';
		print'----------------------------';

		SET @START_TIME=GETDATE();
		PRINT '>> TRUNCATING TABLES: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info --IF DATA IS ALREADY THERE THEN DELETE IT

		PRINT '>> INSERTING DATA INTO: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\ADMIN\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
		   FIRSTROW = 2, --SAYS FIRST ROW IS FROM 2ND ROW
		   FIELDTERMINATOR =',',-- TELLS THE DELIMITER
		   TABLOCK --TELLS TO LOCK THE TABLE DURING LOADING IT INC PERFORMANCE
		);
		--SELECT COUNT(*) FROM bronze.crm_prd_info;
		SET @END_TIME=GETDATE();
		PRINT '>> LOAD DURATION :'+CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR)+' second';
		print'----------------------------';

		SET @START_TIME=GETDATE();
		PRINT '>> TRUNCATING TABLES: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details --IF DATA IS ALREADY THERE THEN DELETE IT

		PRINT '>> INSERTING DATA INTO: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\ADMIN\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
		   FIRSTROW = 2, --SAYS FIRST ROW IS FROM 2ND ROW
		   FIELDTERMINATOR =',',-- TELLS THE DELIMITER
		   TABLOCK --TELLS TO LOCK THE TABLE DURING LOADING IT INC PERFORMANCE
		);
		--SELECT COUNT(*) FROM bronze.crm_sales_details;
		SET @END_TIME=GETDATE();
		PRINT '>> LOAD DURATION :'+CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR)+' second';
		print'----------------------------';

		PRINT'--------------';
		PRINT 'LOADING ERP TABLES';
		PRINT'--------------';

		SET @START_TIME=GETDATE();
		PRINT '>> TRUNCATING TABLES: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12 --IF DATA IS ALREADY THERE THEN DELETE IT

		PRINT '>> INSERTING DATA INTO: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\ADMIN\Desktop\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
		   FIRSTROW = 2, --SAYS FIRST ROW IS FROM 2ND ROW
		   FIELDTERMINATOR =',',-- TELLS THE DELIMITER
		   TABLOCK --TELLS TO LOCK THE TABLE DURING LOADING IT INC PERFORMANCE
		);
		--SELECT COUNT(*) FROM bronze.erp_cust_az12;
	    SET @END_TIME=GETDATE();
		PRINT '>> LOAD DURATION :'+CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR)+' second';
		print'----------------------------';


	    SET @START_TIME=GETDATE();
		PRINT '>> TRUNCATING TABLES: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101 --IF DATA IS ALREADY THERE THEN DELETE IT

		PRINT '>> INSERTING DATA INTO: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\ADMIN\Desktop\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
		   FIRSTROW = 2, --SAYS FIRST ROW IS FROM 2ND ROW
		   FIELDTERMINATOR =',',-- TELLS THE DELIMITER
		   TABLOCK --TELLS TO LOCK THE TABLE DURING LOADING IT INC PERFORMANCE
		);
		--SELECT COUNT(*) FROM bronze.erp_loc_a101;
		SET @END_TIME=GETDATE();
		PRINT '>> LOAD DURATION :'+CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR)+' second';
		print'----------------------------';


		SET @START_TIME=GETDATE();
		PRINT '>> TRUNCATING TABLES: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2 --IF DATA IS ALREADY THERE THEN DELETE IT

		PRINT '>> INSERTING DATA INTO: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2 
		FROM 'C:\Users\ADMIN\Desktop\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
		   FIRSTROW = 2, --SAYS FIRST ROW IS FROM 2ND ROW
		   FIELDTERMINATOR =',',-- TELLS THE DELIMITER
		   TABLOCK --TELLS TO LOCK THE TABLE DURING LOADING IT INC PERFORMANCE
		);
		--SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2 ;
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
END;
