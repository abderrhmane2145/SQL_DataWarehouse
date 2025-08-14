/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
USE [DataWarehouse];
GO
CREATE OR ALTER PROCEDURE bronez.load_bronez AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @whole_start_time DATETIME, @whole_end_time DATETIME;
	
	BEGIN TRY
	    SET @whole_start_time = GETDATE();
		PRINT '===========================================';
		PRINT 'Loading Bronez Layer';
		PRINT '===========================================';


		PRINT '-------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------';
		PRINT '>> Truncating Table: bronez.crm_cust_info';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronez.crm_cust_info;
		PRINT '>> Inserting Into: bronez.crm_cust_info';
		BULK INSERT bronez.crm_cust_info
		FROM 'C:\Users\LENOVO\Desktop\Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH
		(
		  FIRSTROW = 2,
		  FIELDTERMINATOR = ',',
		  TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
		PRINT '>> ------------- ------------- ------------- ------------- '

		PRINT '>> Truncating Table: bronez.crm_prd_info';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronez.crm_prd_info;
		PRINT '>> Inserting Into: bronez.crm_prd_info';
		BULK INSERT bronez.crm_prd_info
		FROM 'C:\Users\LENOVO\Desktop\Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH
		(
		  FIRSTROW = 2,
		  FIELDTERMINATOR = ',',
		  TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
		PRINT '>> ------------- ------------- ------------- ------------- '

		PRINT '>> Truncating Table: bronez.crm_sales_details';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronez.crm_sales_details;
		PRINT '>> Inserting Into: bronez.crm_sales_details';
		BULK INSERT bronez.crm_sales_details
		FROM 'C:\Users\LENOVO\Desktop\Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH
		(
		  FIRSTROW = 2,
		  FIELDTERMINATOR = ',',
		  TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
		PRINT '>> ------------- ------------- ------------- ------------- '

		PRINT '-------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------';
		PRINT '>> Truncating Table: bronez.epr_cust_az12';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronez.epr_cust_az12;
		PRINT '>> Inserting Into: bronez.epr_cust_az12';
		BULK INSERT bronez.epr_cust_az12
		FROM 'C:\Users\LENOVO\Desktop\Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH
		(
		  FIRSTROW = 2,
		  FIELDTERMINATOR = ',',
		  TABLOCK
		);
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
		PRINT '>> ------------- ------------- ------------- ------------- '

		PRINT '>> Truncating Table: bronez.epr_loc_a101';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronez.epr_loc_a101;
		PRINT '>> Inserting Into: bronez.epr_loc_a101';
		BULK INSERT bronez.epr_loc_a101
		FROM 'C:\Users\LENOVO\Desktop\Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH
		(
		  FIRSTROW = 2,
		  FIELDTERMINATOR = ',',
		  TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
		PRINT '>> ------------- ------------- ------------- ------------- '

		PRINT '>> Truncating Table: bronez.epr_px_cat_g1v2';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronez.epr_px_cat_g1v2;
		PRINT '>> Inserting Into: bronez.epr_px_cat_g1v2';
		BULK INSERT bronez.epr_px_cat_g1v2
		FROM 'C:\Users\LENOVO\Desktop\Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH
		(
		  FIRSTROW = 2,
		  FIELDTERMINATOR = ',',
		  TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
		PRINT '>> ------------- ------------- ------------- ------------- '
		SET @whole_end_time = GETDATE();
		PRINT 'Load Duration Completed'
		PRINT 'Total Duration Load ' + CAST(DATEDIFF(second, @whole_start_time, @whole_end_time) AS NVARCHAR) + ' seconds'
	END TRY
	BEGIN CATCH
		PRINT '========================================================'
		PRINT 'Error Occured.'
		PRINT 'ERROR MESSAGE : ' + ERROR_MESSAGE()
		PRINT 'ERROR NUMBER  : ' + CAST(ERROR_NUMBER() AS NVARCHAR)
		PRINT 'ERROR LINE    : ' + CAST(ERROR_LINE() AS NVARCHAR)
		PRINT 'PRCEDURE NAME : ' + ERROR_PROCEDURE()
		PRINT '========================================================'
	END CATCH

END



