/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema from schema 'bronez,
    By Using The Process ETL (Extract, Transform ,Load). 

    It performs the following actions:
	    - Truncates the silver tables.
	    - Insert Transformed and Cleaned Data into silver tables from bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


CREATE OR ALTER PROCEDURE  silver.load_silver AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @whole_start_time DATETIME, @whole_end_time DATETIME
BEGIN TRY
    SET @whole_start_time = GETDATE()
    
    PRINT '==========================================='
    PRINT 'Loading Silver Layer'
    PRINT '==========================================='

    PRINT '-------------------------------------------'
    PRINT 'Loading CRM Tables. '
    PRINT '-------------------------------------------'
    PRINT '>> Truncating Table: silver.crm_cust_info'
    SET @start_time = GETDATE()
    TRUNCATE TABLE  silver.crm_cust_info
    PRINT '>> Inserting Into Table: silver.crm_cust_info'
    INSERT INTO silver.crm_cust_info
        (cst_id,
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
                CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                     WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                     ELSE 'N/A'
                END AS cst_marital_status, -- Standardization
                CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                     ELSE 'N/A'
                END AS cst_gndr,  -- Standardization
                cst_create_date
        FROM
        (
        SELECT 
              *, 
              ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) AS Ordering_Date 
        FROM bronez.crm_cust_info
        )t WHERE Ordering_Date = 1 -- Retrieve most recent data ( Remove Duplicates )
            AND cst_id IS NOT NULL -- Remove Null Values
        ;
        SET @end_time = GETDATE()
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time)  AS NVARCHAR) + ' seconds'
        PRINT'>> ------------- ------------- ------------- -------------'



    PRINT '>> Truncating Table: silver.crm_prd_info'
    SET @start_time = GETDATE()
    TRUNCATE TABLE  silver.crm_prd_info

    PRINT '>> Inserting Into Table: silver.crm_prd_info'
    INSERT INTO silver.crm_prd_info 
           (prd_id,       
            prd_key,     
            cat_id,      
            sls_prd_key, 
            prd_nm,      
            prd_cost,    
            prd_line,    
            prd_start_dt,  
            prd_end_dt  
            )

        SELECT 
           prd_id,
           prd_key,
           REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')  AS cat_id,
           SUBSTRING(prd_key, 7, LEN(prd_key)) AS sls_prd_key,
           prd_nm,
           ISNULL(prd_cost, 0) AS prd_cost,
           CASE UPPER(TRIM(prd_line))  
                WHEN 'R' THEN 'Road'
                WHEN 'M' THEN 'Mountain'
                WHEN 'S' THEN 'Others Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'N/A'
           END AS prd_line,
           CAST(prd_start_dt AS DATE) AS prd_start_dt,
           CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
        FROM bronez.crm_prd_info;
        SET @end_time = GETDATE()
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time)  AS NVARCHAR) + ' seconds'
        PRINT'>> ------------- ------------- ------------- -------------'

    PRINT '>> Truncating Table: silver.crm_sales_details'
    SET @start_time = GETDATE()
    TRUNCATE TABLE  silver.crm_sales_details
    PRINT '>> Inserting Into Table: silver.crm_sales_details'
    INSERT INTO silver.crm_sales_details
        (
        sls_ord_num  ,
        sls_prd_key  ,
        sls_cust_id  ,
        sls_order_dt ,
        sls_ship_dt  ,
        sls_due_dt   ,
        sls_sales    ,
        sls_quantity ,
        sls_price    
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN LEN(sls_order_dt) != 8 OR sls_order_dt = 0 THEN NULL
                 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE WHEN LEN(sls_ship_dt) != 8 OR sls_ship_dt = 0 THEN NULL
                 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE WHEN LEN(sls_due_dt) != 8 OR sls_due_dt = 0 THEN NULL
                 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != (sls_quantity *ABS(sls_price)) THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE WHEN  sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
            END AS sls_price
        FROM 
        bronez.crm_sales_details;
        SET @end_time = GETDATE()
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time)  AS NVARCHAR) + ' seconds'
        PRINT'>> ------------- ------------- ------------- -------------'
        PRINT '-------------------------------------------'
        PRINT 'Loading ERP Tables. '
        PRINT '-------------------------------------------'



    PRINT '>> Truncating Table: silver.epr_cust_az12'
    SET @start_time = GETDATE()
    TRUNCATE TABLE  silver.epr_cust_az12
    PRINT '>> Inserting Into Table: silver.epr_cust_az12'
    INSERT INTO silver.epr_cust_az12 (cid, bdate, gen)
        SELECT
        CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
             ELSE CID
        END AS CID,
        CASE WHEN BDATE > GETDATE() THEN NULL
             ELSE BDATE
        END AS BDATE,
        CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
             WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE')   THEN 'Male'
             ELSE 'N/A'
        END AS GEN
        FROM bronez.epr_cust_az12;
        SET @end_time = GETDATE()
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time)  AS NVARCHAR) + ' seconds'
        PRINT'>> ------------- ------------- ------------- -------------'


    PRINT '>> Truncating Table: silver.epr_loc_a101'
    SET @start_time = GETDATE()
    TRUNCATE TABLE  silver.epr_loc_a101
    PRINT '>> Inserting Into Table: silver.epr_loc_a101'
    INSERT INTO silver.epr_loc_a101 (cid, cntry)
        SELECT 
        REPLACE(CID, '-', '') AS cid,
        CASE WHEN CNTRY = '' OR CNTRY IS NULL THEN 'N/A'
             WHEN UPPER(TRIM(CNTRY)) IN ('US', 'USA') THEN 'United States'
             WHEN UPPER(TRIM(CNTRY)) = 'DE' THEN 'Germany'
             ELSE TRIM(CNTRY)
        END AS cntry
        FROM bronez.epr_loc_a101;
    SET @end_time = GETDATE()
    PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time)  AS NVARCHAR) + ' seconds'
    PRINT'>> ------------- ------------- ------------- -------------'


    PRINT '>> Truncating Table: silver.epr_px_cat_g1v2'
    SET @start_time = GETDATE()
    TRUNCATE TABLE  silver.epr_px_cat_g1v2
    PRINT '>> Inserting Into Table: silver.epr_px_cat_g1v2'
    INSERT INTO silver.epr_px_cat_g1v2 
        (id, cat, subcat, maintenance)
	    SELECT 
	    *
	    FROM bronez.epr_px_cat_g1v2;
        SET @end_time = GETDATE()
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time)  AS NVARCHAR) + ' seconds'
        PRINT'>> ------------- ------------- ------------- -------------'
        SET @whole_end_time = GETDATE()
        
        PRINT 'Load Duration Completed'
        PRINT 'Total Duration Load : ' + CAST(DATEDIFF(SECOND, @whole_start_time, @whole_end_time)AS NVARCHAR) +' seconds'
        
        
        
END TRY
BEGIN CATCH
    PRINT '========================================================'
    PRINT 'Error Occured.'
    PRINT 'Error Message : ' + ERROR_MESSAGE() 
    PRINT 'Error Number : '+ CAST(ERROR_NUMBER() AS NVARCHAR)
    PRINT 'Error Line : '+ CAST(ERROR_LINE() AS NVARCHAR)
    PRINT 'Error Procedure :'+ ERROR_PROCEDURE()
    PRINT '========================================================'
END CATCH
END




