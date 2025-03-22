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
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.crm_sales_orders';
			TRUNCATE TABLE bronze.crm_sales_orders;
			PRINT '>> Inserting Data Into: bronze.crm_sales_orders';
			BULK INSERT bronze.crm_sales_orders
			FROM 'C:\sql\dwh_project\datasets\source_crm\SalesOrders.csv'
			WITH (
					CODEPAGE = '65001', --UTF-8 Encoding
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		
		SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.erp_addresses';
			TRUNCATE TABLE bronze.erp_addresses;
			PRINT '>> Inserting Data Into: bronze.erp_addresses';
			BULK INSERT bronze.erp_addresses
			FROM 'C:\sql\dwh_project\datasets\source_erp\Addresses.csv'
			WITH (
					CODEPAGE = '65001', --UTF-8 Encoding
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.erp_business_partners';
			TRUNCATE TABLE bronze.erp_business_partners;
			PRINT '>> Inserting Data Into: bronze.erp_business_partners';
			BULK INSERT bronze.erp_business_partners
			FROM 'C:\sql\dwh_project\datasets\source_erp\BusinessPartners.csv'
			WITH (
					CODEPAGE = '65001', --UTF-8 Encoding
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.erp_employees';
			TRUNCATE TABLE bronze.erp_employees;
			PRINT '>> Inserting Data Into: bronze.erp_employees';
			BULK INSERT bronze.erp_employees
			FROM 'C:\sql\dwh_project\datasets\source_erp\Employees.csv'
			WITH (
					CODEPAGE = '65001', --UTF-8 Encoding
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.erp_product_categories';
			TRUNCATE TABLE bronze.erp_product_categories;
			PRINT '>> Inserting Data Into: bronze.erp_product_categories';
			BULK INSERT bronze.erp_product_categories
			FROM 'C:\sql\dwh_project\datasets\source_erp\ProductCategories.csv'
			WITH (
					CODEPAGE = '65001', --UTF-8 Encoding
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.erp_product_category_text';
			TRUNCATE TABLE bronze.erp_product_category_text;
			PRINT '>> Inserting Data Into: bronze.erp_product_category_text';
			BULK INSERT bronze.erp_product_category_text
			FROM 'C:\sql\dwh_project\datasets\source_erp\ProductCategoryText.csv'
			WITH (
					CODEPAGE = '65001', --UTF-8 Encoding
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.erp_product_texts';
			TRUNCATE TABLE bronze.erp_product_texts;
			PRINT '>> Inserting Data Into: bronze.erp_product_texts';
			BULK INSERT bronze.erp_product_texts
			FROM 'C:\sql\dwh_project\datasets\source_erp\ProductTexts.csv'
			WITH (
					CODEPAGE = '65001', --UTF-8 Encoding
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.erp_products';
			TRUNCATE TABLE bronze.erp_products;
			PRINT '>> Inserting Data Into: bronze.erp_products';
			BULK INSERT bronze.erp_products
			FROM 'C:\sql\dwh_project\datasets\source_erp\Products.csv'
			WITH (
					CODEPAGE = '65001', --UTF-8 Encoding
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.erp_sales_order_items';
			TRUNCATE TABLE bronze.erp_sales_order_items;
			PRINT '>> Inserting Data Into: bronze.erp_sales_order_items';
			BULK INSERT bronze.erp_sales_order_items
			FROM 'C:\sql\dwh_project\datasets\source_erp\SalesOrderItems.csv'
			WITH (
					CODEPAGE = '65001', --UTF-8 Encoding
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT 'Error Severity: ' + CAST (ERROR_SEVERITY() AS NVARCHAR);
		PRINT 'Error Line: ' + CAST (ERROR_LINE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
