/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Loading crm_sales_orders
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_orders';
		TRUNCATE TABLE silver.crm_sales_orders;
		PRINT '>> Inserting Data Into: silver.crm_sales_orders';
		INSERT INTO silver.crm_sales_orders (
			sls_order_id,
			sls_order_created_by,
			sls_order_created_at,
			sls_order_changed_by,
			sls_order_changed_at,
			sls_order_fisc_variant,
			sls_order_fiscal_year_period,
			sls_order_partner_id,
			sls_order_org,
			sls_order_currency,
			sls_order_gross_amount,
			sls_order_net_amount,
			sls_order_tax_amount,
			sls_order_lifecycle_status,
			sls_order_billing_status,
			sls_order_delivery_status
		)
		SELECT
			sales_order_id,
			created_by,
			created_at,
			changed_by,
			changed_at,
			fisc_variant,
			FORMAT(EOMONTH(DATEFROMPARTS(LEFT(fiscal_year_period, 4),				--	Transform to YYYY-MM-DD Format
			                                 fiscal_year_period % 100, 
			                                 1)), 'yyyy-MM-dd') AS fiscal_year_period,
			partner_id,
			sales_org,
			currency,
			gross_amount,
			net_amount,
			tax_amount,
			CASE UPPER(TRIM(lifecycle_status)) 
			    WHEN 'C' THEN 'Completed'
			    WHEN 'I' THEN 'Incompleted'
			    WHEN 'X' THEN 'Cancelled'
			END AS lifecycle_status,
			CASE UPPER(TRIM(billing_status)) 
			    WHEN 'C' THEN 'Completed'
			    WHEN 'I' THEN 'Incompleted'
			    WHEN 'X' THEN 'Cancelled'
			END AS billing_status,
			CASE UPPER(TRIM(delivery_status)) 
			    WHEN 'C' THEN 'Completed'
			    WHEN 'I' THEN 'Incompleted'
			    WHEN 'X' THEN 'Cancelled'
			END AS delivery_status
		FROM bronze.crm_sales_orders
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

        -- Loading erp_addresses
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_addresses';
		TRUNCATE TABLE silver.erp_addresses;
		PRINT '>> Inserting Data Into: silver.erp_addresses';
		INSERT INTO silver.erp_addresses (
			addr_id,
			addr_city,
			addr_postal_code,
			addr_street,
			addr_building,
			addr_country,
			addr_region,
			addr_address_type,
			addr_validity_start_date,
			addr_latitude,
			addr_longitude
		)
		SELECT
			address_id,
			city,
			postal_code,
			TRIM(street) AS street,
			ISNULL(building, -1) AS building, -- Transform null values to -1
			country,
			region,
			address_type,
			validity_start_date,
			latitude,
			longitude
		FROM bronze.erp_addresses
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading erp_business_partners
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_business_partners';
		TRUNCATE TABLE silver.erp_business_partners;
		PRINT '>> Inserting Data Into: silver.erp_business_partners';
		INSERT INTO silver.erp_business_partners (
			ptnr_id,
			ptnr_role,
			ptnr_email_address,
			ptnr_phone_number,
			ptnr_web_address,
			ptnr_address_id,
			ptnr_company_name,
			ptnr_legal_form,
			ptnr_created_by,
			ptnr_created_at,
			ptnr_changed_by,
			ptnr_changed_at,
			ptnr_currency
		)
		SELECT
			partner_id,
			CASE 
			    WHEN partner_role = 1 THEN 'Supplier'
			    WHEN partner_role = 2 THEN 'Customer'
			END AS partner_role,
			email_address,
			phone_number,
			web_address,
			address_id,
			company_name,
			legal_form,
			created_by,
			created_at,
			changed_by,
			changed_at,
			currency
		FROM bronze.erp_business_partners


--silver.erp_employees
INSERT INTO silver.erp_employees(
emp_id,
emp_name_first,
emp_name_middle,
emp_name_last,
emp_sex,
emp_language,
emp_phone_number,
emp_email_address,
emp_login_name,
emp_address_id,
emp_validity_start_date
)
SELECT
employee_id,
REPLACE(name_first, '"', '') AS name_first,  -- Remove quotes from name_first
COALESCE(name_middle, 'N/A') AS name_middle, -- Replace nulls in name_middle with 'N/A'
name_last,
CASE 
    WHEN sex = 'M' THEN 'Male'
    WHEN sex = 'F' THEN 'Female'
END AS sex,

CASE 
    WHEN language = 'E' THEN 'English'
END AS language,
REPLACE(REPLACE(REPLACE(phone_number, ' ', ''), '-', ''), '.', '') AS phone_number, -- Normalize by removing spaces, hyphens, and periods
email_address,
login_name,
address_id,
validity_start_date

FROM (
	SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY validity_start_date DESC) as flag_last
	FROM bronze.erp_employees
)t WHERE flag_last = 1

--silver.erp_product_categories
INSERT INTO silver.erp_product_categories(
prod_category_id,
prod_cat_created_by,
prod_cat_created_at
)
SELECT
prod_category_id,
created_by,
created_at

FROM (
	SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY prod_category_id ORDER BY created_at DESC) as flag_last
	FROM bronze.erp_product_categories
)t WHERE flag_last = 1

--silver.erp_product_category_text
INSERT INTO silver.erp_product_category_text(
prod_category_id,
language,
short_descr
)
SELECT
prod_category_id,
CASE 
    WHEN language = 'EN' THEN 'English'
END AS language,
short_descr

FROM (
	SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY prod_category_id ORDER BY short_descr DESC) as flag_last
	FROM bronze.erp_product_category_text
)t WHERE flag_last = 1

--silver.erp_products
INSERT INTO silver.erp_products(
product_id,
prod_type_code,
prod_category_id,
prod_created_by,
prod_created_at,
prod_changed_by,
prod_changed_at,
prod_supplier_partner_id,
prod_tax_tariff_code,
prod_quantity_unit,
prod_weight_measure,
prod_weight_unit,
prod_currency,
prod_price
)
SELECT
product_id,
type_code,
prod_category_id,
created_by,
created_at,
changed_by,
changed_at,
supplier_partner_id,
tax_tariff_code,
CASE 
    WHEN quantity_unit = 'EA' THEN 'Each'
END AS quantity_unit,
weight_measure,
weight_unit,
currency,
price

FROM bronze.erp_products

--silver.erp_product_texts
INSERT INTO silver.erp_product_texts(
prod_category_id,
language,
short_descr,
medium_descr

)

-- Para los registros RC-1055 y RC-1056 (concatenar descripciones e idiomas)
SELECT 
    prod_category_id,
    STRING_AGG(
        CASE 
            WHEN language = 'EN' THEN 'English'
            WHEN language = 'DE' THEN 'German'
        END, 
        ' / '
    ) AS language,  -- Concatenar idiomas
    STRING_AGG(CONCAT(language, ' ', short_descr), ' / ') AS short_descr,
    STRING_AGG(CONCAT(language, ' ', TRIM(COALESCE(medium_descr, short_descr))), ' / ') AS medium_descr
FROM 
    bronze.erp_product_texts
WHERE
    prod_category_id IN ('RC-1055', 'RC-1056')
GROUP BY 
    prod_category_id

UNION ALL

-- Para el resto de los registros (sin concatenación)
SELECT
    prod_category_id,
    CASE 
        WHEN language = 'EN' THEN 'English'
        WHEN language = 'DE' THEN 'German'
    END AS language,
    short_descr,
    TRIM(COALESCE(medium_descr, short_descr)) AS medium_descr
FROM 
    bronze.erp_product_texts
WHERE
    prod_category_id NOT IN ('RC-1055', 'RC-1056')
ORDER BY 
    prod_category_id;



--silver.erp_sales_order_items
INSERT INTO silver.erp_sales_order_items(
sls_order_id,
sls_order_item,
sls_order_product_id,
sls_order_currency,
sls_order_gross_amount,
sls_order_net_amount,
sls_order_tax_amount,
sls_order_item_atp_status,
sls_order_quantity,
sls_order_quantity_unit,
sls_order_delivery_date
)
SELECT
sales_order_id,
sales_order_item,
product_id,
currency,
gross_amount,
net_amount,
tax_amount,
item_atp_status,
quantity,
CASE 
    WHEN quantity_unit = 'EA' THEN 'Each'
END AS quantity_unit,
delivery_date

FROM bronze.erp_sales_order_items
