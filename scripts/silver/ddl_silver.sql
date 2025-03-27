/*
 ===============================================================================
 DDL Script: Create silver Tables
 ===============================================================================
 Script Purpose:
     This script creates tables in the 'silver' schema, dropping existing tables 
     if they already exist.
 	 Run this script to re-define the DDL structure of 'silver' Tables
 ===============================================================================
 */

IF OBJECT_ID ('silver.crm_sales_orders' , 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_orders;
GO

CREATE TABLE silver.crm_sales_orders (
	sls_order_id CHAR(10),					      	-- Unique order identifier
	sls_order_created_by CHAR(10),				      	-- ID of the user who created the order
	sls_order_created_at DATE,				      	-- Order creation date in YYYYMMDD format
	sls_order_changed_by CHAR(10),			              	-- ID of the user who modified the order
	sls_order_changed_at DATE,				      	-- Last modification date in YYYYMMDD format
	sls_order_fisc_variant NVARCHAR(10),			      	-- Fiscal variant code
	sls_order_fiscal_year_period NVARCHAR(10),			-- Fiscal period in YYYYPPP format
	--sls_order_note_id NVARCHAR(10) NULL,				-- Notes field (Dropped for being completely empty)
	sls_order_partner_id CHAR(10),				
	sls_order_org NVARCHAR(10),					-- Sales organization
	sls_order_currency CHAR(3),					-- ISO currency code
	sls_order_gross_amount INT,					
	sls_order_net_amount DECIMAL(12,3),			
	sls_order_tax_amount DECIMAL(12,3),			
	sls_order_lifecycle_status NVARCHAR(15),			
	sls_order_billing_status NVARCHAR(15),				
	sls_order_delivery_status NVARCHAR(15),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_addresses' , 'U') IS NOT NULL
	DROP TABLE silver.erp_addresses;
GO

CREATE TABLE silver.erp_addresses (
	addr_id NVARCHAR(10),						-- Unique address identifier
	addr_city NVARCHAR(50),						-- Name of the city
	addr_postal_code NVARCHAR(10),					-- Postal code or ZIP code
	addr_street NVARCHAR(50),					-- Street name
	addr_building NVARCHAR(50) NULL,				-- Building number or name
	addr_country VARCHAR(10),					-- Country code
	addr_region VARCHAR(10),					-- Geographical region
	addr_address_type INT,
	addr_validity_start_date DATE,					-- Address validity start date in YYYYMMDD format
	--addr_validity_end_date DATE,					-- Address validity end date (doesn´t provide relevant information)
	addr_latitude DECIMAL(10,6),
    	addr_longitude DECIMAL(10,6),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_business_partners' , 'U') IS NOT NULL
	DROP TABLE silver.erp_business_partners;
GO

CREATE TABLE silver.erp_business_partners (
	ptnr_id NVARCHAR(10),						-- Unique business partner identifier
	ptnr_role NVARCHAR(10),						-- Business partner role
	ptnr_email_address NVARCHAR(100),
	ptnr_phone_number NVARCHAR(20),
	--ptnr_fax_number NVARCHAR(20) NULL,				-- Business partner's fax number (completely empty)
	ptnr_web_address NVARCHAR(100),
	ptnr_address_id NVARCHAR(10),					-- Address ID associated with the partner (foreign key to 'erp_addresses')
	ptnr_company_name NVARCHAR(100),
	ptnr_legal_form NVARCHAR(10),
	ptnr_created_by CHAR(10),					-- ID of the user who created the record
	ptnr_created_at DATE,						-- Record creation date in YYYYMMDD format
	ptnr_changed_by CHAR(10),					-- ID of the user who last modified the record
	ptnr_changed_at DATE,						-- Last modification date in YYYYMMDD format
	ptnr_currency CHAR(3),						-- Currency code associated with the partner
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_employees' , 'U') IS NOT NULL
	DROP TABLE silver.erp_employees;
GO

CREATE TABLE silver.erp_employees (
	emp_id CHAR(10),						-- Unique employee identifier
	emp_name_first NVARCHAR(20),
	emp_name_middle NVARCHAR(10) NULL,
	emp_name_last NVARCHAR(20),
	--emp_name_initials NVARCHAR(3) NULL,				-- Initials of the name (completely empty)
	emp_sex NVARCHAR(10),
	emp_language NVARCHAR(10),					-- Employee's preferred language
	emp_phone_number NVARCHAR(20),
	emp_email_address NVARCHAR(100),
	emp_login_name NVARCHAR(20),
	emp_address_id INT,						-- Address ID associated with the employee (foreign key to 'erp_addresses')
	emp_validity_start_date DATE,					-- Record validity start date in YYYYMMDD format
	--emp_validity_end_date NVARCHAR(20),				-- Record validity end date in YYYYMMDD format (doesn´t provide relevant information)
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_product_categories' , 'U') IS NOT NULL
	DROP TABLE silver.erp_product_categories;
GO

CREATE TABLE silver.erp_product_categories (
	prod_category_id CHAR(2),					-- Unique identifier for the product category 
	prod_cat_created_by CHAR(10),					-- ID of the user who created the category  
	prod_cat_created_at DATE,					-- Category creation date in YYYYMMDD format
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_product_category_text' , 'U') IS NOT NULL
	DROP TABLE silver.erp_product_category_text;
GO

CREATE TABLE silver.erp_product_category_text (
	prod_category_id CHAR(2),					-- Unique identifier for the product category 
	language NVARCHAR(10),						-- Language code for the description
	short_descr NVARCHAR(50),					-- Short description of the product category
	--MEDIUM_DESCR NVARCHAR(100) NULL, 				-- Medium-length description (completely empty)
	--LONG_DESCR NVARCHAR(200) NULL,				-- Long description (completely empty)
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_products' , 'U') IS NOT NULL
	DROP TABLE silver.erp_products;
GO

CREATE TABLE silver.erp_products (
	product_id CHAR(7),						-- Unique identifier for the product
	prod_type_code CHAR(2),
	prod_category_id CHAR(2),
	prod_created_by CHAR(10),					-- ID of the user who created the product record
	prod_created_at DATE,						-- Creation date of the product record in YYYYMMDD format
	prod_changed_by CHAR(10),					-- ID of the user who last modified the product record
	prod_changed_at DATE,						-- Last modification date of the product record in YYYYMMDD format
	prod_supplier_partner_id CHAR(10),				-- ID of the supplier partner associated with the product
	prod_tax_tariff_code INT,
	prod_quantity_unit NVARCHAR(10),
	prod_weight_measure DECIMAL(5,1),				-- Weight of the product
	prod_weight_unit CHAR(2),
	prod_currency CHAR(3),
	prod_price DECIMAL(7,2),
	--prod_width DECIMAL(5,1) NULL,					-- Width of the product (completely empty)
    	--prod_depth DECIMAL(5,1) NULL,					-- Depth of the product (completely empty)
    	--prod_height DECIMAL(5,1) NULL,				-- Height of the product (completely empty)
	--prod_dimension_unit CHAR(2) NULL,				-- Unit of measure for dimensions (completely empty)
    	--product_pic_url NVARCHAR(200) NULL,				-- URL to the product picture, if available (completely empty)
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_product_texts' , 'U') IS NOT NULL
	DROP TABLE silver.erp_product_texts;
GO

CREATE TABLE silver.erp_product_texts (
	prod_category_id NVARCHAR(15),					-- Unique identifier for the product category 
	language NVARCHAR(20),						-- Language code for the description
	short_descr NVARCHAR(50),					-- Short description of the product category
	medium_descr NVARCHAR(100) NULL, 	
	--long_descr NVARCHAR(200) NULL,				-- Long description (completely empty)
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_sales_order_items' , 'U') IS NOT NULL
	DROP TABLE silver.erp_sales_order_items;
GO

CREATE TABLE silver.erp_sales_order_items (
	sls_order_id CHAR(10),						-- Unique identifier for the sales order
    	sls_order_item CHAR(10),					-- Line item number within the sales order
	sls_order_product_id CHAR(7),					-- Product identifier associated with the line item
	--sls_order_note_id NVARCHAR(10) NULL,				-- Note identifier for the line item (completely empty)
	sls_order_currency CHAR(3),					-- Currency code for pricing
	sls_order_gross_amount DECIMAL(10,2),				-- Gross amount of the line item in the specified currency
	sls_order_net_amount DECIMAL(10,3),
	sls_order_tax_amount DECIMAL(10,3),
	sls_order_item_atp_status CHAR(1),				-- ATP (Available to Promise) status of the line item
	--sls_order_op_item_pos NVARCHAR(10) NULL,			-- Operational position of the line item (completely empty)
	sls_order_quantity INT,						-- Quantity ordered for the product
	sls_order_quantity_unit NVARCHAR(10),
	sls_order_delivery_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
