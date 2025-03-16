/*
 ===============================================================================
 DDL Script: Create Bronze Tables
 ===============================================================================
 Script Purpose:
     This script creates tables in the 'bronze' schema, dropping existing tables 
     if they already exist.
 	 Run this script to re-define the DDL structure of 'bronze' Tables
 ===============================================================================
 */

IF OBJECT_ID ('bronze.crm_sales_orders' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_orders;
GO

CREATE TABLE bronze.crm_sales_orders (
	sales_order_id CHAR(10),			-- Unique order identifier
	created_by CHAR(10),				-- ID of the user who created the order
	created_at DATE,				-- Order creation date in YYYYMMDD format
	changed_by CHAR(10),				-- ID of the user who modified the order
	changed_at DATE,				-- Last modification date in YYYYMMDD format
	fisc_variant NVARCHAR(10),			-- Fiscal variant code
	fiscal_year_period NVARCHAR(10),		-- Fiscal period in YYYYPPP format
	note_id NVARCHAR(10) NULL,			-- Notes field (completely empty in this case)
	partner_id CHAR(10),				
	sales_org NVARCHAR(10),				-- Sales organization
	currency CHAR(3),				-- ISO currency code
	gross_amount INT,					
	net_amount DECIMAL(12,3),			
	tax_amount DECIMAL(12,3),			
	lifecycle_status CHAR(1),			
	billing_status CHAR(1),				
	delivery_status CHAR(1)				
);
GO

IF OBJECT_ID ('bronze.erp_addresses' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_addresses;
GO

CREATE TABLE bronze.erp_addresses (
	address_id NVARCHAR(10),			-- Unique address identifier
	city NVARCHAR(50),				-- Name of the city
	postal_code NVARCHAR(10),			-- Postal code or ZIP code
	street NVARCHAR(50),				-- Street name
	building NVARCHAR(50) NULL,			-- Building number or name
	country VARCHAR(10),				-- Country code
	region VARCHAR(10),				-- Geographical region
	address_type INT,
	validity_start_date DATE,			-- Address validity start date in YYYYMMDD format
	validity_end_date DATE,				-- Address validity end date in YYYYMMDD format
	latitude DECIMAL(10,6),
    longitude DECIMAL(10,6)
);
GO

IF OBJECT_ID ('bronze.erp_business_partners' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_business_partners;
GO

CREATE TABLE bronze.erp_business_partners (
	partner_id NVARCHAR(10),			-- Unique business partner identifier
	partner_role INT,				-- Business partner role
	email_address NVARCHAR(100),
	phone_number NVARCHAR(20),
	fax_number NVARCHAR(20) NULL,			-- Business partner's fax number (completely empty in this case)
	web_address NVARCHAR(100),
	address_id INT,					-- Address ID associated with the partner (foreign key to 'erp_addresses')
	company_name NVARCHAR(100),
	legal_form NVARCHAR(10),
	created_by CHAR(10),				-- ID of the user who created the record
	created_at DATE,				-- Record creation date in YYYYMMDD format
	changed_by CHAR(10),				-- ID of the user who last modified the record
	changed_at DATE,				-- Last modification date in YYYYMMDD format
	currency CHAR(3)				-- Currency code associated with the partner
);
GO

IF OBJECT_ID ('bronze.erp_employees' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_employees;
GO

CREATE TABLE bronze.erp_employees (
	employee_id CHAR(10),				-- Unique employee identifier
	name_first NVARCHAR(20),
	name_middle CHAR(1) NULL,
	name_last NVARCHAR(20),
	name_initials NVARCHAR(3) NULL,			-- Initials of the name (completely empty in this case)
	sex CHAR(1),
	language NVARCHAR(3),				-- Employee's preferred language
	phone_number NVARCHAR(20),
	email_address NVARCHAR(100),
	login_name NVARCHAR(20),
	address_id INT,					-- Address ID associated with the employee (foreign key to 'erp_addresses')
	validity_start_date DATE,			-- Record validity start date in YYYYMMDD format
	validity_end_date NVARCHAR(20)			-- Record validity end date in YYYYMMDD format (problems saving as DATE)
);
GO

IF OBJECT_ID ('bronze.erp_product_categories' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_product_categories;
GO

CREATE TABLE bronze.erp_product_categories (
	prod_category_id CHAR(2),			-- Unique identifier for the product category 
	created_by CHAR(10),				-- ID of the user who created the category  
	created_at DATE					-- Category creation date in YYYYMMDD format
);
GO

IF OBJECT_ID ('bronze.erp_product_category_text' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_product_category_text;
GO

CREATE TABLE bronze.erp_product_category_text (
	prod_category_id CHAR(2),			-- Unique identifier for the product category 
	language CHAR(2),				-- Language code for the description
	short_descr NVARCHAR(50),			-- Short description of the product category
	MEDIUM_DESCR NVARCHAR(100) NULL, 		-- Medium-length description (completely empty in this case)
	LONG_DESCR NVARCHAR(200) NULL			-- Long description (completely empty in this case)
);
GO

IF OBJECT_ID ('bronze.erp_products' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_products;
GO

CREATE TABLE bronze.erp_products (
	product_id CHAR(7),				-- Unique identifier for the product
	type_code CHAR(2),
	prod_category_id CHAR(2),
	created_by CHAR(10),				-- ID of the user who created the product record
	created_at DATE,				-- Creation date of the product record in YYYYMMDD format
	changed_by CHAR(10),				-- ID of the user who last modified the product record
	changed_at DATE,				-- Last modification date of the product record in YYYYMMDD format
	supplier_partner_id CHAR(10),			-- ID of the supplier partner associated with the product
	tax_tariff_code INT,
	quantity_unit CHAR(2),
	weight_measure DECIMAL(5,1),			-- Weight of the product
	weight_unit CHAR(2),
	currency CHAR(3),
	price DECIMAL(7,2),
	width DECIMAL(5,1) NULL,			-- Width of the product (completely empty in this case)
    depth DECIMAL(5,1) NULL,				-- Depth of the product (completely empty in this case)
    height DECIMAL(5,1) NULL,				-- Height of the product (completely empty in this case)
	dimension_unit CHAR(2) NULL,			-- Unit of measure for dimensions (completely empty in this case)
    product_pic_url NVARCHAR(200) NULL			-- URL to the product picture, if available (completely empty in this case)
);
GO

IF OBJECT_ID ('bronze.erp_product_texts' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_product_texts;
GO

CREATE TABLE bronze.erp_product_texts (
	prod_category_id CHAR(7),			-- Unique identifier for the product category 
	language CHAR(2),				-- Language code for the description
	short_descr NVARCHAR(50),			-- Short description of the product category
	medium_descr NVARCHAR(100) NULL, 	
	long_descr NVARCHAR(200) NULL			-- Long description (completely empty in this case)
);
GO

IF OBJECT_ID ('bronze.erp_sales_order_items' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_sales_order_items;
GO

CREATE TABLE bronze.erp_sales_order_items (
	sales_order_id CHAR(10),			-- Unique identifier for the sales order
    sales_order_item CHAR(10),				-- Line item number within the sales order
	product_id CHAR(7),				-- Product identifier associated with the line item
	note_id NVARCHAR(10) NULL,			-- Note identifier for the line item (completely empty in this case)
	currency CHAR(3),				-- Currency code for pricing
	gross_amount DECIMAL(10,2),			-- Gross amount of the line item in the specified currency
	net_amount DECIMAL(10,3),
	tax_amount DECIMAL(10,3),
	item_atp_status CHAR(1),			-- ATP (Available to Promise) status of the line item
	op_item_pos NVARCHAR(10) NULL,			-- Operational position of the line item (completely empty in this case)
	quantity INT,					-- Quantity ordered for the product
	quantity_unit CHAR(2),
	delivery_date DATE
);
GO
