/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_sales_orders'
-- ====================================================================
--Check For Nulls or Duplicates in Primary Key
--Expectation: No Result
SELECT
	sales_order_id,
	COUNT(*)
FROM bronze.crm_sales_orders
GROUP BY sales_order_id 
HAVING COUNT (*) > 1 OR sales_order_id IS NULL;

--Check for unwanted Spaces
--Expectation: No Result
SELECT
	fisc_variant
FROM bronze.crm_sales_orders
WHERE fisc_variant != TRIM(fisc_variant);

SELECT
	sales_org
FROM bronze.crm_sales_orders
WHERE sales_org != TRIM(sales_org);

SELECT
	currency
FROM bronze.crm_sales_orders
WHERE currency != TRIM(currency);

-- Data Standardization & Consistency
SELECT DISTINCT
	lifecycle_status
FROM bronze.crm_sales_orders;

SELECT DISTINCT
	billing_status
FROM bronze.crm_sales_orders;

SELECT DISTINCT
	delivery_status
FROM bronze.crm_sales_orders;

SELECT DISTINCT
	note_id
FROM bronze.crm_sales_orders;

SELECT
	fiscal_year_period,
       FORMAT(EOMONTH(DATEFROMPARTS(LEFT(fiscal_year_period, 4), 
                                    fiscal_year_period % 100, 
                                    1)), 'yyyy-MM-dd') 
	AS fiscal_year_period
FROM bronze.crm_sales_orders;

-- Check for NULLs or Negative Values in Amounts
-- Expectation: No Results
SELECT
	gross_amount
FROM bronze.crm_sales_orders
WHERE gross_amount <= 0 OR gross_amount IS NULL;

SELECT
	net_amount
FROM bronze.crm_sales_orders
WHERE net_amount <= 0 OR net_amount IS NULL;

SELECT
	tax_amount
FROM bronze.crm_sales_orders
WHERE tax_amount <= 0 OR tax_amount IS NULL;

-- Check for Invalid Date Orders (Created Date > Changed Date)
-- Expectation: No Results
SELECT 
    * 
FROM bronze.crm_sales_orders
WHERE changed_at < created_at;

-- ====================================================================
-- Checking 'silver.erp_addresses'
-- ====================================================================
--Check For Nulls or Duplicates in Primary Key
--Expectation: No Result
SELECT
	address_id,
	COUNT(*)
FROM bronze.erp_addresses
GROUP BY address_id 
HAVING COUNT (*) > 1 OR address_id IS NULL;

--Check for unwanted Spaces
--Expectation: No Result
SELECT
	city
FROM bronze.erp_addresses
WHERE city != TRIM(city);

SELECT
	postal_code
FROM bronze.erp_addresses
WHERE postal_code != TRIM(postal_code);

SELECT
	street
FROM bronze.erp_addresses
WHERE street != TRIM(street);

SELECT
	country
FROM bronze.erp_addresses
WHERE country != TRIM(country);

SELECT
	region
FROM bronze.erp_addresses
WHERE region != TRIM(region);

-- Check for NULLs or Negative Values in Building
-- Expectation: No Results
SELECT
	building
FROM bronze.erp_addresses
WHERE building <= 0 OR building IS NULL;

-- ====================================================================
-- Checking 'silver.erp_business_partners'
-- ====================================================================
--Check For Nulls or Duplicates in Primary Key
--Expectation: No Result
SELECT
	partner_id,
	COUNT(*)
FROM bronze.erp_business_partners
GROUP BY partner_id 
HAVING COUNT (*) > 1 OR partner_id IS NULL;

--Check for unwanted Spaces
--Expectation: No Result
SELECT
	email_address
FROM bronze.erp_business_partners
WHERE email_address != TRIM(email_address);

SELECT
	web_address
FROM bronze.erp_business_partners
WHERE web_address != TRIM(web_address);

SELECT
	company_name
FROM bronze.erp_business_partners
WHERE company_name != TRIM(company_name);

SELECT
	legal_form
FROM bronze.erp_business_partners
WHERE legal_form != TRIM(legal_form);

SELECT
	currency
FROM bronze.erp_business_partners
WHERE currency != TRIM(currency);

-- Data Standardization & Consistency
SELECT DISTINCT
	partner_role
FROM bronze.erp_business_partners;

SELECT DISTINCT
	email_address
FROM bronze.erp_business_partners;

SELECT DISTINCT
	web_address
FROM bronze.erp_business_partners;

SELECT DISTINCT
	company_name
FROM bronze.erp_business_partners;

SELECT DISTINCT
	legal_form
FROM bronze.erp_business_partners;

-- ====================================================================
-- Checking 'silver.erp_employees'
-- ====================================================================
--Check For Nulls or Duplicates in Primary Key
--Expectation: No Result
SELECT
	employee_id,
	COUNT(*)
FROM bronze.erp_employees
GROUP BY employee_id 
HAVING COUNT (*) > 1 OR employee_id IS NULL;

--Check for unwanted Spaces
--Expectation: No Result
SELECT
	name_first
FROM bronze.erp_employees
WHERE name_first != TRIM(name_first);

SELECT
	name_middle
FROM bronze.erp_employees
WHERE name_middle != TRIM(name_middle);

SELECT
	name_last
FROM bronze.erp_employees
WHERE name_last != TRIM(name_last);

SELECT
	email_address
FROM bronze.erp_employees
WHERE email_address != TRIM(email_address);

SELECT
	login_name
FROM bronze.erp_employees
WHERE login_name != TRIM(login_name);

-- Data Standardization & Consistency
SELECT DISTINCT
	sex
FROM bronze.erp_employees;

SELECT DISTINCT
	language
FROM bronze.erp_employees;

SELECT REPLACE(
				REPLACE(
						REPLACE(
								phone_number, ' ', ''
							   )
						, '-', '')
			  , '.', '') AS phone_number
FROM bronze.erp_employees;

-- ====================================================================
-- Checking 'silver.erp_product_categories'
-- ====================================================================
--Check For Nulls or Duplicates in Primary Key
--Expectation: No Result
SELECT
	prod_category_id,
	COUNT(*)
FROM bronze.erp_product_categories
GROUP BY prod_category_id 
HAVING COUNT (*) > 1 OR prod_category_id IS NULL;

-- ====================================================================
-- Checking 'silver.erp_product_category_text'
-- ====================================================================
--Check For Nulls or Duplicates in Primary Key
--Expectation: No Result
SELECT
	prod_category_id,
	COUNT(*)
FROM bronze.erp_product_category_text	
GROUP BY prod_category_id 
HAVING COUNT (*) > 1 OR prod_category_id IS NULL;

--Check for unwanted Spaces
--Expectation: No Result
SELECT
	language
FROM bronze.erp_product_category_text
WHERE language != TRIM(language);

SELECT
	short_descr
FROM bronze.erp_product_category_text
WHERE short_descr != TRIM(short_descr);

-- ====================================================================
-- Checking 'silver.erp_products'
-- ====================================================================
--Check For Nulls or Duplicates in Primary Key
--Expectation: No Result
SELECT
	product_id,
	COUNT(*)
FROM bronze.erp_products	
GROUP BY product_id 
HAVING COUNT (*) > 1 OR product_id IS NULL;

--Check for unwanted Spaces
--Expectation: No Result
SELECT
	product_id
FROM bronze.erp_products
WHERE product_id != TRIM(product_id);

SELECT
	type_code
FROM bronze.erp_products
WHERE type_code != TRIM(type_code);

SELECT
	prod_category_id
FROM bronze.erp_products
WHERE prod_category_id != TRIM(prod_category_id);

-- Data Standardization & Consistency
SELECT DISTINCT
	quantity_unit
FROM bronze.erp_products;

-- ====================================================================
-- Checking 'silver.erp_product_texts'
-- ====================================================================
--Check For Nulls or Duplicates in Primary Key
--Expectation: No Result
SELECT
	prod_category_id,
	COUNT(*)
FROM bronze.erp_product_texts	
GROUP BY prod_category_id
HAVING COUNT (*) > 1 OR prod_category_id IS NULL;

--Check for unwanted Spaces
--Expectation: No Result
SELECT
	prod_category_id
FROM bronze.erp_product_texts
WHERE prod_category_id != TRIM(prod_category_id);

SELECT
	language
FROM bronze.erp_product_texts
WHERE language != TRIM(language);

SELECT
	short_descr
FROM bronze.erp_product_texts
WHERE short_descr != TRIM(short_descr);

SELECT
	medium_descr
FROM bronze.erp_product_texts
WHERE medium_descr != TRIM(medium_descr);

-- Data Standardization & Consistency
SELECT
TRIM(COALESCE(medium_descr, short_descr)) AS medium_descr --impute values with short_descr and remove blanks
FROM bronze.erp_product_texts

-- ====================================================================
-- Checking 'silver.erp_sales_order_items'
-- ====================================================================
--Check For Nulls or Duplicates in Primary Key
--Expectation: No Result
SELECT
	sales_order_id,
	COUNT(*)
FROM bronze.erp_sales_order_items	
GROUP BY sales_order_id
HAVING COUNT (*) > 1 OR sales_order_id IS NULL;

--Check for unwanted Spaces
--Expectation: No Result
SELECT
	product_id
FROM bronze.erp_sales_order_items
WHERE product_id != TRIM(product_id);

SELECT
	currency
FROM bronze.erp_sales_order_items
WHERE currency != TRIM(currency);

SELECT
	item_atp_status
FROM bronze.erp_sales_order_items
WHERE item_atp_status != TRIM(item_atp_status);

SELECT
	quantity_unit
FROM bronze.erp_sales_order_items
WHERE quantity_unit != TRIM(quantity_unit);

-- Data Standardization & Consistency
SELECT DISTINCT
	note_id
FROM bronze.erp_sales_order_items;

SELECT DISTINCT
	op_item_pos
FROM bronze.erp_sales_order_items;

SELECT DISTINCT
	item_atp_status
FROM bronze.erp_sales_order_items;
