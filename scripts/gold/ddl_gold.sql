/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY product_id) AS product_key, -- Surrogate key
    -- 1. Primary key
    pi.product_id,

    -- 2. Main product attributes
    --pi.prod_type_code		    AS product_type_code, (Always PR)
    pi.prod_category_id			AS product_category_id,
    pi.prod_supplier_partner_id 	AS product_supplier_partner_id,
    pi.prod_tax_tariff_code		AS product_tax_tariff_code,
    --pi.prod_quantity_unit		    AS product_quantity_unit, (Always Each)
    pi.prod_weight_measure		AS product_weight_kg,
    --pi.prod_weight_unit		    AS product_weight_unit, (Always Kg)
    --pi.prod_currency		    AS product_currency, (Always USD)
    pi.prod_price			AS product_price_$,

    -- 3. Category and related attributes
    pc.prod_cat_created_by		AS product_category_created_by,
    pc.prod_cat_created_at		AS product_category_created_at,
    --pct.language			    AS category_language, (Always English and same as product_language)
    pct.short_descr			AS category_short_description,

    -- 4. Creation and modification metadata
    pi.prod_created_by			AS product_created_by,
    pi.prod_created_at			AS product_created_at,
    pi.prod_changed_by			AS product_changed_by,
    pi.prod_changed_at			AS product_changed_at,

    -- 5. Additional product details
    pt.language				AS product_language,
    pt.short_descr			AS product_short_description,
    pt.medium_descr			AS product_medium_description,

    -- 6. Data Warehouse Metadata
    pi.dwh_create_date
	
FROM silver.erp_products AS pi
LEFT JOIN silver.erp_product_categories pc
    ON pi.prod_category_id = pc.prod_category_id
LEFT JOIN silver.erp_product_category_text pct
    ON pi.prod_category_id = pct.prod_category_id
LEFT JOIN silver.erp_product_texts pt
    ON pi.product_id = pt.prod_category_id
GO

-- =============================================================================
-- Create Dimension: gold.dim_business_partners
-- =============================================================================
IF OBJECT_ID('gold.dim_business_partners', 'V') IS NOT NULL
    DROP VIEW gold.dim_business_partners;
GO

CREATE VIEW gold.dim_business_partners AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ptnr_id) AS business_partner_key, -- Surrogate key
    -- 1. Primary key
    bp.ptnr_id				AS partner_id,
	
    -- 2. Partner Role
    bp.ptnr_role			AS partner_role,

    -- 3. Company Information
    bp.ptnr_company_name		AS company_name,
    bp.ptnr_legal_form			AS company_legal_form,

    -- 4. Contact Information
    bp.ptnr_email_address		AS email_address,
    bp.ptnr_phone_number		AS phone_number,
    bp.ptnr_web_address			AS web_address,

    -- 5. Address Information
    bp.ptnr_address_id			AS partner_address_id,
    ad.addr_id				AS address_id,
    ad.addr_address_type		AS address_type,
    ad.addr_building			AS building,
    ad.addr_street			AS street,
    ad.addr_city			AS city,
    ad.addr_region			AS region,
    ad.addr_postal_code			AS postal_code,
    ad.addr_country			AS country,
    ad.addr_latitude			AS latitude,
    ad.addr_longitude			AS longitude,
    ad.addr_validity_start_date 	AS address_validity_start_date,

    -- 6. Financial Information
    bp.ptnr_currency			AS currency,

    -- 7. Metadata: Creation & Modification
    bp.ptnr_created_by			AS partner_created_by,
    bp.ptnr_created_at			AS partner_created_at,
    bp.ptnr_changed_by			AS partner_changed_by,
    bp.ptnr_changed_at			AS partner_changed_at,
    
    -- 8. Data Warehouse Metadata
    bp.dwh_create_date
	
FROM silver.erp_business_partners AS bp
FULL OUTER JOIN silver.erp_addresses ad
    ON bp.ptnr_address_id = ad.addr_id

-- =============================================================================
-- Create Dimension: gold.dim_employees
-- =============================================================================
IF OBJECT_ID('gold.dim_employees', 'V') IS NOT NULL
    DROP VIEW gold.dim_employees;
GO

CREATE VIEW gold.dim_employees AS
SELECT
    -- 1. Primary key
    emp_id				AS employee_id,

    -- 2. Employee Personal Information
    emp_name_first			AS first_name,
    emp_name_middle			AS middle_name,
    emp_name_last			AS last_name,
    emp_sex					AS sex,
    emp_language			AS language,

	-- 3. Contact Information
    emp_phone_number		AS phone_number,
    emp_email_address		AS email_address,
    emp_login_name			AS login_name,

	-- 4. Address id and Employment start date
    emp_address_id			AS address_id,
    emp_validity_start_date	AS employee_validity_start_date,

	-- 5. Data Warehouse Metadata
    dwh_create_date
FROM silver.erp_employees

GO
