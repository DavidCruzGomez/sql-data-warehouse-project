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
    pi.prod_category_id			AS category_id,
    pi.prod_supplier_partner_id 	AS supplier_id,
    pi.prod_tax_tariff_code		AS tax_code,
    --pi.prod_quantity_unit		    AS product_quantity_unit, (Always Each)
    pi.prod_weight_measure		AS weight_kg,
    --pi.prod_weight_unit		    AS product_weight_unit, (Always Kg)
    --pi.prod_currency		    AS product_currency, (Always USD)
    pi.prod_price			AS price_usd,

    -- 3. Category and related attributes
    pc.prod_cat_created_by		AS category_created_by,
    pc.prod_cat_created_at		AS category_created_at,
    --pct.language			    AS category_language, (Always English and same as product_language)
    pct.short_descr			AS category_short_description,

    -- 4. Creation and modification metadata
    pi.prod_created_by			AS created_by,
    pi.prod_created_at			AS created_at,
    pi.prod_changed_by			AS changed_by,
    pi.prod_changed_at			AS changed_at,

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
    COALESCE(bp.ptnr_id, 'N/A')			AS partner_id,
	
    -- 2. Partner Role
    COALESCE(bp.ptnr_role, 'N/A')		AS partner_role,

    -- 3. Company Information
    COALESCE(bp.ptnr_company_name, 'N/A')	AS company_name,
    COALESCE(bp.ptnr_legal_form, 'N/A')		AS legal_form,

    -- 4. Contact Information
    COALESCE(bp.ptnr_email_address, 'N/A')	AS email,
    COALESCE(bp.ptnr_phone_number, 'N/A')	AS phone,
    COALESCE(bp.ptnr_web_address, 'N/A')	AS website,

    -- 5. Address Information
    --bp.ptnr_address_id		    	    AS partner_address_id,(same as ad.addr_id)
    COALESCE(bp.ptnr_address_id, 'N/A')		AS address_id,
    ad.addr_address_type			AS address_type,
    ad.addr_building				AS building,
    ad.addr_street				AS street,
    ad.addr_city				AS city,
    ad.addr_region				AS region,
    ad.addr_postal_code				AS postal_code,
    ad.addr_country				AS country,
    ad.addr_latitude				AS latitude,
    ad.addr_longitude				AS longitude,
    --ad.addr_validity_start_date 	            AS address_validity_start_date,(not useful for gold layer)

    -- 6. Financial Information
    CASE ad.addr_country
        WHEN 'US' THEN 'USD'
        WHEN 'CA' THEN 'CAD'
        WHEN 'AU' THEN 'AUD'
        WHEN 'DE' THEN 'EUR'
        WHEN 'FR' THEN 'EUR'
        WHEN 'GB' THEN 'GBP'
        WHEN 'IN' THEN 'INR'
        WHEN 'DU' THEN 'AED'  
        ELSE 'N/A'
    END AS currency,

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
    emp_sex				AS gender,
    emp_language			AS employee_language,

    -- 3. Contact Information
    emp_phone_number			AS phone,
    emp_email_address			AS email,
    emp_login_name			AS login,

    -- 4. Address id and Employment start date
    emp_address_id			AS address_id,
    emp_validity_start_date		AS valid_from,

    -- 5. Data Warehouse Metadata
    dwh_create_date
	
FROM silver.erp_employees

GO

-- =============================================================================
-- Create Dimension: gold.dim_date
-- =============================================================================
IF OBJECT_ID('gold.dim_date', 'V') IS NOT NULL
    DROP VIEW gold.dim_date;
GO
	
CREATE VIEW gold.dim_date AS
-- Date dimension view combining created_at and changed_at dates from sales orders
	
-- Block 1: Order creation date
SELECT DISTINCT
    -- Surrogate key generated from hash of date + type (ensures uniqueness per date_type)
    CAST(HASHBYTES('SHA1', 
        CONVERT(VARCHAR, sls_order_created_at, 126) + '|created_at') AS BIGINT) AS surrogate_key,

    -- 1. Date and its type label
    sls_order_created_at                             AS date,
    'created_at'                                     AS date_type,

    -- 2. Date ID (used for joining with fact tables)
    CONVERT(INT, FORMAT(sls_order_created_at, 'yyyyMMdd')) AS date_id,

    -- 3. Calendar attributes
    YEAR(sls_order_created_at)                       AS year,
    DATEPART(QUARTER, sls_order_created_at)          AS quarter,
    MONTH(sls_order_created_at)                      AS month,
    DATENAME(MONTH, sls_order_created_at)            AS month_name,
    DATEPART(WEEK, sls_order_created_at)             AS week,
    DAY(sls_order_created_at)                        AS day,
    DATEPART(WEEKDAY, sls_order_created_at)          AS day_of_week,
    DATENAME(WEEKDAY, sls_order_created_at)          AS weekday_name,

    -- 4. Weekend flag: 1 for Saturday and Sunday, 0 otherwise
    CASE 
        WHEN DATEPART(WEEKDAY, sls_order_created_at) IN (1, 7) THEN 1
        ELSE 0
    END                                              AS is_weekend

FROM silver.crm_sales_orders

UNION

-- Block 2: Order modification date
SELECT DISTINCT
    -- Surrogate key generated from hash of date + type (ensures uniqueness per date_type)
    CAST(HASHBYTES('SHA1', 
        CONVERT(VARCHAR, sls_order_changed_at, 126) + '|changed_at') AS BIGINT) AS surrogate_key,

    -- 1. Date and its type label
    sls_order_changed_at                             AS date,
    'changed_at'                                     AS date_type,

    -- 2. Date ID (used for joining with fact tables)
    CONVERT(INT, FORMAT(sls_order_changed_at, 'yyyyMMdd')) AS date_id,

    -- 3. Calendar attributes
    YEAR(sls_order_changed_at)                       AS year,
    DATEPART(QUARTER, sls_order_changed_at)          AS quarter,
    MONTH(sls_order_changed_at)                      AS month,
    DATENAME(MONTH, sls_order_changed_at)            AS month_name,
    DATEPART(WEEK, sls_order_changed_at)             AS week,
    DAY(sls_order_changed_at)                        AS day,
    DATEPART(WEEKDAY, sls_order_changed_at)          AS day_of_week,
    DATENAME(WEEKDAY, sls_order_changed_at)          AS weekday_name,

    -- 4. Weekend flag: 1 for Saturday and Sunday, 0 otherwise
    CASE 
        WHEN DATEPART(WEEKDAY, sls_order_changed_at) IN (1, 7) THEN 1
        ELSE 0
    END                                              AS is_weekend

FROM silver.crm_sales_orders;

GO
	
-- =============================================================================
-- Create Fact: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    ROW_NUMBER() OVER (ORDER BY sls_order_id) AS sales_key, -- Surrogate key
    -- 1. Primary key
    sls_order_id			AS order_id,
	
    -- 2. Foreign keys
    sls_order_partner_id		AS order_partner_id,
	
    -- 3. Sales metadata
    sls_order_created_by		AS order_created_by,
    sls_order_created_at		AS order_created_at,
    sls_order_changed_by		AS order_changed_by,
    sls_order_changed_at		AS order_changed_at,
    sls_order_fisc_variant		AS order_fiscal_variant,
    sls_order_fiscal_year_period	AS order_fiscal_year_period,
    sls_order_org,
	
    -- 4. Financials
    sls_order_currency			AS currency,
    sls_order_gross_amount		AS gross_amount,
    sls_order_net_amount		AS net_amount,
    sls_order_tax_amount		AS tax_amount,

    -- 5. Statuses
    sls_order_lifecycle_status		AS lifecycle_status,
    sls_order_billing_status		AS billing_status,
    sls_order_delivery_status		AS delivery_status,
	
    -- 6. Data Warehouse Metadata
    dwh_create_date

FROM silver.crm_sales_orders
