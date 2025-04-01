/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_product'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_customers
-- Expectation: No results 
SELECT
  product_key,
  COUNT(*) AS duplicate_key_count
FROM gold.dim_product
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.dim_business_partner'
-- ====================================================================
-- Check for Uniqueness of Business Partner Key in gold.dim_customers
-- Expectation: No results 
SELECT
  business_partner_key,
  COUNT(*) AS duplicate_key_count
FROM gold.dim_business_partner
GROUP BY business_partner_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.dim_employee'
-- ====================================================================
-- Check for Uniqueness of Employee Key in gold.dim_customers
-- Expectation: No results 
SELECT
  employee_id,
  COUNT(*) AS duplicate_key_count
FROM gold.dim_employee
GROUP BY employee_id
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.dim_date'
-- ====================================================================
-- Check for Uniqueness of Date Key in gold.dim_customers
-- Expectation: No results 
SELECT
  surrogate_key,
  COUNT(*) AS duplicate_key_count
FROM gold.dim_date
GROUP BY surrogate_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check for Uniqueness of Sales Key in gold.fact_sales
-- Expectation: No results 
SELECT
  sales_key,
  COUNT(*) AS duplicate_key_count
FROM gold.fact_sales
GROUP BY sales_key
HAVING COUNT(*) > 1;

-- Detect orphan foreign keys (no match in dimension)
-- Expectation: No results
    --Validate product_key
    SELECT COUNT(*) AS missing_products
    FROM gold.fact_sales
    WHERE product_key IS NULL;
    
    --Validate business_partner_key
    SELECT COUNT(*) AS missing_partners
    FROM gold.fact_sales
    WHERE business_partner_key IS NULL;
    
    --Validate dates
    SELECT COUNT(*) AS missing_created_dates
    FROM gold.fact_sales
    WHERE created_date_key IS NULL;
    
    SELECT COUNT(*) AS missing_modified_dates
    FROM gold.fact_sales
    WHERE modified_date_key IS NULL;
    
    --Validate employees:
    SELECT COUNT(*) AS missing_employees
    FROM gold.fact_sales
    WHERE employee_key IS NULL;

-- Check the data model connectivity between Fact and Dimensions
SELECT 
    fs.fact_sales_id,
    dp.product_short_description,
    bp.company_name,
    emp.first_name,
    dc.date AS created_date,
    fs.total_price
FROM gold.fact_sales fs
LEFT JOIN gold.dim_product dp ON fs.product_key = dp.product_key
LEFT JOIN gold.dim_business_partner bp ON fs.business_partner_key = bp.business_partner_key
LEFT JOIN gold.dim_employee emp ON fs.employee_key = emp.employee_id
LEFT JOIN gold.dim_date dc ON fs.created_date_key = dc.surrogate_key
WHERE fs.total_price IS NOT NULL
ORDER BY fs.total_price DESC;

-- Check for Missing values of Foreign Keys in gold.fact_sales
-- Expectation: No results 
CREATE OR ALTER VIEW gold.vw_fact_sales_join_coverage AS
SELECT
    COUNT(*) AS total_records,

    SUM(CASE WHEN product_key IS NOT NULL THEN 1 ELSE 0 END) AS matched_products,
    SUM(CASE WHEN business_partner_key IS NOT NULL THEN 1 ELSE 0 END) AS matched_partners,
    SUM(CASE WHEN employee_key IS NOT NULL THEN 1 ELSE 0 END) AS matched_employees,
    SUM(CASE WHEN created_date_key IS NOT NULL THEN 1 ELSE 0 END) AS matched_created_dates,
    SUM(CASE WHEN modified_date_key IS NOT NULL THEN 1 ELSE 0 END) AS matched_modified_dates,

    -- Percentages
    CAST(SUM(CASE WHEN product_key IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pct_product_match,
    CAST(SUM(CASE WHEN business_partner_key IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pct_partner_match,
    CAST(SUM(CASE WHEN employee_key IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pct_employee_match,
    CAST(SUM(CASE WHEN created_date_key IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pct_created_date_match,
    CAST(SUM(CASE WHEN modified_date_key IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pct_modified_date_match

FROM gold.fact_sales;
GO

SELECT * FROM gold.fact_sales_join_coverage;
