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
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
SELECT
  product_id,
  COUNT(*) AS duplicate_key_count
FROM gold.dim_product
GROUP BY product_id
HAVING COUNT(*) > 1;






Validar product_key
SELECT COUNT(*) AS missing_products
FROM gold.fact_sales
WHERE product_key IS NULL;

Validar business_partner_key
SELECT COUNT(*) AS missing_partners
FROM gold.fact_sales
WHERE business_partner_key IS NULL;

Validar fechas
SELECT COUNT(*) AS missing_created_dates
FROM gold.fact_sales
WHERE created_date_key IS NULL;

SELECT COUNT(*) AS missing_modified_dates
FROM gold.fact_sales
WHERE modified_date_key IS NULL;

Validar empleados:
SELECT COUNT(*) AS missing_employees
FROM gold.fact_sales
WHERE employee_key IS NULL;



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

