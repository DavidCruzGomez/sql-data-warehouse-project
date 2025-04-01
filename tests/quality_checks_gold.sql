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
