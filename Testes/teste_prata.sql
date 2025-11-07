-- ====================================================================
-- Checking 'prata.crm_cli_info'
-- ====================================================================
SELECT 
    cst_id,
    COUNT(*) 
FROM prata.crm_cli_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

SELECT 
    cst_key 
FROM prata.crm_cli_info
WHERE cst_key != TRIM(cst_key);

SELECT DISTINCT 
    cst_marital_status 
FROM prata.crm_cli_info;

-- ====================================================================
-- Checking 'prata.crm_prod_info'
-- ====================================================================
SELECT 
    prd_id,
    COUNT(*) 
FROM prata.crm_prod_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT 
    prd_nm 
FROM prata.crm_prod_info
WHERE prd_nm != TRIM(prd_nm);

SELECT 
    prd_cost 
FROM prata.crm_prod_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

SELECT DISTINCT 
    prd_line 
FROM prata.crm_prod_info;

SELECT 
    * 
FROM prata.crm_prod_info
WHERE prd_end_dt < prd_start_dt;

-- ====================================================================
-- Checking 'prata.crm_vendas_info'
-- ====================================================================
SELECT 
    * 
FROM prata.crm_vendas_info
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM prata.crm_vendas_info
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Checking 'prata.erp_cust_info'
-- ====================================================================
SELECT DISTINCT 
    bdate 
FROM prata.erp_cust_info
WHERE bdate < '1924-01-01' 
   OR bdate > CURRENT_DATE;

SELECT DISTINCT 
    gndr 
FROM prata.erp_cust_info;

-- ====================================================================
-- Checking 'prata.erp_local_info'
-- ====================================================================
SELECT DISTINCT 
    cntry 
FROM prata.erp_local_info
ORDER BY cntry;

-- ====================================================================
-- Checking 'prata.erp_px_cat'
-- ====================================================================
SELECT 
    * 
FROM prata.erp_px_cat
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

SELECT DISTINCT 
    maintenance 
FROM prata.erp_px_cat;