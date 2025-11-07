INSERT INTO prata.crm_vendas_info(
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    -- Order Date
CASE WHEN sls_order_dt = 0 
       OR LENGTH(sls_order_dt::text) != 8 
       THEN NULL
       ELSE TO_CHAR(TO_DATE(sls_order_dt::text, 'YYYYMMDD'), 'YYYYMMDD')::INTEGER
END AS sls_order_dt,

    -- Ship Date
CASE WHEN sls_ship_dt = 0 
       OR LENGTH(sls_ship_dt::text) != 8 
       THEN NULL
       ELSE TO_DATE(sls_ship_dt::text, 'YYYYMMDD')
END AS sls_ship_dt,
-- Due Date
CASE WHEN sls_due_dt = 0 
       OR LENGTH(sls_due_dt::text) != 8 
       THEN NULL
       ELSE TO_DATE(sls_due_dt::text, 'YYYYMMDD')
END AS sls_due_dt,
-- Quantity
    sls_quantity,
-- Corrected Sales
CASE WHEN sls_sales IS NULL 
      OR sls_sales <= 0 
      OR sls_sales != sls_quantity * ABS(sls_price)
      THEN sls_quantity * ABS(sls_price)
      ELSE sls_sales
END AS sls_sales,

-- Corrected Price
CASE WHEN sls_price IS NULL 
     OR sls_price <= 0 
     THEN sls_sales / NULLIF(sls_quantity, 0)
     ELSE sls_price
END AS sls_price

FROM bronze.crm_vendas_info;

--Checar por datas inválidas
SELECT sls_ship_dt
FROM bronze.crm_vendas_info
WHERE sls_ship_dt IS NOT NULL
  AND (
      sls_ship_dt < 19000101
      OR sls_ship_dt > 20500101
      OR LENGTH(sls_ship_dt::text) != 8
      OR sls_ship_dt = 0
  );

SELECT sls_order_dt
FROM bronze.crm_vendas_info
WHERE sls_order_dt IS NOT NULL
  AND (
      sls_order_dt < 19000101
      OR sls_order_dt > 20500101
      OR LENGTH(sls_order_dt::text) != 8
      OR sls_order_dt = 0
  );
  
SELECT 
    sls_due_dt
FROM bronze.crm_vendas_info
WHERE sls_due_dt IS NOT NULL
  AND (
      sls_due_dt < 19000101
      OR sls_due_dt > 20500101
      OR LENGTH(sls_due_dt::text) != 8
      OR sls_due_dt = 0
  );

--Checar Datas de Pedidos inválidas
SELECT
*
FROM prata.crm_vendas_info
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

--Checar qualidade dos dados entre vendas,quantidade e preço
--> vendas = quantidade * preco
--> valores nao podem ser nulo,zero ou negativo


SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_vendas_info
WHERE sls_sales != sls_quantity * sls_price 
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0;

SELECT * FROM prata.crm_vendas_info

ALTER TABLE prata.crm_vendas_info
ALTER COLUMN sls_order_dt TYPE DATE USING TO_DATE(sls_order_dt::text, 'YYYYMMDD'),
ALTER COLUMN sls_ship_dt TYPE DATE USING TO_DATE(sls_ship_dt::text, 'YYYYMMDD'),
ALTER COLUMN sls_due_dt TYPE DATE USING TO_DATE(sls_due_dt::text, 'YYYYMMDD');