DROP TABLE IF EXISTS prata.crm_prod_info;
CREATE TABLE prata.crm_prod_info(
	prd_id INT,
	cat_id VARCHAR(50),
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	cst_create_date DATE
);

INSERT INTO prata.crm_prod_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt,
	cst_create_date 
)
SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
    SUBSTRING(prd_key FROM 7) AS prd_key,
    prd_nm,
    COALESCE(prd_cost, 0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring' 
        ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    LEAD(CAST(prd_end_dt AS DATE)) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt,
    CURRENT_DATE AS cst_create_date
FROM bronze.crm_prod_info;

-- Checar duplicatas e nulos, na chave primaria;
SELECT 
 	prd_id,
	COUNT(*)
	FROM prata.crm_prod_info
	GROUP BY prd_id
	HAVING COUNT(*)> 1 OR prd_id IS NULL

--Checar espaços vazios
SELECT prd_cost
FROM bronze.crm_prod_info
WHERE prd_cost < 0 OR prd_cost IS NULL

--Padronização dos dados
SELECT DISTINCT prd_line
FROM bronze.crm_prod_info

--Checagem por data de pedidos inválidas
SELECT *
FROM bronze.crm_prod_info
WHERE prd_end_dt < prd_start_dt