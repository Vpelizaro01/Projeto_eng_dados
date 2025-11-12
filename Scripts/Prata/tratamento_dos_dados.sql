-- =============================================
-- PROCEDURE: prata.load_silver (com controle de tempo e exceção)
-- =============================================
CREATE OR REPLACE PROCEDURE prata.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time      TIMESTAMP;
    v_end_time        TIMESTAMP;
    v_batch_start     TIMESTAMP;
    v_batch_end       TIMESTAMP;
BEGIN
    v_start_time := clock_timestamp();
    RAISE NOTICE 'Início da carga da camada prata: %', v_start_time;

    -- =============================================
    -- LIMPEZA E CARGA: TABELA prata.crm_cli_info
    -- =============================================
    v_batch_start := clock_timestamp();
    RAISE NOTICE 'Iniciando carga da tabela prata.crm_cli_info em %', v_batch_start;

    TRUNCATE TABLE prata.crm_cli_info;

    INSERT INTO prata.crm_cli_info(
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cli_info
    ) sub
    WHERE flag_last = 1;

    v_batch_end := clock_timestamp();
    RAISE NOTICE 'Carga de prata.crm_cli_info concluída em % segundos', 
        EXTRACT(EPOCH FROM (v_batch_end - v_batch_start));

    -- =============================================
    -- LIMPEZA E CARGA: TABELA prata.crm_prod_info
    -- =============================================
    v_batch_start := clock_timestamp();
    RAISE NOTICE 'Iniciando carga de prata.crm_prod_info em %', v_batch_start;

    TRUNCATE TABLE prata.crm_prod_info;

    INSERT INTO prata.crm_prod_info(
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt,
        dwh_create_date 
    )
    SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
        SUBSTRING(prd_key FROM 7),
        prd_nm,
        COALESCE(prd_cost, 0),
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        CAST(prd_start_dt AS DATE),
        COALESCE(
            prd_end_dt,
            LEAD(CAST(prd_start_dt AS DATE)) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
        ),
        CURRENT_DATE
    FROM bronze.crm_prod_info;

    v_batch_end := clock_timestamp();
    RAISE NOTICE 'Carga de prata.crm_prod_info concluída em % segundos', 
        EXTRACT(EPOCH FROM (v_batch_end - v_batch_start));

    -- =============================================
    -- LIMPEZA E CARGA: TABELA prata.crm_vendas_info
    -- =============================================
    v_batch_start := clock_timestamp();
    RAISE NOTICE 'Iniciando carga de prata.crm_vendas_info em %', v_batch_start;

    TRUNCATE TABLE prata.crm_vendas_info;

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

    CASE 
        WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::text) != 8 THEN NULL
        ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD')
    END AS sls_order_dt,
    

    CASE 
        WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
        ELSE TO_DATE(sls_ship_dt::text, 'YYYYMMDD')
    END AS sls_ship_dt,
    
   
    CASE 
        WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::text) != 8 THEN NULL
        ELSE TO_DATE(sls_due_dt::text, 'YYYYMMDD')
    END AS sls_due_dt,
	
	sls_quantity,
    
	CASE 
        WHEN sls_sales IS NULL 
             OR sls_sales <= 0 
             OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,

    CASE 
        WHEN sls_price IS NULL 
             OR sls_price <= 0
        THEN 
            CASE 
                WHEN sls_quantity IS NULL OR sls_quantity = 0 THEN NULL
                ELSE ABS(sls_sales) / sls_quantity
            END
        ELSE sls_price
    END AS sls_price

FROM bronze.crm_vendas_info;


    v_batch_end := clock_timestamp();
    RAISE NOTICE 'Carga de prata.crm_vendas_info concluída em % segundos', 
        EXTRACT(EPOCH FROM (v_batch_end - v_batch_start));

    -- =============================================
    -- LIMPEZA E CARGA: TABELA prata.erp_px_cat
    -- =============================================
    v_batch_start := clock_timestamp();
    RAISE NOTICE 'Iniciando carga de prata.erp_px_cat em %', v_batch_start;

    TRUNCATE TABLE prata.erp_px_cat;

    INSERT INTO prata.erp_px_cat(
        id,
        cat,
        subcat,
        maintenace
    )
    SELECT 
        id,
        TRIM(cat),
        TRIM(subcat),
        TRIM(maintenance)
    FROM bronze.erp_px_cat;

    v_batch_end := clock_timestamp();
    RAISE NOTICE 'Carga de prata.erp_px_cat concluída em % segundos', 
        EXTRACT(EPOCH FROM (v_batch_end - v_batch_start));

    -- =============================================
    -- FINALIZAÇÃO
    -- =============================================
    v_end_time := clock_timestamp();
    RAISE NOTICE 'Carga da camada prata finalizada com sucesso em % segundos',
        EXTRACT(EPOCH FROM (v_end_time - v_start_time));

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'ERRO na execução da procedure: %', SQLERRM;
END;
$$;
