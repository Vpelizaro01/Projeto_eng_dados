DROP TABLE if EXISTS bronze.crm_cli_info;
CREATE TABLE bronze.crm_cli_info(
	cst_id INT,
	cst_key VARCHAR (50),
	cst_firstname VARCHAR (50),
	cst_lastname VARCHAR (50),
	cst_marital_status VARCHAR (50),
	cst_gndr VARCHAR (50),
	dwh_create_date DATE
	
);
-- Criar tabela com as informções dos produtos.
DROP TABLE if EXISTS bronze.crm_prod_info;
CREATE TABLE bronze.crm_prod_info(
	prd_id INT,
	cat_id VARCHAR (50),
	prd_key VARCHAR (50),
	prd_nm VARCHAR (50),
	prd_cost INT,
	prd_line VARCHAR (50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATE
);
--Criar tabela com as informções de vendas.
DROP TABLE if EXISTS bronze.crm_vendas_info;
CREATE TABLE bronze.crm_vendas_info(
	sls_ord_num VARCHAR (50),
	sls_prd_key VARCHAR (50),
	sls_cust_id INT,
	sls_order_dt INT,	
	sls_ship_dt date,	
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

--Criar tabela clientes.
DROP TABLE if EXISTS bronze.erp_cust_info;
CREATE TABLE bronze.erp_cust_info(
	cid VARCHAR(50),
	bdate DATE,
	gndr VARCHAR

);



--Criar tabela com localição dos clientes.
DROP TABLE if EXISTS bronze.erp_local_info;
CREATE TABLE bronze.erp_local_info(
	cid VARCHAR(50),
	cntry VARCHAR(50)
);

--Criar tabela com categorias.
DROP TABLE if EXISTS bronze.erp_px_cat;
CREATE TABLE bronze.erp_px_cat(
	id VARCHAR(50),
	cat VARCHAR (50),
	subcat VARCHAR (50),
	maintenance VARCHAR (50)
);

-- =====================================
-- PROCEDURE DE CARGA
-- =====================================

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE '=====================================';
    RAISE NOTICE 'Iniciando carga da camada bronze';
    RAISE NOTICE '=====================================';

    -- =====================================
    -- CRM - CLIENTES
    -- =====================================
    RAISE NOTICE 'Carregando tabela CRM_CLIENTES';
    TRUNCATE TABLE bronze.crm_cli_info;
    
    COPY bronze.crm_cli_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
		dwh_create_date
    )
    FROM 'L:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (FORMAT CSV, HEADER true, DELIMITER ',');

    RAISE NOTICE 'Tabela CRM_CLIENTES carregada com sucesso.';

    -- =====================================
    -- CRM - PRODUTOS
    -- =====================================
    RAISE NOTICE 'Carregando tabela CRM_PRODUTOS';
    TRUNCATE TABLE bronze.crm_prod_info;

    COPY bronze.crm_prod_info (
        prd_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    FROM 'L:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (FORMAT CSV, HEADER true, DELIMITER ',');

    RAISE NOTICE 'Tabela CRM_PRODUTOS carregada com sucesso.';

    -- =====================================
    -- CRM - VENDAS
    -- =====================================
    RAISE NOTICE 'Carregando tabela CRM_VENDAS';
    TRUNCATE TABLE bronze.crm_vendas_info;

    COPY bronze.crm_vendas_info (
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
    FROM 'L:\sql-data-warehouse-project\datasets\source_crm\detalhes_venda.csv'
    WITH (FORMAT CSV, HEADER true, DELIMITER ',');

    RAISE NOTICE 'Tabela CRM_VENDAS carregada com sucesso.';

    -- =====================================
    -- ERP - CLIENTES
    -- =====================================
    RAISE NOTICE 'Carregando tabela ERP_CLIENTES';
    TRUNCATE TABLE bronze.erp_cust_info;

    COPY bronze.erp_cust_info (
        cid,
        bdate,
        gndr
    )
    FROM 'L:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH (FORMAT CSV, HEADER true, DELIMITER ',');

    RAISE NOTICE 'Tabela ERP_CLIENTES carregada com sucesso.';

    -- =====================================
    -- ERP - LOCALIZAÇÃO
    -- =====================================
    RAISE NOTICE 'Carregando tabela ERP_LOCAL_INFO';
    TRUNCATE TABLE bronze.erp_local_info;

    COPY bronze.erp_local_info (
        cid,
        cntry
    )
    FROM 'L:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH (FORMAT CSV, HEADER true, DELIMITER ',');

    RAISE NOTICE 'Tabela ERP_LOCAL_INFO carregada com sucesso.';

    -- =====================================
    -- ERP - CATEGORIAS
    -- =====================================
    RAISE NOTICE 'Carregando tabela ERP_PX_CAT';
    TRUNCATE TABLE bronze.erp_px_cat;

    COPY bronze.erp_px_cat (
        id,
        cat,
        subcat,
        maintenace
    )
    FROM 'L:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (FORMAT CSV, HEADER true, DELIMITER ',');

    RAISE NOTICE 'Tabela ERP_PX_CAT carregada com sucesso.';

    -- =====================================
    -- LOG FINAL
    -- =====================================
    RAISE NOTICE '=====================================';
    RAISE NOTICE 'Carga da camada bronze finalizada com sucesso!';
    RAISE NOTICE '=====================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==================';
        RAISE NOTICE 'ERRO ao carregar dados na camada bronze';
        RAISE NOTICE 'Mensagem: %', SQLERRM;
        RAISE NOTICE 'Código: %', SQLSTATE;
        RAISE NOTICE '==================';
        RAISE;
END;
$$;
