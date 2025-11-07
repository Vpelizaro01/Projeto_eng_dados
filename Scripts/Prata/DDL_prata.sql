DROP TABLE if EXISTS prata.crm_cli_info;
CREATE TABLE prata.crm_cli_info(
	cst_id INT,
	cst_key VARCHAR (50),
	cst_firtsname VARCHAR (50),
	cst_lastname VARCHAR (50),
	cst_material_status VARCHAR (50),
	cst_gndr VARCHAR (50),
	cst_create_date DATE
	
);
-- Criar tabela com as informções dos produtos.
DROP TABLE if EXISTS prata.crm_prod_info;
CREATE TABLE prata.crm_prod_info(
	prd_id INT,
	prd_key VARCHAR (50),
	prd_nm VARCHAR (50),
	prd_cost INT,
	prd_line VARCHAR (50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	cst_create_date DATE
);
--Criar tabela com as informções de vendas.
DROP TABLE if EXISTS prata.crm_vendas_info;
CREATE TABLE prata.crm_vendas_info(
	sls_ord_num VARCHAR (50),
	sls_prd_key VARCHAR (50),
	sls_cust_id INT,
	sls_order_dt INT,	
	sls_ship_dt INT,	
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	cst_create_date DATE
);

--Criar tabela clientes.
DROP TABLE if EXISTS prata.erp_cust_info;
CREATE TABLE prata.erp_cust_info(
	cep VARCHAR(50),
	bdate DATE,
	gndr VARCHAR,
	cst_create_date DATE
);

-- renomear as tabelas para evitar conflitos com arquivos CSV
ALTER TABLE prata.erp_cust_info
RENAME COLUMN cep TO cid;

--Criar tabela com localição dos clientes.
DROP TABLE if EXISTS prata.erp_local_info;
CREATE TABLE prata.erp_local_info(
	cep VARCHAR(50),
	pais VARCHAR(50),
	cst_create_date DATE
);

--Criar tabela com categorias.
DROP TABLE if EXISTS prata.erp_px_cat;
CREATE TABLE prata.erp_px_cat(
	id VARCHAR(50),
	cat VARCHAR (50),
	subcat VARCHAR (50),
	maintenace VARCHAR (50),
	cst_create_date DATE
);
-- renomear as tabelas para evitar conflitos com arquivos CSV
ALTER TABLE prata.erp_local_info
RENAME COLUMN cep TO cid;

ALTER TABLE prata.erp_local_info
RENAME COLUMN pais TO cntry;



--Comando para inserir dados nas tabelas ("Não é necessário para postgreSQL, devido a função importar direto as tabelas via menu")
--Utilizei do TRUNCATE para limpar as linhas das tabelas antes de inserir os valores com COPY para que assim evite duplicatas.
--Utilizei dos comandos FORMAT,HEADER e DELIMITER para que o arquivo fosse importado corretamente
--SELECT foi utilizado para ver os conteudos postos nas tabelas

CREATE OR REPLACE PROCEDURE prata.load_prata()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE '=====================================';
    RAISE NOTICE 'Carregando camada prata';
    RAISE NOTICE '=====================================';
    
    RAISE NOTICE '----------------------------';
    RAISE NOTICE 'Carregando tabelas CRM';
    RAISE NOTICE '----------------------------';
    
    RAISE NOTICE 'Limpando a tabela com TRUNCATE';
    TRUNCATE TABLE prata.crm_cli_info;
    
    RAISE NOTICE 'Inserindo os dados na tabela';
    COPY prata.crm_cli_info
    FROM 'L:/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
    WITH (FORMAT CSV, HEADER true, DELIMITER ',');
    
    RAISE NOTICE 'Limpando a tabela com TRUNCATE';
    TRUNCATE TABLE prata.crm_prod_info;
    
    RAISE NOTICE 'Inserindo os dados na tabela';
    COPY prata.crm_prod_info
    FROM 'L:/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
    WITH (FORMAT CSV, HEADER true, DELIMITER ',');
    
    RAISE NOTICE 'Limpando a tabela com TRUNCATE';
    TRUNCATE TABLE prata.crm_vendas_info;
   
    RAISE NOTICE 'Inserindo os dados na tabela';
    COPY prata.crm_vendas_info
    FROM 'L:/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
    WITH (FORMAT CSV, HEADER true, DELIMITER ',');
    
    RAISE NOTICE '----------------------------';
    RAISE NOTICE 'Carregando tabelas ERP';
    RAISE NOTICE '----------------------------';
    
    RAISE NOTICE 'Limpando a tabela com TRUNCATE';
    TRUNCATE TABLE prata.erp_cust_info;
    
    RAISE NOTICE 'Inserindo os dados na tabela';
    COPY prata.erp_cust_info
    FROM 'L:/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
    WITH (FORMAT CSV, HEADER true, DELIMITER ',');
    
    RAISE NOTICE 'Limpando a tabela com TRUNCATE';
    TRUNCATE TABLE prata.erp_local_info;
    
    RAISE NOTICE 'Inserindo os dados na tabela';
    COPY prata.erp_local_info
    FROM 'L:/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
    WITH (FORMAT CSV, HEADER true, DELIMITER ',');
    
    RAISE NOTICE 'Limpando a tabela com TRUNCATE';
    TRUNCATE TABLE prata.erp_px_cat;
   
    RAISE NOTICE 'Inserindo os dados na tabela';
    COPY prata.erp_px_cat
    FROM 'L:/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
    WITH (FORMAT CSV, HEADER true, DELIMITER ',');
    
    RAISE NOTICE '=====================================';
    RAISE NOTICE 'Carga finalizada com sucesso';
    RAISE NOTICE '=====================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==================';
        RAISE NOTICE 'ERRO ao carregar';
        RAISE NOTICE 'Mensagem: %', SQLERRM;
        RAISE NOTICE 'Código: %', SQLSTATE;
        RAISE NOTICE '==================';
        RAISE; 
END;
$$;