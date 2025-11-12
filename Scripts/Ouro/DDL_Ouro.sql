-- =============================================================================
-- Create Dimension: ouro.dimensao_clientes
-- =============================================================================

CREATE OR REPLACE VIEW ouro.dimensao_clientes AS
SELECT
    ci.cst_id AS id_cliente,
    ci.cst_key AS numero_do_cliente,
    ci.cst_firstname AS primeiro_nome,
    ci.cst_lastname AS ultimo_nome,
    la.cntry AS pais,
    ci.cst_marital_status AS status_de_relacionamento,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gndr, 'n/a')
    END AS genero,
    ca.bdate AS aniversario,
   ci.dwh_create_date  AS data_criacao
FROM prata.crm_cli_info AS ci
LEFT JOIN prata.erp_cust_info AS ca
    ON ci.cst_key = ca.cid
LEFT JOIN prata.erp_local_info AS la
    ON ci.cst_key = la.cid;

-- =============================================================================
-- Create Dimension: ouro.dimensao_produto
-- =============================================================================

CREATE OR REPLACE VIEW ouro.dimensao_produto AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS chave_produto,
    pn.prd_id AS id_produto,
    pn.cat_id AS id_categoria,
    pn.prd_key AS numero_produto,
    pn.prd_nm AS nome_produto,
    pc.cat AS categoria,
    pc.subcat AS subcategoria,
    pc.maintenance AS manutencao,
    pn.prd_cost AS custo,
    pn.prd_line AS linha_de_produto,
    pn.prd_start_dt AS data_inicio
FROM prata.crm_prod_info AS pn
LEFT JOIN prata.erp_px_cat AS pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;

-- =============================================================================
-- Create Fact Table: ouro.dimensao_vendas
-- =============================================================================

CREATE OR REPLACE VIEW ouro.dimensao_vendas AS
SELECT
    sd.sls_ord_num AS numero_ordem,
    pr.chave_produto AS chave_produto,
    co.id_cliente AS chave_cliente,
    sd.sls_order_dt AS data_pedido,
    sd.sls_ship_dt AS data_envio,
    sd.sls_due_dt AS data_pendente,
    sd.sls_sales AS valor_venda,
    sd.sls_quantity AS quantidade,
    sd.sls_price AS preco_unitario
FROM prata.crm_vendas_info AS sd
LEFT JOIN ouro.dimensao_produto AS pr
    ON sd.sls_prd_key = pr.numero_produto
LEFT JOIN ouro.dimensao_clientes AS co
    ON sd.sls_cust_id = co.id_cliente;
