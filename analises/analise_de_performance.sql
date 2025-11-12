WITH vendas_anuais AS (
    SELECT 
        EXTRACT(YEAR FROM f.data_pedido) AS ano_do_pedido,
        p.prd_nome,
        SUM(f.valor_venda) AS vendas_atuais
    FROM 
        ouro.dimensao_vendas f 
    LEFT JOIN 
        ouro.dimensao_produto p
        ON f.chave_produto = p.chave_produto
    WHERE 
        f.data_pedido IS NOT NULL
    GROUP BY
        EXTRACT(YEAR FROM f.data_pedido),
        p.prd_nome
)

SELECT
    ano_do_pedido,
    prd_nome,
    
    ROUND(
        AVG(vendas_atuais) 
		OVER (PARTITION BY prd_nome),2)
		AS media_de_vendas,
    
    ROUND(vendas_atuais - AVG(vendas_atuais)
	OVER (PARTITION BY prd_nome),2) 
		AS diferencial_de_vendas,
    
    CASE 
        WHEN vendas_atuais - AVG(vendas_atuais) OVER (PARTITION BY prd_nome) > 0 
            THEN 'Acima da média'
        WHEN vendas_atuais - AVG(vendas_atuais) OVER (PARTITION BY prd_nome) < 0 
            THEN 'Abaixo da média'
        ELSE 'Na média'
    END AS mudanca_na_media,
    
    LAG(vendas_atuais)
        OVER (
            PARTITION BY prd_nome 
            ORDER BY ano_do_pedido) AS vendas_do_ano_anterior,
    ROUND(vendas_atuais - LAG(vendas_atuais) 
            OVER (
                PARTITION BY prd_nome
                ORDER BY ano_do_pedido),2) AS diferencial_do_ano_anterior

FROM 
    vendas_anuais
ORDER BY 
    prd_nome, 
    ano_do_pedido;
