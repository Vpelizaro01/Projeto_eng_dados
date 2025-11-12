SELECT 
    data_pedido,
    vendas_totais,
    SUM(vendas_totais) OVER (ORDER BY data_pedido) AS vendas_totais_recorrentes,
    AVG(preco_medio) OVER (ORDER BY data_pedido) AS preco_medio_recorrente

FROM (
    SELECT
        DATE_TRUNC('month', data_pedido)::date AS data_pedido,
        SUM(valor_venda) AS vendas_totais,
        AVG(preco_unitario) AS preco_medio
    FROM ouro.dimensao_vendas
    WHERE data_pedido IS NOT NULL
    GROUP BY DATE_TRUNC('month', data_pedido)
)t
ORDER BY data_pedido;
