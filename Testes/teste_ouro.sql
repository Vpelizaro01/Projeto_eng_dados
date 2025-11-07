-- ====================================================================
-- Checking 'ouro.dimensao_clientes'
-- ====================================================================
SELECT 
    id_cliente,
    COUNT(*) AS duplicados
FROM ouro.dimensao_clientes
GROUP BY id_cliente
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'ouro.dimensao_produto'
-- ====================================================================
SELECT 
    chave_produto,
    COUNT(*) AS duplicados
FROM ouro.dimensao_produto
GROUP BY chave_produto
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'ouro.dimensao_vendas'
-- ====================================================================
-- Verifica integridade entre fato e dimensões
SELECT 
    f.numero_ordem,
    f.chave_cliente,
    f.chave_produto
FROM ouro.dimensao_vendas AS f
LEFT JOIN ouro.dimensao_clientes AS c
    ON c.id_cliente = f.chave_cliente
LEFT JOIN ouro.dimensao_produto AS p
    ON p.chave_produto = f.chave_produto
WHERE c.id_cliente IS NULL 
   OR p.chave_produto IS NULL;