WITH lbp_info AS (
        SELECT *
        FROM balancer.view_lbps
        WHERE name = '{{LBP}}'
    ),
    
    denorm_weight AS (
        SELECT DISTINCT
            token,
            denorm AS denorm_weight,
            call_block_time AS block_time
        FROM balancer."BPool_call_rebind" r
        INNER JOIN lbp_info l ON l.pool = r.contract_address
    ),
    
    denorm_sum AS (
        SELECT 
            block_time,
            COUNT(*) AS n_tokens,
            SUM(denorm_weight) AS denorm_sum
        FROM denorm_weight
        GROUP BY 1
    )

SELECT 
    w.block_time,
    w.token,
    COALESCE(e.symbol, SUBSTRING(w.token::text, 3, 3)) AS symbol,
    (denorm_weight/denorm_sum) * 100 AS normalized_weight
FROM denorm_weight w
INNER JOIN denorm_sum s ON s.block_time = w.block_time
LEFT JOIN erc20.tokens e ON e.contract_address = w.token
WHERE n_tokens > 1
ORDER BY 1