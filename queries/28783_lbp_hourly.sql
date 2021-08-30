WITH lbp_info AS (
    SELECT *
    FROM balancer.view_lbps
    WHERE name = '{{LBP}}'
)

SELECT 
    date_trunc('hour', block_time) AS hour, 
    CONCAT(l.token_symbol, ' LBP') AS symbol,
    COUNT(*) AS transactions,
    SUM(usd_amount) AS volume,
    SUM(CASE WHEN token_b_address = l.token_sold THEN 1 ELSE 0 END) AS sales,
    SUM(CASE WHEN token_b_address != l.token_sold THEN 1 ELSE 0 END) AS purcharses
FROM dex."trades" t
INNER JOIN lbp_info l ON l.pool = t.exchange_contract_address
WHERE project = 'Balancer' AND block_time <= l.final_time AND block_time >= l.initial_time
GROUP BY 1, 2