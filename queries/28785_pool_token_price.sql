WITH lbp_info AS (
        SELECT *
        FROM balancer.view_lbps
        WHERE name = '{{LBP}}'
    ),
    
    pool_token_price AS (
        SELECT  
            date_trunc('hour', block_time) AS hour,
            l.token_symbol,
            AVG(usd_amount/(token_a_amount_raw/10^COALESCE(decimals, 18))) AS price
        FROM dex.trades d
        INNER JOIN lbp_info l ON l.pool = d.exchange_contract_address AND l.token_sold = d.token_a_address
        LEFT JOIN erc20.tokens t ON t.contract_address = l.pool
        WHERE project = 'Balancer' AND block_time <= l.final_time
        GROUP BY 1, 2
    )

SELECT * FROM pool_token_price