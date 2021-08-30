WITH lbp_info AS (
        SELECT *
        FROM balancer.view_lbps
        WHERE name = '{{LBP}}'
    ),
    
    pool_token_price AS (
        SELECT  
            date_trunc('hour', block_time) AS hour,
            token_a_address AS token,
            AVG(usd_amount/(token_a_amount_raw/10^COALESCE(decimals, 18))) AS price
        FROM dex.trades d
        INNER JOIN lbp_info l ON l.pool = d.exchange_contract_address AND l.token_sold = d.token_a_address
        LEFT JOIN erc20.tokens t ON t.contract_address = l.token_sold
        WHERE project = 'Balancer' AND block_time <= l.final_time
        GROUP BY 1, 2
    ),

    sales_stats AS (
        SELECT *
        FROM  (SELECT DISTINCT ON (1) token, MIN(price) AS min_price, MAX(price) AS max_price FROM pool_token_price GROUP BY 1) f
        JOIN  (SELECT DISTINCT ON (1) token, price AS final_price FROM pool_token_price ORDER BY token, hour DESC) l USING (token)
    )
    
SELECT * FROM sales_stats