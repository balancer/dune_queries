WITH lbp_info AS (
        SELECT *
        FROM balancer.view_lbps
        WHERE name = '{{LBP}}'
    ),
    
    lbp_token_out AS (
        SELECT  
            SUM(token_a_amount_raw/10^COALESCE(decimals, 18)) AS amount_out,
            SUM(usd_amount) AS usd_amount_out
        FROM dex.trades d
        INNER JOIN lbp_info l ON l.pool = d.exchange_contract_address AND l.token_sold = d.token_a_address
        LEFT JOIN erc20.tokens t ON t.contract_address = l.token_sold
        WHERE project = 'Balancer'AND d.block_time <= l.final_time
    ),
    
    lbp_token_in AS (
        SELECT  
            SUM(token_b_amount_raw/10^COALESCE(decimals, 18)) AS amount_in,
            SUM(usd_amount) AS usd_amount_in
        FROM dex.trades d
        INNER JOIN lbp_info l ON l.pool = d.exchange_contract_address AND l.token_sold = d.token_b_address
        LEFT JOIN erc20.tokens t ON t.contract_address = l.token_sold
        WHERE project = 'Balancer' AND d.block_time <= l.final_time
    )
    
SELECT (amount_out - amount_in) AS token_amount, (usd_amount_out - usd_amount_in) AS usd_amount
FROM lbp_token_in
JOIN lbp_token_out 
ON 1=1