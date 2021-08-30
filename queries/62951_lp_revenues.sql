WITH swap_fees AS (
        SELECT 
            contract_address AS pool, 
            LAST_VALUE("swapFee") OVER(PARTITION BY contract_address ORDER BY call_block_time RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)/1e18 AS swap_fee
        FROM balancer."BPool_call_setSwapFee"
        WHERE call_success
    ),
    
    swaps AS (
        SELECT 
            block_time AS week,
            exchange_contract_address AS pool,
            usd_amount
        FROM dex.trades
        WHERE project = 'Balancer'
        AND version = '1'
    )

SELECT 
    date_trunc('week', week) AS week,
    SUM(usd_amount * swap_fee) AS revenues
FROM swaps ta
INNER JOIN swap_fees f ON f.pool = ta.pool
GROUP BY 1