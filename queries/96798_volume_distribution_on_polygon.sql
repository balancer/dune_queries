WITH prices AS (
        SELECT date_trunc('day', minute) AS day, contract_address AS token, decimals, AVG(price) AS price
        FROM prices.usd
        WHERE minute >= '{{2. Start date}}'
        AND minute <= '{{3. End date}}'
        GROUP BY 1, 2, 3
    ),
    
    swaps AS (
        SELECT 
            COALESCE(("amountIn" / 10 ^ p1.decimals) * p1.price, ("amountOut" / 10 ^ p2.decimals) * p2.price) AS usd_amount
        FROM balancer_v2."Vault_evt_Swap" s
        LEFT JOIN prices p1 ON p1.day = date_trunc('day', evt_block_time) AND p1.token = s."tokenIn"
        LEFT JOIN prices p2 ON p2.day = date_trunc('day', evt_block_time) AND p2.token = s."tokenOut"
        WHERE evt_block_time >= '{{2. Start date}}'
        AND evt_block_time <= '{{3. End date}}'
    )
    
SELECT
    CASE 
        WHEN (usd_amount) BETWEEN 0 AND 100 THEN '< 100' 
        WHEN (usd_amount) BETWEEN 100 AND 1000 THEN '< 1K' 
        WHEN (usd_amount) BETWEEN 1000 AND 10000 THEN '< 10K' 
        WHEN (usd_amount) BETWEEN 10000 AND 100000 THEN '< 100K' 
    END AS volume,
    COUNT(usd_amount) AS n_trades
FROM swaps
GROUP BY 1
ORDER BY 2 DESC