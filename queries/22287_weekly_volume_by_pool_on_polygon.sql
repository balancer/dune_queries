WITH prices AS (
        SELECT date_trunc('day', minute) AS day, contract_address AS token, decimals, AVG(price) AS price
        FROM prices.usd
        WHERE minute >= '{{2. Start date}}'
        AND minute <= '{{3. End date}}'
        GROUP BY 1, 2, 3
    ),
    
    labels AS (
        SELECT
            address,
            label AS name
        FROM dune_user_generated."balancer_pools"
        WHERE "type" = 'balancer_v2_pool'
        GROUP BY 1, 2
    ),
    
    swaps AS (
        SELECT 
            date_trunc('week', evt_block_time) AS week,
            "poolId" AS pool,
            SUM(COALESCE(("amountIn" / 10 ^ p1.decimals) * p1.price, ("amountOut" / 10 ^ p2.decimals) * p2.price)) AS volume
        FROM balancer_v2."Vault_evt_Swap" s
        LEFT JOIN prices p1 ON p1.day = date_trunc('day', evt_block_time) AND p1.token = s."tokenIn"
        LEFT JOIN prices p2 ON p2.day = date_trunc('day', evt_block_time) AND p2.token = s."tokenOut"
        WHERE evt_block_time >= '{{2. Start date}}'
        AND evt_block_time <= '{{3. End date}}'
        AND ('{{1. Pool ID}}' = 'All' OR
        "poolId" = CONCAT('\', SUBSTRING('{{1. Pool ID}}', 2))::bytea)
        GROUP BY 1, 2
    ),
    
    ranking AS (
        SELECT
            pool, 
            name,
            ROW_NUMBER() OVER (ORDER BY SUM(volume) DESC NULLS LAST) AS position
        FROM swaps s
        LEFT JOIN labels l ON l.address = SUBSTRING(s.pool, 0, 21)
        WHERE week = date_trunc('week', CURRENT_DATE - interval '1 week')
        GROUP BY 1, 2
    ),
    
    total_volume AS (
        SELECT week, 'Total' AS pool, SUM(volume) AS volume
        FROM swaps
        GROUP BY 1
    )
    
SELECT * FROM total_volume

UNION ALL

SELECT
    s.week, 
    CASE
        WHEN r.position <= 7 THEN SUBSTRING(UPPER(r.name), 0, 16)
        ELSE 'Others'
    END AS pool, 
    SUM(s.volume) AS volume
FROM swaps s
LEFT JOIN ranking r ON r.pool = s.pool
GROUP BY 1, 2
ORDER BY 1, 2