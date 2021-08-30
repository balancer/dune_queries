WITH prices AS (
        SELECT date_trunc('day', minute) AS day, contract_address AS token, decimals, AVG(price) AS price
        FROM prices.usd
        WHERE minute >= '{{2. Start date}}'
        AND minute <= '{{3. End date}}'
        GROUP BY 1, 2, 3
    ), 
    
    polygon_rewards AS (
        SELECT date_trunc('week', day) AS week, SUM(amount) AS amount, SUM(usd_amount) AS usd_amount
        FROM dune_user_generated.balancer_liquidity_mining
        WHERE ('{{1. Pool ID}}' = 'All' OR pool_id = CONCAT('\', SUBSTRING('{{1. Pool ID}}', 2))::bytea)
        GROUP BY 1
    ),
    
    swaps AS (
        SELECT
            date_trunc('week', s.evt_block_time) AS week,
            SUBSTRING("poolId"::text, 0, 43)::bytea AS pool,
            COALESCE(("amountIn" / 10 ^ p1.decimals) * p1.price, ("amountOut" / 10 ^ p2.decimals) * p2.price) AS usd_amount,
            COALESCE(s1."swapFeePercentage", s2."swapFeePercentage")/1e18 AS swap_fee
        FROM balancer_v2."Vault_evt_Swap" s
        LEFT JOIN prices p1 ON p1.day = date_trunc('day', evt_block_time) AND p1.token = s."tokenIn"
        LEFT JOIN prices p2 ON p2.day = date_trunc('day', evt_block_time) AND p2.token = s."tokenOut"
        LEFT JOIN balancer_v2."WeightedPool_evt_SwapFeePercentageChanged" s1 ON s1.contract_address = SUBSTRING(s."poolId", 0, 21)
        AND s1.evt_block_time = (
            SELECT MAX(evt_block_time)
            FROM balancer_v2."WeightedPool_evt_SwapFeePercentageChanged"
            WHERE evt_block_time <= s.evt_block_time
            AND contract_address = SUBSTRING(s."poolId", 0, 21))
        LEFT JOIN balancer_v2."StablePool_evt_SwapFeePercentageChanged" s2 ON s2.contract_address = SUBSTRING(s."poolId", 0, 21)
        AND s2.evt_block_time = (
            SELECT MAX(evt_block_time)
            FROM balancer_v2."StablePool_evt_SwapFeePercentageChanged"
            WHERE evt_block_time <= s.evt_block_time
            AND contract_address = SUBSTRING(s."poolId", 0, 21)
        )
        WHERE s.evt_block_time >= '{{2. Start date}}'
        AND s.evt_block_time <= '{{3. End date}}'
        AND ('{{1. Pool ID}}' = 'All' OR "poolId" = CONCAT('\', SUBSTRING('{{1. Pool ID}}', 2))::bytea)
    ),
    
    revenues_volume AS (
        SELECT
            week,
            pool,
            SUM(usd_amount) AS volume,
            SUM(usd_amount * swap_fee) AS revenues
        FROM swaps s
        GROUP BY 1, 2
    )

SELECT s.week, COALESCE(amount::int, 0) AS amount, s.revenues, s.volume, s.revenues/r.usd_amount AS revenues_ratio, s.volume/r.usd_amount AS volume_ratio
FROM revenues_volume s 
LEFT JOIN polygon_rewards r ON r.week = s.week
WHERE s.week >= '{{2. Start date}}'
AND s.week <= '{{3. End date}}'
ORDER BY 1