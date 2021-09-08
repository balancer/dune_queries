WITH prices AS (
    SELECT
        date_trunc('day', MINUTE) AS DAY,
        contract_address AS token,
        decimals,
        AVG(price) AS price
    FROM
        prices.usd
    GROUP BY
        1,
        2,
        3
),
rewards AS (
    SELECT
        date_trunc('week', DAY) AS week,
        SUM(amount) AS amount,
        SUM(usd_amount) AS usd_amount
    FROM
        dune_user_generated.balancer_liquidity_mining
    WHERE
        (
            '{{1. Pool ID}}' = 'All'
            OR pool_id = CONCAT(
                '\', SUBSTRING(' { { 1.Pool ID } } ', 2))::bytea)
        GROUP BY 1
    ),
    
    swaps_changes AS (
        SELECT day, pool, token, SUM(COALESCE(delta, 0)) AS delta FROM (
        SELECT date_trunc(' DAY ', evt_block_time) AS day, "poolId" AS pool, "tokenIn" AS token, "amountIn" AS delta
        FROM balancer_v2."Vault_evt_Swap"
        UNION ALL
        SELECT date_trunc(' DAY ', evt_block_time) AS day, "poolId" AS pool, "tokenOut" AS token, -"amountOut" AS delta
        FROM balancer_v2."Vault_evt_Swap") swaps
        GROUP BY 1, 2, 3
    ),
    
    internal_changes AS (
        SELECT date_trunc(' DAY ', evt_block_time) AS day, ' \ xBA12222222228d8Ba445958a75a0704d566BF2C8 '::bytea AS pool, token, SUM(COALESCE(delta, 0)) AS delta 
        FROM balancer_v2."Vault_evt_InternalBalanceChanged"
        GROUP BY 1, 2, 3
    ),
    
    balances_changes AS (
        SELECT date_trunc(' DAY ', evt_block_time) AS day, "poolId" AS pool, UNNEST(tokens) AS token, UNNEST(deltas) AS delta 
        FROM balancer_v2."Vault_evt_PoolBalanceChanged"
    ),
    
    managed_changes AS (
        SELECT date_trunc(' DAY ', evt_block_time) AS day, "poolId" AS pool, token, "managedDelta" AS delta
        FROM balancer_v2."Vault_evt_PoolBalanceManaged"
    ),
    
    daily_delta_balance AS (
        SELECT day, pool, token, SUM(COALESCE(amount, 0)) AS amount 
        FROM (
            SELECT day, pool, token, SUM(COALESCE(delta, 0)) AS amount 
            FROM balances_changes
            GROUP BY 1, 2, 3
            UNION ALL
            SELECT day, pool, token, delta AS amount 
            FROM swaps_changes
            UNION ALL
            SELECT day, pool, token, delta AS amount 
            FROM internal_changes
            UNION ALL
            SELECT day, pool, token, delta AS amount 
            FROM managed_changes
            ) balance
        WHERE day <= ' { { 3.
            END date } } '
        AND (' { { 1.Pool ID } } ' = ' ALL ' OR
        pool = CONCAT(' \ ', SUBSTRING(' { { 1.Pool ID } } ', 2))::bytea)
        GROUP BY 1, 2, 3
    ),
    
    cumulative_balance AS (
        SELECT 
            day,
            pool, 
            token,
            LEAD(day, 1, now()) OVER (PARTITION BY token, pool ORDER BY day) AS day_of_next_change,
            SUM(amount) OVER (PARTITION BY pool, token ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
        FROM daily_delta_balance
    ),
    
    weekly_delta_balance_by_token AS (
        SELECT day, pool, token, cumulative_amount, (cumulative_amount - COALESCE(LAG(cumulative_amount, 1) OVER (PARTITION BY pool, token ORDER BY day), 0)) AS amount
        FROM (SELECT day, pool, token, SUM(cumulative_amount) AS cumulative_amount
        FROM cumulative_balance b
        WHERE extract(dow from day) = 1
        GROUP BY 1, 2, 3) foo
    ),
    
    calendar AS (
        SELECT generate_series(' 2021 -07 -01 '::timestamp, CURRENT_DATE, ' 1 DAY '::interval) AS day
    ),
    
    cumulative_usd_balance AS (
        SELECT c.day, b.pool, b.token, cumulative_amount,
        (p1.price * cumulative_amount / 10 ^ p1.decimals) AS amount_usd_from_api,
        0 AS amount_usd_from_dex
        FROM calendar c
        LEFT JOIN cumulative_balance b ON b.day <= c.day AND c.day < b.day_of_next_change
        LEFT JOIN prices p1 ON p1.day = c.day AND p1.token = b.token
    ),
    
    estimated_pool_liquidity as (
        SELECT 
            day, 
            pool, 
            SUM(COALESCE(amount_usd_from_api, amount_usd_from_dex)) AS liquidity
        FROM cumulative_usd_balance
        GROUP BY 1, 2
    ),

    total_tvl AS (
        SELECT date_trunc(' week ', day) AS week, pool, AVG(liquidity) AS tvl
        FROM estimated_pool_liquidity
        GROUP BY 1, 2
    )
    
SELECT t.week, COALESCE(amount, 0) AS amount, t.tvl, t.tvl/r.usd_amount AS tvl_ratio
FROM total_tvl t
LEFT JOIN rewards r ON r.week = t.week
WHERE tvl IS NOT NULL
AND t.week >= ' { { 2.START date } } '
AND t.week <= ' { { 3.
        END date } } '
ORDER BY 1