WITH prices AS (
        SELECT date_trunc('day', minute) AS day, contract_address AS token, decimals, AVG(price) AS price
        FROM prices.usd
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
    
    swaps_changes AS (
        SELECT day, pool, token, SUM(COALESCE(delta, 0)) AS delta FROM (
        SELECT date_trunc('day', evt_block_time) AS day, "poolId" AS pool, "tokenIn" AS token, "amountIn" AS delta
        FROM balancer_v2."Vault_evt_Swap"
        UNION ALL
        SELECT date_trunc('day', evt_block_time) AS day, "poolId" AS pool, "tokenOut" AS token, -"amountOut" AS delta
        FROM balancer_v2."Vault_evt_Swap") swaps
        GROUP BY 1, 2, 3
    ),
    
    internal_changes AS (
        SELECT date_trunc('day', evt_block_time) AS day, '\xBA12222222228d8Ba445958a75a0704d566BF2C8'::bytea AS pool, token, SUM(COALESCE(delta, 0)) AS delta 
        FROM balancer_v2."Vault_evt_InternalBalanceChanged"
        GROUP BY 1, 2, 3
    ),
    
    balances_changes AS (
        SELECT date_trunc('day', evt_block_time) AS day, "poolId" AS pool, UNNEST(tokens) AS token, UNNEST(deltas) AS delta 
        FROM balancer_v2."Vault_evt_PoolBalanceChanged"
    ),
    
    managed_changes AS (
        SELECT date_trunc('day', evt_block_time) AS day, "poolId" AS pool, token, "managedDelta" AS delta
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
        SELECT generate_series('2021-07-01'::timestamp, CURRENT_DATE, '1 day'::interval) AS day
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
        SELECT day, pool, SUM(liquidity) AS tvl
        FROM estimated_pool_liquidity
        GROUP BY 1, 2
    ),

    last_tvl AS(
        SELECT SUBSTRING(t.pool, 0, 21) AS pool, t.tvl
        FROM total_tvl t
        WHERE day = date_trunc('day', now())
    )
    
SELECT 
    CONCAT(SUBSTRING(UPPER(l.name), 0, 16)) AS composition,
    tvl,
    CONCAT('<a href="https://duneanalytics.com/balancerlabs/Balancer-Pool-Analysis-on-Polygon?1.%20Pool%20ID=0', SUBSTRING(r."poolId"::text, 2), '">view stats</a>') AS stats,
    CONCAT('<a target="_blank" href="https://polygon.balancer.fi/#/pool/0', SUBSTRING(r."poolId"::text, 2), '">balancer ↗</a>') AS pool,
    CONCAT('<a target="_blank" href="https://polygonscan.io/address/0', SUBSTRING(pool::text, 2, 42), '">etherscan ↗</a>') AS polygonscan
FROM last_tvl p
INNER JOIN balancer_v2."Vault_evt_PoolRegistered" r ON p.pool = r."poolAddress"
LEFT JOIN labels l ON l.address = p.pool
ORDER BY 2 DESC NULLS LAST