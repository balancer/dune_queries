WITH labels AS (
    SELECT
        *
    FROM
        (
            SELECT
                address,
                name,
                ROW_NUMBER() OVER (
                    PARTITION BY address
                    ORDER BY
                        MAX(updated_at) DESC
                ) AS num
            FROM
                labels.labels
            WHERE
                "type" IN ('balancer_pool', 'balancer_v2_pool')
            GROUP BY
                1,
                2
        ) l
    WHERE
        num = 1
),
prices AS (
    SELECT
        date_trunc('day', MINUTE) AS DAY,
        contract_address AS token,
        AVG(price) AS price
    FROM
        prices.usd
    WHERE
        MINUTE > '2021-04-20'
    GROUP BY
        1,
        2
),
calendar AS (
    SELECT
        generate_series(
            '2020/06/01' :: timestamptz,
            NOW(),
            '1 day' :: INTERVAL
        ) AS DAY
),
mainnet_rewards AS (
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
        AND chain_id = ' 1 '
        GROUP BY 1
    ),
    
    dex_prices_1 AS (
        SELECT date_trunc(' DAY ', hour) AS day, 
        contract_address AS token, 
        (PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY median_price)) AS price,
        SUM(sample_size) as sample_size
        FROM dex.view_token_prices
        WHERE hour > ' 2021 -04 -20 '
        GROUP BY 1, 2
        HAVING sum(sample_size) > 3
    ),
    
    dex_prices AS (
        SELECT *, LEAD(day, 1, now()) OVER (PARTITION BY token ORDER BY day) AS day_of_next_change
        FROM dex_prices_1
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
        WHERE (' { { 1.Pool ID } } ' = ' ALL ' OR
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
    
    cumulative_usd_balance AS (
        SELECT c.day, b.pool, b.token, cumulative_amount,
        cumulative_amount / 10 ^ t.decimals * p1.price AS amount_usd_from_api,
        cumulative_amount /10 ^ t.decimals * p2.price AS amount_usd_from_dex
        FROM calendar c
        LEFT JOIN cumulative_balance b ON b.day <= c.day AND c.day < b.day_of_next_change
        LEFT JOIN erc20.tokens t ON t.contract_address = b.token
        LEFT JOIN prices p1 ON p1.day = b.day AND p1.token = b.token
        LEFT JOIN dex_prices p2 ON p2.day <= c.day AND c.day < p2.day_of_next_change AND p2.token = b.token
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
        SELECT date_trunc(' week ', day) AS week, AVG(liquidity) AS tvl
        FROM estimated_pool_liquidity
        GROUP BY 1
    )
   
SELECT t.week, COALESCE(amount::int, 0) AS amount, t.tvl, t.tvl/r.usd_amount AS tvl_ratio
FROM total_tvl t
LEFT JOIN mainnet_rewards r ON r.week = t.week
WHERE tvl IS NOT NULL
AND t.week >= ' { { 2.START date } } '
AND t.week <= ' { { 3.
            END date } } '
ORDER BY 1