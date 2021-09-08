WITH prices AS (
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
dex_prices_1 AS (
    SELECT
        date_trunc('day', HOUR) AS DAY,
        contract_address AS token,
        (
            PERCENTILE_DISC(0.5) WITHIN GROUP (
                ORDER BY
                    median_price
            )
        ) AS price,
        SUM(sample_size) AS sample_size
    FROM
        dex.view_token_prices
    WHERE
        HOUR > '2021-04-20'
    GROUP BY
        1,
        2
    HAVING
        sum(sample_size) > 3
),
dex_prices AS (
    SELECT
        *,
        LEAD(DAY, 1, NOW()) OVER (
            PARTITION BY token
            ORDER BY
                DAY
        ) AS day_of_next_change
    FROM
        dex_prices_1
),
swaps AS (
    SELECT
        block_time,
        SUBSTRING(exchange_contract_address :: text, 0, 43) :: bytea AS pool,
        token_b_address,
        token_b_amount,
        COALESCE(s1."swapFeePercentage", s2."swapFeePercentage") / 1e18 AS swap_fee
    FROM
        dex.trades t
        LEFT JOIN balancer_v2."WeightedPool_evt_SwapFeePercentageChanged" s1 ON s1.contract_address = SUBSTRING(exchange_contract_address, 0, 21)
        AND s1.evt_block_time = (
            SELECT
                MAX(evt_block_time)
            FROM
                balancer_v2."WeightedPool_evt_SwapFeePercentageChanged"
            WHERE
                evt_block_time <= t.block_time
                AND contract_address = SUBSTRING(exchange_contract_address, 0, 21)
        )
        LEFT JOIN balancer_v2."StablePool_evt_SwapFeePercentageChanged" s2 ON s2.contract_address = SUBSTRING(exchange_contract_address, 0, 21)
        AND s2.evt_block_time = (
            SELECT
                MAX(evt_block_time)
            FROM
                balancer_v2."StablePool_evt_SwapFeePercentageChanged"
            WHERE
                evt_block_time <= t.block_time
                AND contract_address = SUBSTRING(exchange_contract_address, 0, 21)
        )
    WHERE
        project = 'Balancer'
        AND version = '2'
        AND (
            '{{1. Pool ID}}' = 'All'
            OR REGEXP_REPLACE(
                '{{1. Pool ID}}',
                '^.',
                '\')::bytea = exchange_contract_address)
    ),
    
    token_revenues AS (
        SELECT
            date_trunc(' DAY ', block_time) AS day,
            pool,
            token_b_address,
            SUM(token_b_amount * swap_fee) AS revenues
        FROM swaps s
        GROUP BY 1, 2, 3
    ),
    
    revenues AS (
        SELECT 
            t.day, 
            SUM(revenues * COALESCE(p1.price, p2.price)) AS revenues
        FROM token_revenues t
        LEFT JOIN prices p1 ON p1.day = CURRENT_DATE AND p1.token = t.token_b_address
        LEFT JOIN dex_prices p2 ON p2.day <= CURRENT_DATE AND CURRENT_DATE < p2.day_of_next_change AND p2.token = t.token_b_address  
        GROUP BY 1
    ),
    
    weekly_revenues AS (
        SELECT date_trunc(' week ', day) AS week, AVG(revenues) AS revenues
        FROM revenues
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
    
    calendar AS (
        SELECT generate_series(' 2021 -04 -21 '::timestamp, CURRENT_DATE, ' 1 DAY '::interval) AS day
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
            SUM(COALESCE(amount_usd_from_api, amount_usd_from_dex)) AS liquidity
        FROM cumulative_usd_balance
        GROUP BY 1
    ),

    total_tvl AS (
        SELECT date_trunc(' week ', day) AS week, AVG(liquidity) AS tvl
        FROM estimated_pool_liquidity
        GROUP BY 1
    )
    
SELECT 
    r.week,
    revenues,
    tvl,
    52*(revenues/tvl) AS apr
FROM weekly_revenues r
JOIN total_tvl t ON t.week = r.week
WHERE t.week >= ' { { 2.START date } } '
AND t.week <= ' { { 3.
            END date } } '