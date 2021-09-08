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
        sum(sample_size) > 2
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
events_v2 AS (
    SELECT
        date_trunc('day', evt_block_time) AS DAY,
        "poolId" AS pool,
        "tokenIn" AS token,
        "amountIn" AS delta
    FROM
        balancer_v2."Vault_evt_Swap"
    UNION
    ALL
    SELECT
        date_trunc('day', evt_block_time) AS DAY,
        "poolId" AS pool,
        "tokenOut" AS token,
        - "amountOut" AS delta
    FROM
        balancer_v2."Vault_evt_Swap"
    UNION
    ALL
    SELECT
        date_trunc('day', evt_block_time) AS DAY,
        '\xBA12222222228d8Ba445958a75a0704d566BF2C8' :: bytea AS pool,
        token,
        SUM(COALESCE(delta, 0)) AS delta
    FROM
        balancer_v2."Vault_evt_InternalBalanceChanged"
    GROUP BY
        1,
        2,
        3
    UNION
    ALL
    SELECT
        date_trunc('day', evt_block_time) AS DAY,
        "poolId" AS pool,
        UNNEST(tokens) AS token,
        UNNEST(deltas) AS delta
    FROM
        balancer_v2."Vault_evt_PoolBalanceChanged"
    UNION
    ALL
    SELECT
        date_trunc('day', evt_block_time) AS DAY,
        "poolId" AS pool,
        token,
        "cashDelta" + "managedDelta" AS delta
    FROM
        balancer_v2."Vault_evt_PoolBalanceManaged"
),
v2_daily_delta_balance AS (
    SELECT
        DAY,
        pool,
        token,
        SUM(COALESCE(delta, 0)) AS amount
    FROM
        events_v2
    GROUP BY
        1,
        2,
        3
),
v2_cumulative_balance AS (
    SELECT
        DAY,
        pool,
        token,
        LEAD(DAY, 1, NOW()) OVER (
            PARTITION BY token,
            pool
            ORDER BY
                DAY
        ) AS day_of_next_change,
        SUM(amount) OVER (
            PARTITION BY pool,
            token
            ORDER BY
                DAY ROWS BETWEEN UNBOUNDED PRECEDING
                AND CURRENT ROW
        ) AS cumulative_amount
    FROM
        v2_daily_delta_balance
),
calendar AS (
    SELECT
        generate_series(
            '2021-04-21' :: timestamp,
            CURRENT_DATE,
            '1 day' :: INTERVAL
        ) AS DAY
),
v2_cumulative_usd_balance AS (
    SELECT
        c.day,
        b.pool,
        b.token,
        cumulative_amount,
        cumulative_amount / 10 ^ t.decimals * p1.price AS amount_usd_from_api,
        cumulative_amount / 10 ^ t.decimals * p2.price AS amount_usd_from_dex
    FROM
        calendar c
        LEFT JOIN v2_cumulative_balance b ON b.day <= c.day
        AND c.day < b.day_of_next_change
        LEFT JOIN erc20.tokens t ON t.contract_address = b.token
        LEFT JOIN prices p1 ON p1.day = b.day
        AND p1.token = b.token
        LEFT JOIN dex_prices p2 ON p2.day <= c.day
        AND c.day < p2.day_of_next_change
        AND p2.token = b.token
),
v2_estimated_pool_liquidity AS (
    SELECT
        DAY,
        pool,
        token,
        SUM(
            COALESCE(amount_usd_from_api, amount_usd_from_dex)
        ) AS liquidity
    FROM
        v2_cumulative_usd_balance
    GROUP BY
        1,
        2,
        3
),
v1_estimated_token_liquidity AS (
    SELECT
        token_address,
        DAY,
        SUM(liquidity * normalized_weight) AS liquidity
    FROM
        balancer."view_pools_liquidity" b
        INNER JOIN balancer."view_pools_tokens_weights" w ON b.pool = w.pool_id -- INNER JOIN erc20.tokens t ON w.token_address = t.contract_address
    WHERE
        token_address = (
            SELECT
                contract_address
            FROM
                erc20.tokens
            WHERE
                symbol = '{{Token}}'
        )
    GROUP BY
        1,
        2
),
swaps AS (
    SELECT
        version,
        token_a_address AS token,
        date_trunc('day', block_time) AS DAY,
        SUM(usd_amount) AS volume
    FROM
        dex.trades d
    WHERE
        project = 'Balancer'
        AND date_trunc('day', block_time) > date_trunc('day', CURRENT_DATE - '1 month' :: INTERVAL)
        AND token_a_address = (
            SELECT
                contract_address
            FROM
                erc20.tokens
            WHERE
                symbol = '{{Token}}'
        )
    GROUP BY
        1,
        2,
        3
    UNION
    ALL
    SELECT
        version,
        token_b_address AS token,
        date_trunc('day', block_time) AS DAY,
        SUM(usd_amount) AS volume
    FROM
        dex.trades d
    WHERE
        project = 'Balancer'
        AND date_trunc('day', block_time) > date_trunc('day', CURRENT_DATE - '1 month' :: INTERVAL)
        AND token_b_address = (
            SELECT
                contract_address
            FROM
                erc20.tokens
            WHERE
                symbol = '{{Token}}'
        )
    GROUP BY
        1,
        2,
        3
),
volume AS (
    SELECT
        version,
        DAY,
        SUM(volume) / 2 AS volume
    FROM
        swaps
    GROUP BY
        1,
        2
),
liquidity AS (
    SELECT
        '1' AS version,
        DAY,
        SUM(liquidity) AS liquidity
    FROM
        v1_estimated_token_liquidity b
    WHERE
        date_trunc('day', DAY) > date_trunc('day', CURRENT_DATE - '1 month' :: INTERVAL)
    GROUP BY
        1,
        2
    UNION
    ALL
    SELECT
        '2' AS version,
        DAY,
        SUM(liquidity) AS liquidity
    FROM
        v2_estimated_pool_liquidity
    WHERE
        date_trunc('day', DAY) > date_trunc('day', CURRENT_DATE - '1 month' :: INTERVAL)
        AND token = (
            SELECT
                contract_address
            FROM
                erc20.tokens
            WHERE
                symbol = '{{Token}}'
        )
    GROUP BY
        1,
        2
),
liquidity_volume AS (
    SELECT
        l.day,
        CONCAT('V', l.version) AS version,
        volume,
        liquidity,
        COALESCE(volume / liquidity, 0) AS liquidity_utilization
    FROM
        liquidity l
        JOIN volume v ON v.day = l.day
        AND v.version = l.version
)
SELECT
    *
FROM
    liquidity_volume