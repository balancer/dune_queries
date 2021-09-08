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
                "type" = 'balancer_v2_pool'
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
swaps_changes AS (
    SELECT
        DAY,
        pool,
        token,
        SUM(COALESCE(delta, 0)) AS delta
    FROM
        (
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
        ) swaps
    GROUP BY
        1,
        2,
        3
),
internal_changes AS (
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
),
balances_changes AS (
    SELECT
        date_trunc('day', evt_block_time) AS DAY,
        "poolId" AS pool,
        UNNEST(tokens) AS token,
        UNNEST(deltas) AS delta
    FROM
        balancer_v2."Vault_evt_PoolBalanceChanged"
),
managed_changes AS (
    SELECT
        date_trunc('day', evt_block_time) AS DAY,
        "poolId" AS pool,
        token,
        "managedDelta" AS delta
    FROM
        balancer_v2."Vault_evt_PoolBalanceManaged"
),
daily_delta_balance AS (
    SELECT
        DAY,
        pool,
        token,
        SUM(COALESCE(amount, 0)) AS amount
    FROM
        (
            SELECT
                DAY,
                pool,
                token,
                SUM(COALESCE(delta, 0)) AS amount
            FROM
                balances_changes
            GROUP BY
                1,
                2,
                3
            UNION
            ALL
            SELECT
                DAY,
                pool,
                token,
                delta AS amount
            FROM
                swaps_changes
            UNION
            ALL
            SELECT
                DAY,
                pool,
                token,
                delta AS amount
            FROM
                internal_changes
            UNION
            ALL
            SELECT
                DAY,
                pool,
                token,
                delta AS amount
            FROM
                managed_changes
        ) balance
    GROUP BY
        1,
        2,
        3
),
cumulative_balance AS (
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
        daily_delta_balance
),
calendar AS (
    SELECT
        generate_series(
            '2021-04-21' :: timestamp,
            CURRENT_DATE,
            '1 day' :: INTERVAL
        ) AS DAY
),
cumulative_usd_balance AS (
    SELECT
        c.day,
        b.pool,
        b.token,
        cumulative_amount,
        cumulative_amount / 10 ^ t.decimals * p1.price AS amount_usd_from_api,
        cumulative_amount / 10 ^ t.decimals * p2.price AS amount_usd_from_dex
    FROM
        calendar c
        LEFT JOIN cumulative_balance b ON b.day <= c.day
        AND c.day < b.day_of_next_change
        LEFT JOIN erc20.tokens t ON t.contract_address = b.token
        LEFT JOIN prices p1 ON p1.day = b.day
        AND p1.token = b.token
        LEFT JOIN dex_prices p2 ON p2.day <= c.day
        AND c.day < p2.day_of_next_change
        AND p2.token = b.token
),
estimated_pool_liquidity AS (
    SELECT
        DAY,
        pool,
        SUM(
            COALESCE(amount_usd_from_api, amount_usd_from_dex)
        ) AS liquidity
    FROM
        cumulative_usd_balance
    GROUP BY
        1,
        2
),
total_tvl AS (
    SELECT
        DAY,
        pool,
        SUM(liquidity) AS "TVL"
    FROM
        estimated_pool_liquidity
    GROUP BY
        1,
        2
),
last_tvl AS(
    SELECT
        SUBSTRING(t.pool, 0, 21) AS pool,
        t."TVL" AS tvl
    FROM
        total_tvl t
    WHERE
        DAY = date_trunc('day', NOW())
),
transfers AS (
    SELECT
        *
    FROM
        balancer_v2."WeightedPool_evt_Transfer"
    UNION
    ALL
    SELECT
        *
    FROM
        balancer_v2."StablePool_evt_Transfer"
),
joins AS (
    SELECT
        date_trunc('day', e.evt_block_time) AS DAY,
        "to" AS lp,
        contract_address AS pool,
        SUM(value) / 1e18 AS amount
    FROM
        transfers e
    WHERE
        "from" IN (
            '\xBA12222222228d8Ba445958a75a0704d566BF2C8',
            '\x0000000000000000000000000000000000000000'
        )
    GROUP BY
        1,
        2,
        3
),
exits AS (
    SELECT
        date_trunc('day', e.evt_block_time) AS DAY,
        "from" AS lp,
        contract_address AS pool,
        - SUM(value) / 1e18 AS amount
    FROM
        transfers e
    WHERE
        "to" IN (
            '\xBA12222222228d8Ba445958a75a0704d566BF2C8',
            '\x0000000000000000000000000000000000000000'
        )
    GROUP BY
        1,
        2,
        3
),
daily_delta_bpt_by_pool AS (
    SELECT
        DAY,
        lp,
        pool,
        SUM(COALESCE(amount, 0)) AS amount
    FROM
        (
            SELECT
                *
            FROM
                joins j
            UNION
            ALL
            SELECT
                *
            FROM
                exits e
        ) foo
    GROUP BY
        1,
        2,
        3
),
cumulative_bpt_by_pool AS (
    SELECT
        DAY,
        lp,
        pool,
        amount,
        LEAD(DAY :: timestamptz, 1, CURRENT_DATE :: timestamptz) OVER (
            PARTITION BY lp,
            pool
            ORDER BY
                DAY
        ) AS next_day,
        SUM(amount) OVER (
            PARTITION BY lp,
            pool
            ORDER BY
                DAY ROWS BETWEEN UNBOUNDED PRECEDING
                AND CURRENT ROW
        ) AS amount_bpt
    FROM
        daily_delta_bpt_by_pool
),
running_cumulative_bpt_by_pool AS (
    SELECT
        c.day,
        lp,
        pool,
        amount_bpt
    FROM
        calendar c
        LEFT JOIN cumulative_bpt_by_pool b ON b.day <= c.day
        AND c.day <= b.next_day
),
daily_total_bpt AS (
    SELECT
        DAY,
        pool,
        SUM(amount_bpt) AS total_bpt
    FROM
        running_cumulative_bpt_by_pool
    GROUP BY
        1,
        2
),
lps_shares AS (
    SELECT
        c.day,
        c.lp,
        c.pool,
        CASE
            WHEN total_bpt > 0 THEN c.amount_bpt / d.total_bpt
            ELSE 0
        END AS SHARE
    FROM
        running_cumulative_bpt_by_pool c
        INNER JOIN daily_total_bpt d ON d.day = c.day
        AND d.pool = c.pool
    WHERE
        c.day = CURRENT_DATE
),
pools AS (
    SELECT
        DISTINCT ON (s.pool) s.pool,
        t.tvl,
        CASE
            WHEN '{{1. LP address}}' = 'All' THEN NULL
            ELSE SHARE
        END AS SHARE,
        s.lp
    FROM
        lps_shares s
        INNER JOIN last_tvl t ON t.pool = s.pool
    WHERE
        lp = CONCAT(
            '\', SUBSTRING(' { { 1.LP address } } ', 2))::bytea
        OR ' { { 1.LP address } } ' = ' ALL '
    )
    
SELECT 
    CONCAT(SUBSTRING(UPPER(l.name), 0, 16)) AS composition,
    tvl,
    CASE WHEN ' { { 1.LP address } } ' = ' ALL ' THEN NULL ELSE CONCAT(trunc(share*100, 5), ' % ') END AS share,
    CASE WHEN ' { { 1.LP address } } ' = ' ALL ' THEN CONCAT(' < a href = "https://duneanalytics.com/balancerlabs/Balancer-V2-LP-Revenues?2.%20Pool%20ID=0', SUBSTRING(r." poolId "::text, 2), '" > VIEW stats < / a > ')
    ELSE CONCAT(' < a href = "https://duneanalytics.com/balancerlabs/Balancer-V2-LP-Revenues?2.%20Pool%20ID=0', SUBSTRING(r." poolId "::text, 2), '&1.%20LP%20address=0', SUBSTRING(lp::text, 2), '" > VIEW stats < / a > ') END AS stats,
    CONCAT(' < a target = "_blank" href = "https://app.balancer.fi/#/pool/0', SUBSTRING(r." poolId "::text, 2), '" > VIEW pool < / a > ') AS pool,
    CONCAT(' < a target = "_blank" href = "https://etherscan.io/address/0', SUBSTRING(pool::text, 2, 42), '" > 0 ', SUBSTRING(pool::text, 2, 42), ' < / a > ') AS etherscan
FROM pools p
INNER JOIN balancer_v2."Vault_evt_PoolRegistered" r ON p.pool = r."poolAddress"
LEFT JOIN labels l ON l.address = p.pool
ORDER BY 2 DESC NULLS LAST