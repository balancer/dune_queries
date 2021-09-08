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
    UNION
    ALL
    SELECT
        '\x32296969ef14eb0c6d29669c550d4a0449130230' AS address,
        'B-stETH-STABLE' AS name,
        1 AS num
),
prices AS (
    SELECT
        date_trunc('day', MINUTE) AS DAY,
        contract_address AS token,
        AVG(price) AS price
    FROM
        prices.usd
    WHERE
        MINUTE >= GREATEST('{{2. Start date}}', '2021-04-20' :: timestamptz)
        AND MINUTE <= '{{3. End date}}'
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
        HOUR >= GREATEST('{{2. Start date}}', '2021-04-20' :: timestamptz)
        AND HOUR <= '{{3. End date}}'
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
    WHERE
        DAY <= '{{3. End date}}'
        AND (
            '{{1. Pool ID}}' = 'All'
            OR pool = CONCAT(
                '\', SUBSTRING(' { { 1.Pool ID } } ', 2))::bytea)
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
        cumulative_amount / 10 ^ t.decimals * COALESCE(p1.price, p2.price) AS amount_usd
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
            SUM(amount_usd) AS liquidity
        FROM cumulative_usd_balance
        GROUP BY 1, 2
    ),

    tvl AS (
        SELECT day, ' V2 ' as version, pool, SUM(liquidity) AS "TVL"
        FROM estimated_pool_liquidity
        GROUP BY 1, 2, 3
        
        UNION ALL
        
        SELECT p.day, ' V1 ' as version, pool, SUM(p.liquidity) AS tvl
        FROM balancer."view_pools_liquidity" p
        WHERE pool <> ' \ xBA12222222228d8Ba445958a75a0704d566BF2C8 '
        AND day <= ' { { 3.
            END date } } '
        GROUP BY 1, 2, 3
    ),
    
    total_tvl AS (
        SELECT day, ' Total ' AS pool, SUM("TVL") AS "TVL"
        FROM tvl
        GROUP BY 1, 2
    ),
    
    top_pools AS (
        SELECT DISTINCT pool, "TVL", CONCAT(SUBSTRING(UPPER(l.name), 0, 15), ' (', SUBSTRING(t.pool::text, 3, 8), ') ') AS symbol
        FROM tvl t
        LEFT JOIN labels l ON l.address = SUBSTRING(t.pool::text, 0, 43)::bytea
        WHERE day = LEAST(CURRENT_DATE, ' { { 3.
        END date } } ')
        AND "TVL" IS NOT NULL
        ORDER BY 2 DESC, 3 DESC 
        LIMIT 5
    )

SELECT * FROM total_tvl
WHERE day >= ' { { 2.START date } } '

UNION ALL

SELECT t.day, COALESCE(p.symbol, ' Others ') AS pool, SUM(t."TVL") AS "TVL"
FROM tvl t
LEFT JOIN top_pools p ON p.pool = t.pool
WHERE day >= ' { { 2.START date } } '
GROUP BY 1, 2
ORDER BY 1