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
        MINUTE >= GREATEST('{{2. Start date}}', '2021-04-20' :: timestamptz)
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
transfers AS (
    SELECT
        *
    FROM
        balancer_v2."WeightedPool_evt_Transfer"
    WHERE
        evt_block_time <= '{{3. End date}}'
    UNION
    ALL
    SELECT
        *
    FROM
        balancer_v2."StablePool_evt_Transfer"
    WHERE
        evt_block_time <= '{{3. End date}}'
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
    WHERE
        (
            '{{1. Pool ID}}' = 'All'
            OR SUBSTRING(
                REGEXP_REPLACE(
                    '{{1. Pool ID}}',
                    '^.',
                    '\'),0,43)::bytea = pool)
        GROUP BY 1, 2, 3
    ),
    
    cumulative_bpt_by_pool AS (
        SELECT 
            day, 
            lp, 
            pool, 
            amount, 
            LEAD(day::timestamptz, 1, CURRENT_DATE::timestamptz) OVER (PARTITION BY lp, pool ORDER BY day) AS next_day,
            SUM(amount) OVER (PARTITION BY lp, pool ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS amount_bpt
        FROM daily_delta_bpt_by_pool
    ),
    
   calendar AS (
        SELECT generate_series(MIN(day), CURRENT_DATE, ' 1 DAY '::interval) AS day
        FROM cumulative_bpt_by_pool
    ),
    
    running_cumulative_bpt_by_pool as (
        SELECT c.day, lp, pool, amount_bpt
        FROM calendar c
        LEFT JOIN cumulative_bpt_by_pool b ON b.day <= c.day AND c.day < b.next_day
    ),
    
    daily_total_bpt AS (
        SELECT day, pool, SUM(amount_bpt) AS total_bpt
        FROM running_cumulative_bpt_by_pool
        GROUP BY 1, 2
    ),
    
    lps_shares AS (
        SELECT c.day, c.lp, c.pool, c.amount_bpt/d.total_bpt AS share
        FROM running_cumulative_bpt_by_pool c
        INNER JOIN daily_total_bpt d ON d.day = c.day AND d.pool = c.pool
        WHERE d.total_bpt > 0
    ),
    
    swaps AS (
        SELECT 
            block_time,
            SUBSTRING(exchange_contract_address::text, 0, 43)::bytea AS pool,
            token_b_address,
            token_b_amount,
            COALESCE(s1."swapFeePercentage", s2."swapFeePercentage")/1e18 AS swap_fee
        FROM dex.trades t  
        LEFT JOIN balancer_v2."WeightedPool_evt_SwapFeePercentageChanged" s1 ON s1.contract_address = SUBSTRING(exchange_contract_address, 0, 21)
        AND s1.evt_block_time = (
            SELECT MAX(evt_block_time)
            FROM balancer_v2."WeightedPool_evt_SwapFeePercentageChanged"
            WHERE evt_block_time <= t.block_time
            AND contract_address = SUBSTRING(exchange_contract_address, 0, 21)
        )
        LEFT JOIN balancer_v2."StablePool_evt_SwapFeePercentageChanged" s2 ON s2.contract_address = SUBSTRING(exchange_contract_address, 0, 21)
        AND s2.evt_block_time = (
            SELECT MAX(evt_block_time)
            FROM balancer_v2."StablePool_evt_SwapFeePercentageChanged"
            WHERE evt_block_time <= t.block_time
            AND contract_address = SUBSTRING(exchange_contract_address, 0, 21))
        WHERE project = ' Balancer '
        AND block_time <= ' { { 3.
                END date } } '
        AND version = ' 2 '
        AND (' { { 1.Pool ID } } ' = ' ALL '
        OR REGEXP_REPLACE(' { { 1.Pool ID } } ', ' ^.', ' \ ')::bytea = exchange_contract_address)
    ),
    
    revenues AS (
        SELECT
            date_trunc(' DAY ', block_time) AS day,
            pool,
            token_b_address,
            SUM(token_b_amount * swap_fee) AS revenues
        FROM swaps s
        GROUP BY 1, 2, 3
    ),
    
    lp_revenues AS (
        SELECT 
            s.day, 
            s.pool,
            CONCAT(SUBSTRING(UPPER(name), 0, 15), ' (', SUBSTRING(s.pool, 3, 8), ') ') AS label,
            lp,
            token_b_address,
            (revenues * share * COALESCE(p1.price, p2.price)) AS revenues
        FROM lps_shares s
        LEFT JOIN labels l ON l.address = s.pool
        LEFT JOIN revenues r ON r.day = s.day AND r.pool = s.pool
        LEFT JOIN prices p1 ON p1.day = CURRENT_DATE AND p1.token = r.token_b_address
        LEFT JOIN dex_prices p2 ON p2.day <= CURRENT_DATE AND CURRENT_DATE < p2.day_of_next_change AND p2.token = r.token_b_address  
    ),
    
cumulative_revenues AS (
    SELECT 
        date_trunc(' week ', day) AS week,
        SUM(revenues) AS revenues,
        SUM(SUM(revenues)) OVER (ORDER BY day) AS cumulative_revenues
    FROM lp_revenues
    GROUP BY day
)

SELECT * FROM cumulative_revenues WHERE week >= ' { { 2.START date } } ' AND week <= ' { { 3.
            END date } } '