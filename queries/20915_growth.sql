WITH prices AS (
    SELECT
        date_trunc('day', MINUTE) AS DAY,
        contract_address AS token,
        AVG(price) AS price
    FROM
        prices.usd
    GROUP BY
        1,
        2
),
dex_prices AS (
    SELECT
        date_trunc('day', HOUR) AS DAY,
        contract_address AS token,
        (
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY
                    median_price
            )
        ) AS price
    FROM
        dex.view_token_prices
    WHERE
        sample_size > 10
    GROUP BY
        1,
        2
),
daily_delta_balance_by_pool AS (
    SELECT
        DAY,
        pool,
        token,
        (
            cumulative_amount - LAG(cumulative_amount, 1) OVER (
                PARTITION BY pool,
                token
                ORDER BY
                    DAY
            )
        ) AS amount
    FROM
        balancer.view_balances b
),
daily_delta_balance_by_token AS (
    SELECT
        DAY,
        token,
        SUM(amount) AS amount
    FROM
        daily_delta_balance_by_pool
    GROUP BY
        1,
        2
),
cumulative_usd_balance_by_token AS (
    SELECT
        b.pool,
        b.day,
        b.token,
        cumulative_amount / 10 ^ t.decimals * p1.price AS amount_usd_from_api,
        cumulative_amount / 10 ^ t.decimals * p2.price AS amount_usd_from_dex
    FROM
        balancer.view_balances b
        LEFT JOIN erc20.tokens t ON t.contract_address = b.token
        LEFT JOIN prices p1 ON p1.day = b.day
        AND p1.token = b.token
        LEFT JOIN dex_prices p2 ON p2.day = b.day
        AND p2.token = b.token
),
pool_liquidity_estimates AS (
    SELECT
        b.*,
        b.amount_usd_from_api / w.normalized_weight AS liquidity_from_api,
        b.amount_usd_from_dex / w.normalized_weight AS liquidity_from_dex
    FROM
        cumulative_usd_balance_by_token b
        INNER JOIN balancer.view_pools_tokens_weights w ON b.pool = w.pool_address
        AND b.token = w.token_address
        AND (
            b.amount_usd_from_api > 0
            OR b.amount_usd_from_dex > 0
        )
        AND w.normalized_weight > 0
),
estimated_pool_liquidity AS (
    SELECT
        pool,
        DAY,
        coalesce(avg(liquidity_from_api), avg(liquidity_from_dex)) AS liquidity
    FROM
        pool_liquidity_estimates
    GROUP BY
        1,
        2
),
total_value_locked AS (
    SELECT
        DAY,
        SUM(liquidity) AS amount
    FROM
        estimated_pool_liquidity
    GROUP BY
        1
),
-- get the TVL on Sundays and the TVL for today
TVL_at_end_of_sunday AS (
    SELECT
        -- attribute the TVL to Sunday of the week
        date_trunc('week', DAY) + INTERVAL '6 days' AS sunday,
        amount
    FROM
        total_value_locked
    WHERE
        extract(
            dow
            FROM
                DAY
        ) = 0
        OR date_trunc('day', DAY) = date_trunc('day', NOW())
),
-- get the USD value of the tokens added/removed to/from the system on each day
daily_delta_usd_balance_by_token AS (
    SELECT
        b.day,
        b.token,
        COALESCE(
            amount / 10 ^ t.decimals * p1.price,
            amount / 10 ^ t.decimals * p2.price,
            0
        ) AS amount
    FROM
        daily_delta_balance_by_token b
        LEFT JOIN erc20.tokens t ON t.contract_address = b.token
        LEFT JOIN prices p1 ON p1.day = b.day
        AND p1.token = b.token
        LEFT JOIN dex_prices p2 ON p2.day = b.day
        AND p2.token = b.token
),
-- aggregate the USD value of the tokens on a weekly basis
weekly_delta_usd_balance AS (
    SELECT
        date_trunc('week', DAY) AS monday_of_the_week,
        -- the Monday of the week of the state change
        SUM(amount) AS delta_balance
    FROM
        daily_delta_usd_balance_by_token
    GROUP BY
        1
),
-- compute growth based on the initial TVL (TVL on Sunday) and the liquidity added over the following week
-- the last record of TVL will have no corresponding record of growth, 
-- but we'll use it to display growth and "TVL after growth"
growth AS (
    SELECT
        t.sunday + INTERVAL '1 day' AS monday_of_the_week,
        t.amount AS initial_tvl,
        d.delta_balance AS delta_balance_for_the_week,
        d.delta_balance / t.amount AS growth
    FROM
        TVL_at_end_of_sunday t -- join Sunday with the next Monday 
        LEFT JOIN weekly_delta_usd_balance d ON d.monday_of_the_week = t.sunday + INTERVAL '1 day'
) -- for plotting purposes, join the week's growth data
-- with the following week's initial TVL data (that is, this week's final TVL data)
SELECT
    initial_tvl_and_growth.monday_of_the_week AS week,
    initial_tvl_and_growth.growth * 100 AS "Growth (%)",
    tvl_after_growth.initial_tvl / 10 ^ 6 AS "TVL (million USD)"
FROM
    growth initial_tvl_and_growth
    INNER JOIN growth tvl_after_growth ON initial_tvl_and_growth.monday_of_the_week = tvl_after_growth.monday_of_the_week - INTERVAL '1 week'