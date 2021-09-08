WITH prices AS (
    SELECT
        date_trunc('day', MINUTE) AS DAY,
        contract_address AS token,
        AVG(price) AS price
    FROM
        prices.usd
    WHERE
        MINUTE >= '{{2. Start date}}'
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
        HOUR >= '{{2. Start date}}'
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
cumulative_usd_balance_by_token AS (
    SELECT
        b.pool,
        b.day,
        b.token,
        b.cumulative_amount,
        cumulative_amount / 10 ^ t.decimals * p1.price AS amount_usd_from_api,
        cumulative_amount / 10 ^ t.decimals * p1.price AS amount_usd_from_dex
    FROM
        balancer.view_balances b
        LEFT JOIN erc20.tokens t ON t.contract_address = b.token
        LEFT JOIN prices p1 ON p1.day = b.day
        AND p1.token = b.token
        LEFT JOIN dex_prices p2 ON p2.day <= b.day
        AND b.day < p2.day_of_next_change
        AND p2.token = b.token
),
estimated_token_liquidity AS (
    SELECT
        DAY,
        token,
        SUM(
            COALESCE(amount_usd_from_api, amount_usd_from_dex)
        ) AS tvl
    FROM
        cumulative_usd_balance_by_token
    GROUP BY
        1,
        2
),
total_tvl AS (
    SELECT
        DAY,
        'Total' AS token,
        SUM(tvl) AS tvl
    FROM
        estimated_token_liquidity
    GROUP BY
        1,
        2
),
top_tokens AS (
    SELECT
        DAY,
        token,
        t.symbol,
        tvl
    FROM
        estimated_token_liquidity e
        INNER JOIN erc20.tokens t ON t.contract_address = e.token
    WHERE
        DAY = CURRENT_DATE
    ORDER BY
        1 DESC,
        4 DESC NULLS LAST
    LIMIT
        5
)
SELECT
    *
FROM
    total_tvl
UNION
ALL
SELECT
    e.day,
    COALESCE(t.symbol, 'Others') AS token,
    SUM(e.tvl) AS tvl
FROM
    estimated_token_liquidity e
    LEFT JOIN top_tokens t ON t.token = e.token
WHERE
    e.day >= '{{2. Start date}}'
GROUP BY
    1,
    2