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
        LEFT JOIN dex_prices p2 ON p2.day <= b.day
        AND b.day < p2.day_of_next_change
        AND p2.token = b.token
),
pool_liquidity_estimates AS (
    SELECT
        b.*,
        w.normalized_weight,
        b.amount_usd_from_api / w.normalized_weight AS liquidity_from_api,
        b.amount_usd_from_dex / w.normalized_weight AS liquidity_from_dex
    FROM
        cumulative_usd_balance_by_token b
        INNER JOIN balancer.view_pools_tokens_weights w ON b.pool = w.pool_id
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
        token,
        normalized_weight,
        DAY,
        coalesce(avg(liquidity_from_api), avg(liquidity_from_dex)) AS liquidity
    FROM
        pool_liquidity_estimates
    GROUP BY
        1,
        2,
        3,
        4
),
estimated_token_liquidity AS (
    SELECT
        token,
        DAY,
        SUM(liquidity * normalized_weight) AS liquidity
    FROM
        estimated_pool_liquidity p
    GROUP BY
        1,
        2
),
volume AS (
    SELECT
        date_trunc('day', block_time) AS DAY,
        token_b_address AS token,
        SUM(usd_amount) AS volume
    FROM
        dex.trades
    WHERE
        project = 'Balancer'
    GROUP BY
        1,
        2
),
liquidity_volume AS (
    SELECT
        l.day,
        l.token,
        e.symbol,
        liquidity,
        volume
    FROM
        estimated_token_liquidity l
        JOIN volume v ON v.token = l.token
        AND v.day = l.day
        LEFT JOIN erc20.tokens e ON e.contract_address = l.token
),
last_liquidity_volume AS (
    SELECT
        date_trunc('week', DAY) AS week,
        token,
        symbol,
        liquidity,
        volume,
        CASE
            WHEN '{{Token}}' <> 'none' THEN (
                CASE
                    WHEN symbol = '{{Token}}' THEN symbol
                    ELSE 'Others'
                END
            )
            ELSE symbol
        END AS class
    FROM
        liquidity_volume l
    WHERE
        date_trunc('week', DAY) = date_trunc('week', CURRENT_DATE - '1 week' :: INTERVAL)
)
SELECT
    week,
    token,
    symbol,
    class,
    AVG(liquidity) AS liquidity,
    AVG(volume) AS volume
FROM
    last_liquidity_volume
GROUP BY
    1,
    2,
    3,
    4