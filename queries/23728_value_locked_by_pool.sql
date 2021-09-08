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
top_tokens AS (
    SELECT
        DAY,
        pool,
        liquidity
    FROM
        estimated_pool_liquidity e
    ORDER BY
        1 DESC,
        3 DESC
    LIMIT
        10
), labels AS (
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
                "type" = 'balancer_pool'
            GROUP BY
                1,
                2
        ) l
    WHERE
        num = 1
)
SELECT
    e.day,
    e.pool AS address,
    SUBSTRING(UPPER(l.name), 0, 16) AS pool,
    SUM(e.liquidity) AS "TVL"
FROM
    estimated_pool_liquidity e
    INNER JOIN top_tokens t ON t.pool = e.pool
    LEFT JOIN labels l ON l.address = e.pool
GROUP BY
    1,
    2,
    3
ORDER BY
    3 DESC