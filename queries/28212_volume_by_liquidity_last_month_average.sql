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
                "type" = 'balancer_pool'
            GROUP BY
                1,
                2
        ) l
    WHERE
        num = 1
),
volume AS (
    SELECT
        date_trunc('day', block_time) AS DAY,
        exchange_contract_address AS pool,
        SUM(usd_amount) AS volume
    FROM
        dex.trades
    WHERE
        project = 'Balancer'
    GROUP BY
        1,
        2
),
liquidity AS (
    SELECT
        *
    FROM
        balancer."view_pools_liquidity"
),
liquidity_volume AS (
    SELECT
        l.day,
        l.pool,
        CONCAT(
            SUBSTRING(UPPER(la.name), 0, 15),
            ' (',
            SUBSTRING(l.pool :: text, 3, 8),
            ')'
        ) AS symbol,
        liquidity,
        volume
    FROM
        liquidity l
        JOIN volume v ON v.pool = l.pool
        AND v.day = l.day
        LEFT JOIN labels la ON l.pool = la.address
),
last_liquidity_volume AS (
    SELECT
        date_trunc('month', DAY) AS MONTH,
        pool,
        symbol,
        liquidity,
        volume
    FROM
        liquidity_volume
    WHERE
        date_trunc('month', DAY) = date_trunc('month', CURRENT_DATE - '1 month' :: INTERVAL)
)
SELECT
    MONTH,
    pool,
    symbol,
    AVG(liquidity) AS liquidity,
    AVG(volume) AS volume
FROM
    last_liquidity_volume
GROUP BY
    1,
    2,
    3