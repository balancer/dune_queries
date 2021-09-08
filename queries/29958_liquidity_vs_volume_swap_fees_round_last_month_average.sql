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
fees AS (
    SELECT
        call_block_number,
        contract_address AS pool,
        FIRST_VALUE("swapFee" / 10 ^ 16) OVER (
            PARTITION BY contract_address
            ORDER BY
                call_block_number DESC
        ) AS swap_fee
    FROM
        balancer."BPool_call_setSwapFee"
    WHERE
        call_success = 'true'
    GROUP BY
        call_block_number,
        contract_address,
        "swapFee"
),
last_liquidity_volume AS (
    SELECT
        date_trunc('month', DAY) AS MONTH,
        l.pool,
        l.symbol,
        liquidity,
        volume,
        f.swap_fee :: text,
        ROUND(f.swap_fee :: numeric, { { Swap fee decimals } }) :: text AS fee_class
    FROM
        liquidity_volume l
        INNER JOIN fees f ON l.pool = f.pool
    WHERE
        date_trunc('month', DAY) = date_trunc('month', CURRENT_DATE - '1 month' :: INTERVAL)
)
SELECT
    CASE
        WHEN '{{Fee value}}' <> 'none' THEN (
            CASE
                WHEN fee_class = '{{Fee value}}' THEN fee_class
                ELSE 'Others'
            END
        )
        ELSE fee_class
    END AS class,
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
    3,
    4