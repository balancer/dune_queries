-- Volume (pool breakdown) per hour (last 24 hours)
-- Visualization: bar chart (stacked)
WITH swaps AS (
    SELECT
        date_trunc('hour', d.block_time) AS HOUR,
        sum(usd_amount) AS volume,
        d.exchange_contract_address AS address,
        COUNT(DISTINCT trader_a) AS traders
    FROM
        dex.trades d
    WHERE
        project = 'Balancer'
        AND (
            '{{4. Version}}' = 'Both'
            OR version = SUBSTRING('{{4. Version}}', 2)
        )
        AND date_trunc('hour', d.block_time) > date_trunc('hour', NOW() - INTERVAL '1 day')
        AND exchange_contract_address NOT IN (
            '\x8b6e6e7b5b3801fed2cafd4b22b8a16c2f2db21a',
            '\x1eff8af5d577060ba4ac8a29a13525bb0ee2a3d5',
            '\x59a19d8c652fa0284f44113d0ff9aba70bd46fb4',
            '\xc697051d1c6296c24ae3bcef39aca743861d9a81',
            '\x0b09dea16768f0799065c475be02919503cb2a3500020000000000000000001a'
        )
    GROUP BY
        1,
        3
),
labels AS (
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
),
ranking AS (
    SELECT
        address,
        ROW_NUMBER() OVER (
            ORDER BY
                SUM(volume) DESC NULLS LAST
        ) AS position
    FROM
        swaps
    GROUP BY
        1
)
SELECT
    s.hour,
    s.address,
    r.position,
    s.traders,
    CONCAT(
        SUBSTRING(UPPER(l.name), 0, 15),
        ' (',
        SUBSTRING(s.address :: text, 3, 8),
        ')'
    ) AS pool,
    ROUND(sum(s.volume), 2) AS volume
FROM
    swaps s
    LEFT JOIN ranking r ON r.address = s.address
    LEFT JOIN labels l ON l.address = SUBSTRING(s.address :: text, 0, 43) :: bytea
WHERE
    r.position <= 10
    AND volume > 0
GROUP BY
    1,
    2,
    3,
    4,
    5
ORDER BY
    1,
    2,
    3,
    4,
    5