-- Volume (pool breakdown) per day (top 5 pools of each day) (7 days)
-- Visualization: bar chart (stacked)
WITH swaps AS (
    SELECT
        date_trunc('day', d.block_time) AS DAY,
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
        AND date_trunc('day', d.block_time) > date_trunc('day', NOW() - INTERVAL '1 week')
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
)
SELECT
    *
FROM
    (
        SELECT
            s.address,
            CONCAT(
                SUBSTRING(UPPER(l.name), 0, 15),
                ' (',
                SUBSTRING(s.address :: text, 3, 8),
                ')'
            ) AS pool,
            DAY,
            s.traders,
            ROW_NUMBER() OVER (
                PARTITION BY DAY
                ORDER BY
                    SUM(volume) DESC NULLS LAST
            ) AS position,
            ROUND(sum(s.volume), 2) AS volume
        FROM
            swaps s
            LEFT JOIN labels l ON l.address = SUBSTRING(s.address :: text, 0, 43) :: bytea
        GROUP BY
            1,
            2,
            3,
            4
        ORDER BY
            1,
            2,
            3,
            4
    ) ranking
WHERE
    position <= 5
    AND volume > 0