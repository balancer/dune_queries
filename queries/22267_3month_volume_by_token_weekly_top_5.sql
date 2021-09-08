-- Volume (token breakdown) per week (3 months)
-- Visualization: bar chart (stacked)
WITH swaps AS (
    SELECT
        date_trunc('week', d.block_time) AS week,
        sum(usd_amount) AS volume,
        d.token_b_address AS address,
        t.symbol AS token
    FROM
        dex.trades d
        LEFT JOIN erc20.tokens t ON t.contract_address = d.token_b_address
    WHERE
        project = 'Balancer'
        AND date_trunc('week', d.block_time) >= date_trunc('week', NOW() - INTERVAL '3 months')
        AND (
            '{{4. Version}}' = 'Both'
            OR version = SUBSTRING('{{4. Version}}', 2)
        )
    GROUP BY
        1,
        3,
        4
    UNION
    ALL
    SELECT
        date_trunc('week', d.block_time) AS week,
        sum(usd_amount) AS volume,
        d.token_a_address AS address,
        t.symbol AS token
    FROM
        dex.trades d
        LEFT JOIN erc20.tokens t ON t.contract_address = d.token_a_address
    WHERE
        project = 'Balancer'
        AND date_trunc('week', d.block_time) >= date_trunc('week', NOW() - INTERVAL '2 months')
        AND (
            '{{4. Version}}' = 'Both'
            OR version = SUBSTRING('{{4. Version}}', 2)
        )
    GROUP BY
        1,
        3,
        4
)
SELECT
    *
FROM
    (
        SELECT
            s.week,
            COALESCE(
                s.token,
                CONCAT(SUBSTRING(s.address :: text, 3, 6), '...')
            ) AS token,
            s.address,
            ROW_NUMBER() OVER (
                PARTITION BY week
                ORDER BY
                    SUM(volume) DESC NULLS LAST
            ) AS position,
            sum(s.volume) / 2 AS volume
        FROM
            swaps s
        GROUP BY
            1,
            2,
            3
        ORDER BY
            1,
            3
    ) ranking
WHERE
    position <= 5