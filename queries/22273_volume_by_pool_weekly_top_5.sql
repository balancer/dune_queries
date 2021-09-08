-- Volume (pool breakdown) per week
-- Visualization: bar chart (stacked)
WITH swaps AS (
    SELECT
        date_trunc('week', d.block_time) AS week,
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
        AND (
            '{{1. Pool ID}}' = 'All'
            OR exchange_contract_address = CONCAT(
                '\', SUBSTRING(' { { 1.Pool ID } } ', 2))::bytea)
        AND block_time >= ' { { 2.START date } } '
        AND block_time <= ' { { 3.
            END date } } '
        GROUP BY 1, 3
    ),

    labels AS (
    SELECT * FROM (SELECT
            address,
            name,
            ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
        FROM labels.labels
        WHERE "type" IN (' balancer_pool ', ' balancer_v2_pool ')
        GROUP BY 1, 2) l
        WHERE num = 1
)

SELECT * FROM (
    SELECT
        s.address,
        CONCAT(SUBSTRING(UPPER(l.name), 0, 15), ' (', SUBSTRING(s.address::text, 3, 8), ') ') AS pool,
        week,
        s.traders,
        ROW_NUMBER() OVER (PARTITION BY week ORDER BY SUM(volume) DESC NULLS LAST) AS position,
        ROUND(sum(s.volume), 2) AS volume
    FROM swaps s
    LEFT JOIN labels l ON SUBSTRING(s.address::text, 0, 43)::bytea = l.address
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3, 4
) ranking
WHERE position <= 5
AND volume > 0