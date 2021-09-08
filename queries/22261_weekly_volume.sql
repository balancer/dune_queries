-- Volume per week
-- Visualization: bar chart
WITH swaps AS (
    SELECT
        date_trunc('week', block_time) AS week,
        COUNT(*) AS txns,
        SUM(usd_amount) AS volume
    FROM
        dex."trades"
    WHERE
        project = 'Balancer'
        AND (
            '{{4. Version}}' = 'Both'
            OR version = SUBSTRING('{{4. Version}}', 2)
        )
        AND block_time >= '{{2. Start date}}'
        AND block_time <= '{{3. End date}}'
    GROUP BY
        1
)
SELECT
    week,
    txns AS "Swaps",
    volume AS "Volume",
    volume / txns AS "Avg. Volume per Swap"
FROM
    swaps