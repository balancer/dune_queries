-- Volume per hour
-- Visualization: bar chart

WITH swaps AS (
        SELECT  
            date_trunc('hour', block_time) AS hour,
            COUNT(*) AS txns,
            SUM(usd_amount) AS volume
        FROM dex."trades"
        WHERE project = 'Balancer'
        AND block_time >= now() - interval '7 days'
        AND ('{{4. Version}}' = 'Both' OR version = SUBSTRING('{{4. Version}}', 2))
        GROUP BY 1
    )

SELECT
    hour,
    txns AS "Swaps",
    volume AS "Volume",
    volume/txns AS "Avg. Volume per Swap"
FROM swaps
