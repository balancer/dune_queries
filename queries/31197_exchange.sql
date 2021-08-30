WITH swaps AS (
        SELECT  
            CASE WHEN '{{Aggregation}}' = 'Daily' THEN date_trunc('day', block_time)
            WHEN '{{Aggregation}}' = 'Weekly' THEN date_trunc('week', block_time)
            END AS "date",
            CONCAT('V', version) AS version,
            COUNT(*) AS txns,
            SUM(usd_amount) AS volume
        FROM dex."trades"
        WHERE project = 'Balancer'
        GROUP BY 1, 2
    )

SELECT 
    "date", 
    version,
    txns AS "Swaps", 
    volume AS "Volume",
    volume/txns AS "Avg. Volume per Swap"
FROM swaps