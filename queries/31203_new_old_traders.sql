SELECT
    ssq.time, 
    new_users as "New",
    (unique_users - new_users) as "Old"
FROM (
    SELECT
        sq.time, 
        COUNT(*) AS new_users
    FROM (
        SELECT 
            tx_from as unique_users,
            CASE WHEN '{{Aggregation}}' = 'Daily' THEN MIN(date_trunc('day', block_time))
            ELSE MIN(date_trunc('week', block_time)) END AS time
        FROM dex.trades
        WHERE project = 'Balancer'
        GROUP BY 1
        ORDER BY 1
    ) sq
    GROUP BY 1
) ssq
LEFT JOIN (
        SELECT 
            CASE WHEN '{{Aggregation}}' = 'Daily' THEN date_trunc('day', block_time)
            ELSE date_trunc('week', block_time) END AS time,
            COUNT(DISTINCT tx_from) AS unique_users
        FROM dex.trades
        WHERE project = 'Balancer' 
        GROUP BY 1
        ORDER BY 1
) t2 ON t2.time = ssq.time
ORDER BY 1 DESC
