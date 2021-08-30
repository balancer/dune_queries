-- Number of new/old Liquidity Providers per week
-- Visualization: bar chart (stacked)

SELECT
    ssq.time, 
    new_users as "New",
    (unique_users - new_users) as "Old"
FROM (
    SELECT
        sq.time, 
        COUNT(*) as new_users
    FROM (
        SELECT
            caller AS unique_users,
            MIN(date_trunc('week', evt_block_time)) AS time
        FROM balancer."BPool_evt_LOG_JOIN"
        GROUP BY 1
        UNION ALL
        SELECT
            "liquidityProvider" AS unique_users,
            MIN(date_trunc('week', evt_block_time)) AS time
        FROM balancer_v2."Vault_evt_PoolBalanceChanged"
        GROUP BY 1
  ) sq
    GROUP BY 1
    ORDER BY 1
) ssq
LEFT JOIN (
    SELECT
        date_trunc('week', evt_block_time) AS time,
        COUNT(DISTINCT caller) AS unique_users
        FROM (SELECT caller, evt_block_time FROM balancer."BPool_evt_LOG_JOIN"
        UNION ALL
        SELECT "liquidityProvider" AS caller, evt_block_time FROM balancer_v2."Vault_evt_PoolBalanceChanged") foo
    GROUP BY 1
    ORDER BY 1
) t2 ON t2.time = ssq.time
WHERE (ssq.time >= '{{2. Start date}}' AND ssq.time <= '{{3. End date}}')
OR (t2.time>= '{{2. Start date}}' AND t2.time <= '{{3. End date}}')
ORDER BY 1 DESC