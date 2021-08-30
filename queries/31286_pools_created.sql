WITH core_pools AS (
        SELECT date_trunc('week', evt_block_time) AS week,
               COUNT(contract_address) AS pools
        FROM balancer."BFactory_evt_LOG_NEW_POOL"
        GROUP BY week
    ),
    
    smart_pools AS (
       SELECT date_trunc('week', evt_block_time) AS week,
           COUNT(contract_address) AS pools
        FROM balancer."CRPFactory_evt_LogNewCrp"
        GROUP BY week 
    ),
    
    weighted_pools AS (
        SELECT date_trunc('week', evt_block_time) AS week,
           COUNT(contract_address) AS pools
        FROM balancer_v2."WeightedPoolFactory_evt_PoolCreated"
        GROUP BY week
    ),
    
    weighted_2tokens_pools AS (
        SELECT date_trunc('week', evt_block_time) AS week,
           COUNT(contract_address) AS pools
        FROM balancer_v2."WeightedPool2TokensFactory_evt_PoolCreated"
        GROUP BY week
    ),
    
    stable_pools AS (
        SELECT date_trunc('week', evt_block_time) AS week,
            COUNT(contract_address) AS pools
        FROM balancer_v2."StablePoolFactory_evt_PoolCreated"
        GROUP BY week
    
    )
    
    
SELECT 
    c.week, 
    (c.pools - COALESCE(s.pools, 0)) AS "Core (V1)", 
    COALESCE(s.pools, 0) AS "Smart (V1)",
    COALESCE(w.pools, 0) AS "Weighted (V2)",
    COALESCE(w2.pools, 0) AS "2 Tokens (V2)",
    COALESCE(st.pools, 0) AS "Stable (V2)"
FROM core_pools c
FULL OUTER JOIN smart_pools s ON s.week = c.week
FULL OUTER JOIN weighted_pools w ON w.week = c.week
FULL OUTER JOIN weighted_2tokens_pools w2 ON w2.week = c.week
FULL OUTER JOIN stable_pools st ON st.week = c.week
WHERE (c.week >= '{{2. Start date}}' AND c.week <= '{{3. End date}}')
OR (s.week >= '{{2. Start date}}' AND s.week <= '{{3. End date}}')
OR (w.week >= '{{2. Start date}}' AND w.week <= '{{3. End date}}')
OR (w2.week >= '{{2. Start date}}' AND w2.week <= '{{3. End date}}')
OR (st.week >= '{{2. Start date}}' AND st.week <= '{{3. End date}}')
