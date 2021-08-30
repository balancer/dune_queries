WITH v1 AS (
        SELECT contract_address AS pool
        FROM balancer."BFactory_evt_LOG_NEW_POOL"
    ),
    
    v2 AS (
        SELECT pool
        FROM balancer_v2."WeightedPoolFactory_evt_PoolCreated"
        
        UNION ALL
        
        SELECT pool
        FROM balancer_v2."WeightedPool2TokensFactory_evt_PoolCreated"
        
        UNION ALL
        
        SELECT pool
        FROM balancer_v2."StablePoolFactory_evt_PoolCreated"
    )

SELECT COUNT(pool) AS pools FROM (
SELECT pool FROM v1
UNION ALL
SELECT pool FROM v2
) pools