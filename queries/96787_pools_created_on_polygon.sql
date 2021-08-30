WITH pools AS (
        SELECT pool
        FROM balancer_v2."LiquidityBootstrappingPoolFactory_evt_PoolCreated"
        
        UNION ALL
        
        SELECT pool
        FROM balancer_v2."WeightedPoolFactory_evt_PoolCreated"
        
        UNION ALL
        
        SELECT pool
        FROM balancer_v2."WeightedPool2TokensFactory_evt_PoolCreated"
        
        UNION ALL
        
        SELECT pool
        FROM balancer_v2."StablePoolFactory_evt_PoolCreated"
    )

SELECT COUNT(pool) AS n_pools 
FROM pools