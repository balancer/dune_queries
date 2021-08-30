WITH pools AS (
        SELECT pool as pools
        FROM balancer."BFactory_evt_LOG_NEW_POOL"
        
        UNION ALL
        
        SELECT pool as pools
        FROM balancer_v2."WeightedPoolFactory_evt_PoolCreated"
              
        UNION ALL
        
        SELECT pool as pools
        FROM balancer_v2."WeightedPool2TokensFactory_evt_PoolCreated"
        
        UNION ALL

        SELECT pool as pools
        FROM balancer_v2."StablePoolFactory_evt_PoolCreated"
    ),
    
    joins AS (
        SELECT date_trunc('day', e.evt_block_time) AS day, e.dst AS lp, p.pools AS pool, SUM(e.amt)/1e18 AS amount
        FROM balancer."BPool_evt_Transfer" e
        INNER JOIN pools p ON e."contract_address" = p.pools
        WHERE e.dst NOT IN ('\x0000000000000000000000000000000000000000', '\x9424b1412450d0f8fc2255faf6046b98213b76bd')
        GROUP BY 1, 2, 3
        
        UNION ALL 
        
            SELECT date_trunc('day', e.evt_block_time) AS day, e."to" AS lp, p.pools AS pool, SUM(e.value)/1e18 AS amount
            FROM balancer_v2."WeightedPool_evt_Transfer" e
            INNER JOIN pools p ON e."contract_address" = p.pools
            WHERE e."to" != '\x0000000000000000000000000000000000000000'
            GROUP BY 1, 2, 3
/*        
        UNION ALL 
        
        SELECT date_trunc('day', e.evt_block_time) AS day, e."to" AS lp, p.pools AS pool, SUM(e.value)/1e18 AS amount
        FROM balancer_v2."WeightedPool2Tokens_evt_Transfer" e
        INNER JOIN pools p ON e."contract_address" = p.pools
        WHERE e."to" != '\x0000000000000000000000000000000000000000'
        GROUP BY 1, 2, 3 */
                
        UNION ALL
        
        SELECT date_trunc('day', e.evt_block_time) AS day, e."to" AS lp, p.pools AS pool, SUM(e.value)/1e18 AS amount
        FROM balancer_v2."StablePool_evt_Transfer" e
        INNER JOIN pools p ON e."contract_address" = p.pools
        WHERE e."to" != '\x0000000000000000000000000000000000000000'
        GROUP BY 1, 2, 3
    ),
    
    exits AS (
        SELECT date_trunc('day', e.evt_block_time) AS day, e.src AS lp, p.pools AS pool, -SUM(e.amt)/1e18 AS amount
        FROM balancer."BPool_evt_Transfer" e
        INNER JOIN pools p ON e."contract_address" = p.pools
        WHERE e.src NOT IN ('\x0000000000000000000000000000000000000000', '\x9424b1412450d0f8fc2255faf6046b98213b76bd')
        GROUP BY 1, 2, 3
        
        UNION ALL 
        
        SELECT date_trunc('day', e.evt_block_time) AS day, e."from" AS lp, p.pools AS pool, -SUM(e.value)/1e18 AS amount
        FROM balancer_v2."WeightedPool_evt_Transfer" e
        INNER JOIN pools p ON e."contract_address" = p.pools
        WHERE e."from" != '\x0000000000000000000000000000000000000000'
        GROUP BY 1, 2, 3
/*        
        UNION ALL 
        
        SELECT date_trunc('day', e.evt_block_time) AS day, e."from" AS lp, p.pools AS pool, -SUM(e.value)/1e18 AS amount
        FROM balancer_v2."WeightedPool2Tokens_evt_Transfer" e
        INNER JOIN pools p ON e."contract_address" = p.pools
        WHERE e."from" != '\x0000000000000000000000000000000000000000'
        GROUP BY 1, 2, 3 */
        
                
        UNION ALL
        
        SELECT date_trunc('day', e.evt_block_time) AS day, e."from" AS lp, p.pools AS pool, -SUM(e.value)/1e18 AS amount
        FROM balancer_v2."StablePool_evt_Transfer" e
        INNER JOIN pools p ON e."contract_address" = p.pools
        WHERE e."from" != '\x0000000000000000000000000000000000000000'
        GROUP BY 1, 2, 3
    ),
    
    daily_delta_bpt_by_pool AS (
        SELECT day, lp, pool, SUM(COALESCE(amount, 0)) as amount FROM 
        (SELECT *
        FROM joins j 
        UNION ALL
        SELECT * 
        FROM exits e) foo
        GROUP BY 1, 2, 3
    ),
    
    cumulative_bpt_by_pool AS (
        SELECT day, lp, pool, amount, 
        LEAD(day::timestamp, 1, CURRENT_DATE::timestamp) OVER (PARTITION BY lp, pool ORDER BY day) AS next_day,
        SUM(amount) OVER (PARTITION BY lp, pool ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS amount_bpt
        FROM daily_delta_bpt_by_pool
    ),
    
    calendar AS (
        SELECT generate_series('2020-03-31'::timestamp, CURRENT_DATE, '1 day'::interval) AS day
    ),
    
   running_cumulative_bpt_by_pool as (
        SELECT c.day, lp, pool, amount_bpt
        FROM cumulative_bpt_by_pool b
        JOIN calendar c on b.day <= c.day AND c.day < b.next_day
        WHERE '{{1. Pool ID}}' = 'All' OR pool = CONCAT('\', SUBSTRING('{{1. Pool ID}}', 2, 41))::bytea
    )
    
SELECT COUNT(DISTINCT lp) AS lps
FROM running_cumulative_bpt_by_pool
WHERE day = CURRENT_DATE - '1 day'::interval
AND amount_bpt > 0