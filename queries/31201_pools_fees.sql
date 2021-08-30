WITH fees_v1 AS (
        SELECT f."swapFee"/1e16 AS "swapFee"
        FROM balancer."BPool_call_setSwapFee" f
        WHERE call_success = 'true'
    ),

    fees_v2 AS (
        SELECT "swapFeePercentage"/1e16 FROM
        balancer_v2."WeightedPool_evt_SwapFeePercentageChanged" a
        INNER JOIN (SELECT contract_address, MAX(evt_block_time) AS evt_block_time
        FROM balancer_v2."WeightedPool_evt_SwapFeePercentageChanged"
        GROUP BY 1) b ON a.contract_address = b.contract_address AND a.evt_block_time = b.evt_block_time
        
        UNION ALL
        
        SELECT "swapFeePercentage"/1e16 FROM
        balancer_v2."StablePool_evt_SwapFeePercentageChanged" a
        INNER JOIN (SELECT contract_address, MAX(evt_block_time) AS evt_block_time
        FROM balancer_v2."StablePool_evt_SwapFeePercentageChanged"
        GROUP BY 1) b ON a.contract_address = b.contract_address AND a.evt_block_time = b.evt_block_time
    ),
    
    fees AS (
        SELECT * FROM fees_v1
        UNION ALL
        SELECT * FROM fees_v2
)

SELECT
    CASE 
        WHEN ("swapFee") BETWEEN 0 AND 0.25 THEN '< 0.25' 
        WHEN ("swapFee") BETWEEN 0.25 AND 0.5 THEN '< 0.50' 
        WHEN ("swapFee") BETWEEN 0.5 AND 1 THEN '< 1' 
        WHEN ("swapFee") BETWEEN 1 AND 5 THEN '< 5' 
        WHEN ("swapFee") BETWEEN 5 AND 10 THEN '< 10' 
    END AS "Fee",
    COUNT("swapFee") AS "Pools"
FROM fees
GROUP BY 1
ORDER BY 2 DESC