WITH swap_fees AS (
        SELECT 
            contract_address::text AS address, 
            LAST_VALUE("swapFee") OVER (PARTITION BY contract_address ORDER BY call_block_time RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)/1e18 AS swap_fee
        FROM balancer."BPool_call_setSwapFee"
        WHERE call_success
        
        UNION ALL
        
        SELECT 
            contract_address::text AS address, 
            LAST_VALUE("swapFeePercentage") OVER (PARTITION BY contract_address ORDER BY evt_block_time RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)/1e18 AS swap_fee
        FROM balancer_v2."WeightedPool_evt_SwapFeePercentageChanged"
    ),
    
    labels AS (
    SELECT * FROM (SELECT
            address::text,
            name,
            ROW_NUMBER() OVER (PARTITION BY address::text ORDER BY MAX(updated_at) DESC) AS num
        FROM labels.labels
        WHERE "type" IN ('balancer_pool', 'balancer_v2_pool')
        GROUP BY 1, 2) l
        WHERE num = 1
    ),
    
    swaps AS (
        SELECT 
            date_trunc('week', block_time) AS week,
            SUBSTRING(exchange_contract_address::text, 0, 43) AS address,
            version,
            usd_amount
        FROM dex.trades
        WHERE project = 'Balancer'
        AND ('{{Version}}' = 'Both' OR SUBSTRING('{{Version}}', 2) = version)
    )

SELECT * FROM (
    SELECT
        week,
        CONCAT('V', version) AS version,
        s.address,
        swap_fee,
        COALESCE(CONCAT(SUBSTRING(UPPER(l.name), 0, 15), ' (V', version, ')', ' (', SUBSTRING(s.address, 3, 8), ')'), s.address) AS pool,
        ROW_NUMBER() OVER (PARTITION BY week ORDER BY SUM(usd_amount) DESC NULLS LAST) AS position,
        SUM(usd_amount * swap_fee) AS revenues
    FROM swaps s
    INNER JOIN swap_fees f ON f.address = s.address
    LEFT JOIN labels l ON l.address = s.address
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY 1, 2, 3, 4, 5
) ranking
WHERE position <= 5