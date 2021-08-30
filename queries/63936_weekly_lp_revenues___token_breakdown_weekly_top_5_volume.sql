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
    
    swaps AS (
        SELECT
            date_trunc('week', d.block_time) AS week,
            d.token_b_address AS token_address,
            SUBSTRING(exchange_contract_address::text, 0, 43) AS pool_address,
            t.symbol AS token,
            sum(usd_amount)/2 AS volume
        FROM dex.trades d
        LEFT JOIN erc20.tokens t ON t.contract_address = d.token_b_address
        WHERE project = 'Balancer'
        AND ('{{Version}}' = 'Both' OR SUBSTRING('{{Version}}', 2) = version)
        GROUP BY 1,2,3,4
        
        UNION ALL
        
        SELECT
            date_trunc('week', d.block_time) AS week,
            d.token_a_address AS token_address,
            SUBSTRING(exchange_contract_address::text, 0, 43) AS pool_address,
            t.symbol AS token,
            sum(usd_amount)/2 AS volume
        FROM dex.trades d
        LEFT JOIN erc20.tokens t ON t.contract_address = d.token_a_address
        WHERE project = 'Balancer'
        AND ('{{Version}}' = 'Both' OR SUBSTRING('{{Version}}', 2) = version)
        GROUP BY 1,2,3,4
    )

SELECT * FROM(
    SELECT
        week,
        s.token_address,
        COALESCE(token, s.token_address::text) AS token,
        SUM(volume * swap_fee) AS revenues,
        ROW_NUMBER() OVER (PARTITION BY week ORDER BY SUM(volume) DESC NULLS LAST) AS position
    FROM swaps s
    INNER JOIN swap_fees f ON f.address = s.pool_address
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
    ) ranking
    WHERE position <= 5