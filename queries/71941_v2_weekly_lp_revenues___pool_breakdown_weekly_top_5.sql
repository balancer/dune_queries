WITH labels AS (
    SELECT * FROM (SELECT
            address::text,
            name,
            ROW_NUMBER() OVER (PARTITION BY address::text ORDER BY MAX(updated_at) DESC) AS num
        FROM labels.labels
        WHERE "type" = 'balancer_v2_pool'
        GROUP BY 1, 2) l
        WHERE num = 1
    ),
    
    swaps AS (
        SELECT 
            block_time,
            SUBSTRING(exchange_contract_address::text, 0, 43) AS address,
            usd_amount,
            COALESCE(s1."swapFeePercentage", s2."swapFeePercentage")/1e18 AS swap_fee
        FROM dex.trades t
        LEFT JOIN balancer_v2."WeightedPool_evt_SwapFeePercentageChanged" s1 ON s1.contract_address = SUBSTRING(exchange_contract_address, 0, 21)
        AND s1.evt_block_time = (
            SELECT MAX(evt_block_time)
            FROM balancer_v2."WeightedPool_evt_SwapFeePercentageChanged"
            WHERE evt_block_time <= t.block_time
            AND contract_address = SUBSTRING(exchange_contract_address, 0, 21)
        )
        LEFT JOIN balancer_v2."StablePool_evt_SwapFeePercentageChanged" s2 ON s2.contract_address = SUBSTRING(exchange_contract_address, 0, 21)
        AND s2.evt_block_time = (
            SELECT MAX(evt_block_time)
            FROM balancer_v2."StablePool_evt_SwapFeePercentageChanged"
            WHERE evt_block_time <= t.block_time
            AND contract_address = SUBSTRING(exchange_contract_address, 0, 21)
        )
        WHERE project = 'Balancer'
        AND version = '2'
    )

SELECT * FROM (
    SELECT
        date_trunc('week', block_time) AS week,
        s.address,
        COALESCE(CONCAT(SUBSTRING(UPPER(l.name), 0, 15), '(', SUBSTRING(s.address, 3, 8), ')'), s.address) AS pool,
        ROW_NUMBER() OVER (PARTITION BY date_trunc('week', block_time) ORDER BY SUM(usd_amount*swap_fee) DESC NULLS LAST) AS position,
        SUM(usd_amount * swap_fee) AS revenues
    FROM swaps s
    LEFT JOIN labels l ON l.address = s.address
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
) ranking
WHERE position <= 5