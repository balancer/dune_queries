WITH swap_fees AS (
    SELECT
        date_trunc('minute', evt_block_time) AS minute,
        "swapFeePercentage"/1e18 AS swap_fee,
        contract_address AS pool
    FROM balancer_v2."WeightedPool_evt_SwapFeePercentageChanged"
    WHERE contract_address = SUBSTRING(CONCAT('\', SUBSTRING('{{1. Pool ID}}', 2))::bytea, 0, 21)
    
    UNION ALL
    
    SELECT
        date_trunc('minute', evt_block_time) AS minute,
        "swapFeePercentage"/1e18 AS swap_fee,
        contract_address AS pool
    FROM balancer_v2."StablePool_evt_SwapFeePercentageChanged"
    WHERE contract_address = SUBSTRING(CONCAT('\', SUBSTRING('{{1. Pool ID}}', 2))::bytea, 0, 21)
),

swap_fee_with_gaps AS(
    SELECT 
        minute,
        swap_fee,
        LEAD(minute, 1, now()) OVER (ORDER BY minute) AS next_minute,
        pool
    FROM swap_fees
),

calendar AS (
    SELECT generate_series(MIN(minute), now(), interval '1 minute') AS minute
    FROM swap_fees
)

SELECT
    c.minute, swap_fee, pool
FROM calendar c
LEFT JOIN swap_fee_with_gaps f ON f.minute <= c.minute AND c.minute < f.next_minute
WHERE c.minute >= '{{2. Start date}}' AND c.minute <= '{{3. End date}}'