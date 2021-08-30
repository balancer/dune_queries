WITH mainnet_rewards AS (
        SELECT date_trunc('week', day) AS week, SUM(amount) AS amount, SUM(usd_amount) AS usd_amount
        FROM dune_user_generated.balancer_liquidity_mining
        WHERE ('{{1. Pool ID}}' = 'All' OR pool_id = CONCAT('\', SUBSTRING('{{1. Pool ID}}', 2))::bytea)
        AND chain_id = '1'
        GROUP BY 1
    ),
    
    swaps AS (
        SELECT
            date_trunc('week', d.block_time) AS week,
            SUM(usd_amount*swap_fee) AS revenues,
            SUM(usd_amount) AS volume
        FROM balancer.view_trades d
        WHERE version = '2'
        AND ('{{1. Pool ID}}' = 'All' OR exchange_contract_address = CONCAT('\', SUBSTRING('{{1. Pool ID}}', 2))::bytea)
        GROUP BY 1
    )
    
SELECT s.week, COALESCE(amount, 0) AS amount, s.revenues, s.volume, s.revenues/r.usd_amount AS revenues_ratio, s.volume/r.usd_amount AS volume_ratio
FROM swaps s 
LEFT JOIN mainnet_rewards r ON r.week = s.week
WHERE s.week >= '{{2. Start date}}'
AND s.week <= '{{3. End date}}'
ORDER BY 1