SELECT
    date_trunc('week', d.block_time) AS week,
    SUM(usd_amount) AS volume,
    AVG(usd_amount) AS avg_trade,
    COUNT(DISTINCT trader_a) AS traders
FROM dex.trades d
WHERE project = 'Balancer'
AND ('{{4. Version}}' = 'Both' OR version = SUBSTRING('{{4. Version}}', 2))
AND ('{{1. Pool ID}}' = 'All' OR exchange_contract_address = CONCAT('\', SUBSTRING('{{1. Pool ID}}', 2))::bytea)
AND block_time >= '{{2. Start date}}'
AND block_time <= '{{3. End date}}'
GROUP BY 1