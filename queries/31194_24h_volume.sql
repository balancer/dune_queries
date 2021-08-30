WITH swaps AS (
        SELECT usd_amount AS usd_amount
        FROM dex.trades
        WHERE project = 'Balancer' AND block_time > now() - interval '24h'
    )

SELECT SUM(usd_amount) AS usd_amount
FROM swaps