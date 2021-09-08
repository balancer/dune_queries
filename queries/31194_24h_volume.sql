WITH swaps AS (
    SELECT
        usd_amount AS usd_amount
    FROM
        dex.trades
    WHERE
        project = 'Balancer'
        AND block_time > NOW() - INTERVAL '24h'
)
SELECT
    SUM(usd_amount) AS usd_amount
FROM
    swaps