SELECT
  SUM(usd_amount) AS usd_amount
FROM
  dex.trades
WHERE
  project = 'Balancer'
  AND block_time > NOW() - INTERVAL '7d'