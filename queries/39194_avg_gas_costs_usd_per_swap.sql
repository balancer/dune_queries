WITH prices AS (
        SELECT date_trunc('day', minute) AS day, AVG(price) AS price 
        FROM prices.layer1_usd_eth
        GROUP BY 1
    )

SELECT date_trunc('day', s.block_time) AS day, CONCAT('V', version) AS version, AVG(t.gas_price * t.gas_used * 1e-18 * p.price) AS cost
FROM dex.trades s
LEFT JOIN ethereum."transactions" t ON t.hash = s.tx_hash AND t.block_time > now() - interval '14d'
LEFT JOIN prices p ON p.day = date_trunc('day', s.block_time)
WHERE s.block_time > now() - interval '14d'
AND project = 'Balancer'
GROUP BY 1, 2