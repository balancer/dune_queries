WITH prices AS (
    SELECT
        date_trunc('day', MINUTE) AS DAY,
        AVG(price) AS price
    FROM
        prices.layer1_usd_eth
    GROUP BY
        1
)
SELECT
    date_trunc('day', s.block_time) AS DAY,
    MIN(t.gas_price * t.gas_used * 1e -18 * p.price) AS "Min",
    MAX(t.gas_price * t.gas_used * 1e -18 * p.price) AS "Max",
    AVG(t.gas_price * t.gas_used * 1e -18 * p.price) AS "Avg"
FROM
    dex.trades s
    LEFT JOIN ethereum."transactions" t ON t.hash = s.tx_hash
    AND t.block_time > NOW() - INTERVAL '14d'
    LEFT JOIN prices p ON p.day = date_trunc('day', s.block_time)
WHERE
    s.block_time > NOW() - INTERVAL '14d'
    AND project = 'Balancer'
    AND version = '2'
GROUP BY
    1