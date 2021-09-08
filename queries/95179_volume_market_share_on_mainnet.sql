SELECT
    date_trunc('week', block_time) AS week,
    CASE
        WHEN project = 'Uniswap'
        AND version = '3' THEN 'Uniswap V3'
        WHEN project = 'Uniswap'
        AND version != '3' THEN 'Uniswap V1+V2'
        ELSE project
    END AS project,
    SUM(usd_amount) AS usd_volume,
    COUNT(*) AS n_trades
FROM
    dex."trades" t
WHERE
    block_time > NOW() - INTERVAL '30d'
    AND block_time <= '{{3. End date}}'
    AND project IN (
        'Balancer',
        'Curve',
        'Uniswap',
        'Sushiswap',
        'Bancor Network'
    )
GROUP BY
    1,
    2
ORDER BY
    3,
    2