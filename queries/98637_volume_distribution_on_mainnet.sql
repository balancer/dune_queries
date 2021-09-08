WITH swaps AS (
    SELECT
        usd_amount
    FROM
        dex.trades
    WHERE
        project = 'Balancer'
        AND block_time >= '{{2. Start date}}'
        AND block_time <= '{{3. End date}}'
)
SELECT
    CASE
        WHEN (usd_amount) BETWEEN 0
        AND 100 THEN '< 100'
        WHEN (usd_amount) BETWEEN 100
        AND 1000 THEN '< 1K'
        WHEN (usd_amount) BETWEEN 1000
        AND 10000 THEN '< 10K'
        WHEN (usd_amount) BETWEEN 10000
        AND 100000 THEN '< 100K'
        WHEN (usd_amount) BETWEEN 100000
        AND 1000000 THEN '< 1M'
    END AS volume,
    CASE
        WHEN (usd_amount) BETWEEN 0
        AND 100 THEN '1'
        WHEN (usd_amount) BETWEEN 100
        AND 1000 THEN '2'
        WHEN (usd_amount) BETWEEN 1000
        AND 10000 THEN '3'
        WHEN (usd_amount) BETWEEN 10000
        AND 100000 THEN '4'
        WHEN (usd_amount) BETWEEN 100000
        AND 1000000 THEN '5'
    END AS n,
    COUNT(usd_amount) AS n_trades
FROM
    swaps
WHERE
    usd_amount < 1000000
GROUP BY
    1,
    2
ORDER BY
    2