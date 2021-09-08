WITH swaps AS (
    SELECT
        date_trunc('hour', block_time) AS HOUR,
        exchange_contract_address AS pool,
        SUM(usd_amount) AS usd_amount
    FROM
        dex.trades
    WHERE
        project = 'Balancer'
        AND version = '1'
    GROUP BY
        1,
        2
)
SELECT
    HOUR,
    pool,
    SUM(usd_amount * "swapFee" / 1e18)
FROM
    swaps t
    INNER JOIN balancer."BPool_call_setSwapFee" s ON t.pool = s.contract_address
    AND t.hour = (
        SELECT
            date_trunc('hour', MAX(call_block_time))
        FROM
            balancer."BPool_call_setSwapFee"
        WHERE
            date_trunc('hour', call_block_time) <= t.hour
            AND s.contract_address = t.pool
            AND call_success
    )
GROUP BY
    1,
    2
ORDER BY
    1,
    2 DESC NULLS LAST