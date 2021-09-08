WITH swap_fees AS (
    SELECT
        contract_address AS address,
        LAST_VALUE("swapFee") OVER (
            PARTITION BY contract_address
            ORDER BY
                call_block_time RANGE BETWEEN UNBOUNDED PRECEDING
                AND UNBOUNDED FOLLOWING
        ) / 1e18 AS swap_fee
    FROM
        balancer."BPool_call_setSwapFee"
    WHERE
        call_success
    UNION
    ALL
    SELECT
        contract_address AS address,
        LAST_VALUE("swapFeePercentage") OVER (
            PARTITION BY contract_address
            ORDER BY
                evt_block_time RANGE BETWEEN UNBOUNDED PRECEDING
                AND UNBOUNDED FOLLOWING
        ) / 1e18 AS swap_fee
    FROM
        balancer_v2."WeightedPool_evt_SwapFeePercentageChanged"
),
swaps AS (
    SELECT
        date_trunc('week', block_time) AS week,
        SUBSTRING(exchange_contract_address :: text, 0, 43) AS address,
        version,
        usd_amount
    FROM
        dex.trades
    WHERE
        project = 'Balancer'
)
SELECT
    week,
    CONCAT('V', version) AS version,
    SUM(usd_amount * swap_fee) AS revenues
FROM
    swaps s
    INNER JOIN swap_fees f ON s.address = f.address :: text
GROUP BY
    1,
    2