WITH fees_v1 AS (
    SELECT
        f."swapFee" / 1e16 AS fee
    FROM
        balancer."BPool_evt_LOG_SWAP" b
        LEFT JOIN balancer."BPool_call_setSwapFee" f ON f.contract_address = b.contract_address
),
fees_v2 AS (
    SELECT
        LAST_VALUE("swapFeePercentage" / 1e16) OVER (
            PARTITION BY contract_address
            ORDER BY
                evt_block_time
        ) AS fee
    FROM
        balancer_v2."WeightedPool_evt_SwapFeePercentageChanged"
        /*
         UNION ALL
         
         SELECT LAST_VALUE("swapFeePercentage"/1e16) OVER (PARTITION BY contract_address ORDER BY evt_block_time) AS fee
         FROM balancer_v2."WeightedPool2Tokens_evt_SwapFeePercentageChanged"*/
    UNION
    ALL
    SELECT
        LAST_VALUE("swapFeePercentage" / 1e16) OVER (
            PARTITION BY contract_address
            ORDER BY
                evt_block_time
        ) AS fee
    FROM
        balancer_v2."StablePool_evt_SwapFeePercentageChanged"
),
fees AS (
    SELECT
        *
    FROM
        fees_v1
    UNION
    ALL
    SELECT
        *
    FROM
        fees_v2
)
SELECT
    AVG(fee) AS avgfee
FROM
    fees