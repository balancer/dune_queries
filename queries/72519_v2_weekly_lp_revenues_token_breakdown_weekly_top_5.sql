WITH swaps AS (
    SELECT
        block_time,
        token_address,
        pool_address,
        token,
        volume,
        COALESCE(s1."swapFeePercentage", s2."swapFeePercentage") / 1e18 AS swap_fee
    FROM
        (
            SELECT
                d.block_time,
                d.token_b_address AS token_address,
                SUBSTRING(exchange_contract_address :: text, 0, 43) AS pool_address,
                t.symbol AS token,
                usd_amount AS volume
            FROM
                dex.trades d
                LEFT JOIN erc20.tokens t ON t.contract_address = d.token_b_address
            WHERE
                project = 'Balancer'
                AND version = '2'
            UNION
            ALL
            SELECT
                d.block_time,
                d.token_a_address AS token_address,
                SUBSTRING(exchange_contract_address :: text, 0, 43) AS pool_address,
                t.symbol AS token,
                usd_amount AS volume
            FROM
                dex.trades d
                LEFT JOIN erc20.tokens t ON t.contract_address = d.token_a_address
            WHERE
                project = 'Balancer'
                AND version = '2'
        ) t
        LEFT JOIN balancer_v2."WeightedPool_evt_SwapFeePercentageChanged" s1 ON s1.contract_address :: text = pool_address
        AND s1.evt_block_time = (
            SELECT
                MAX(evt_block_time)
            FROM
                balancer_v2."WeightedPool_evt_SwapFeePercentageChanged"
            WHERE
                evt_block_time <= t.block_time
                AND contract_address :: text = pool_address
        )
        LEFT JOIN balancer_v2."StablePool_evt_SwapFeePercentageChanged" s2 ON s2.contract_address :: text = pool_address
        AND s2.evt_block_time = (
            SELECT
                MAX(evt_block_time)
            FROM
                balancer_v2."StablePool_evt_SwapFeePercentageChanged"
            WHERE
                evt_block_time <= t.block_time
                AND contract_address :: text = pool_address
        )
)
SELECT
    *
FROM
(
        SELECT
            date_trunc('week', block_time) AS week,
            s.token_address,
            COALESCE(token, s.token_address :: text) AS token,
            SUM(volume * swap_fee) / 2 AS revenues,
            ROW_NUMBER() OVER (
                PARTITION BY date_trunc('week', block_time)
                ORDER BY
                    SUM(volume * swap_fee) / 2 DESC NULLS LAST
            ) AS position
        FROM
            swaps s
        GROUP BY
            1,
            2,
            3
        ORDER BY
            1,
            2,
            3
    ) ranking
WHERE
    position <= 5