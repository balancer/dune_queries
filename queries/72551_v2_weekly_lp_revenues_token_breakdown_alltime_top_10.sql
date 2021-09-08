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
),
token_revenues AS (
    SELECT
        date_trunc('week', block_time) AS week,
        s.token_address,
        token,
        SUM(volume * swap_fee) / 2 AS revenues
    FROM
        swaps s
    GROUP BY
        1,
        2,
        3
),
ranking AS (
    SELECT
        token_address,
        ROW_NUMBER() OVER (
            ORDER BY
                SUM(revenues) DESC NULLS LAST
        ) AS position
    FROM
        token_revenues
    GROUP BY
        1
)
SELECT
    week,
    t.token_address,
    COALESCE(token, t.token_address :: text) AS token,
    revenues
FROM
    token_revenues t
    LEFT JOIN ranking r ON t.token_address = r.token_address
WHERE
    position <= 10