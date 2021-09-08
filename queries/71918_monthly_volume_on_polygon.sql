WITH prices AS (
    SELECT
        date_trunc('day', MINUTE) AS DAY,
        contract_address AS token,
        decimals,
        AVG(price) AS price
    FROM
        prices.usd
    GROUP BY
        1,
        2,
        3
),
swaps AS (
    SELECT
        date_trunc('month', evt_block_time) AS MONTH,
        SUM(
            COALESCE(
                ("amountIn" / 10 ^ p1.decimals) * p1.price,
                ("amountOut" / 10 ^ p2.decimals) * p2.price
            )
        ) AS usd_amount,
        COUNT(*) AS n_trades
    FROM
        balancer_v2."Vault_evt_Swap" s
        LEFT JOIN prices p1 ON p1.day = date_trunc('day', evt_block_time)
        AND p1.token = s."tokenIn"
        LEFT JOIN prices p2 ON p2.day = date_trunc('day', evt_block_time)
        AND p2.token = s."tokenOut"
    GROUP BY
        1
)
SELECT
    *
FROM
    swaps