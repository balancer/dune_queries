WITH prices AS (
    SELECT
        date_trunc('day', MINUTE) AS DAY,
        contract_address AS token,
        decimals,
        symbol,
        AVG(price) AS price
    FROM
        prices.usd
    GROUP BY
        1,
        2,
        3,
        4
),
purchases AS (
    SELECT
        s."tokenOut" AS token_address,
        p2.symbol AS token_symbol,
        COUNT(*) AS n_purchases,
        SUM(COALESCE(("amountOut" / 10 ^ p2.decimals))) AS amount_bought,
        SUM(
            COALESCE(
                ("amountIn" / 10 ^ p1.decimals) * p1.price,
                ("amountOut" / 10 ^ p2.decimals) * p2.price
            )
        ) AS volume_bought
    FROM
        balancer_v2."Vault_evt_Swap" s
        LEFT JOIN prices p1 ON p1.day = date_trunc('day', evt_block_time)
        AND p1.token = s."tokenIn"
        LEFT JOIN prices p2 ON p2.day = date_trunc('day', evt_block_time)
        AND p2.token = s."tokenOut"
    GROUP BY
        1,
        2
),
sales AS (
    SELECT
        s."tokenIn" AS token_address,
        p1.symbol AS token_symbol,
        COUNT(*) AS n_sales,
        SUM(COALESCE(("amountIn" / 10 ^ p1.decimals))) AS amount_sold,
        SUM(
            COALESCE(
                ("amountIn" / 10 ^ p1.decimals) * p1.price,
                ("amountOut" / 10 ^ p2.decimals) * p2.price
            )
        ) AS volume_sold
    FROM
        balancer_v2."Vault_evt_Swap" s
        LEFT JOIN dune_user_generated.prices_usd p1 ON p1.minute = date_trunc('day', evt_block_time)
        AND p1.contract_address = s."tokenIn"
        LEFT JOIN dune_user_generated.prices_usd p2 ON p2.minute = date_trunc('day', evt_block_time)
        AND p2.contract_address = s."tokenOut"
    GROUP BY
        1,
        2
)
SELECT
    p.token_symbol AS token,
    n_purchases,
    amount_bought,
    n_sales,
    volume_bought,
    amount_sold,
    volume_sold
FROM
    purchases p
    JOIN sales s ON s.token_address = p.token_address
    AND s.token_symbol = p.token_symbol
ORDER BY
    volume_sold DESC NULLS LAST