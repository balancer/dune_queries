DROP TABLE IF EXISTS dune_user_generated.balancer_arb_bots;

CREATE TABLE dune_user_generated.balancer_arb_bots (
    address bytea,
    author text,
    name text,
    TYPE text
);

WITH balancer_trades AS (
    SELECT
        evt_tx_hash AS tx_hash,
        "tokenOut" token_a_address,
        "tokenIn" token_b_address
    FROM
        balancer_v2."Vault_evt_Swap"
    WHERE
        evt_block_time >= NOW() - INTERVAL '7d'
),
sushi_trades AS (
    SELECT
        t.evt_tx_hash AS tx_hash,
        CASE
            WHEN "amount0Out" = 0 THEN f.token1
            ELSE f.token0
        END AS token_a_address,
        CASE
            WHEN "amount0In" = 0 THEN f.token1
            ELSE f.token0
        END AS token_b_address,
        CASE
            WHEN "amount0Out" = 0 THEN "amount1Out"
            ELSE "amount0Out"
        END AS token_a_amount_raw,
        CASE
            WHEN "amount0In" = 0 THEN "amount1In"
            ELSE "amount0In"
        END AS token_b_amount_raw
    FROM
        sushi."UniswapV2Pair_evt_Swap" t
        INNER JOIN sushi."UniswapV2Factory_evt_PairCreated" f ON f.pair = t.contract_address
    WHERE
        t.evt_block_time >= NOW() - INTERVAL '7d'
),
quick_trades AS (
    SELECT
        t.evt_tx_hash AS tx_hash,
        CASE
            WHEN "amount0Out" = 0 THEN f.token1
            ELSE f.token0
        END AS token_a_address,
        CASE
            WHEN "amount0In" = 0 THEN f.token1
            ELSE f.token0
        END AS token_b_address,
        CASE
            WHEN "amount0Out" = 0 THEN "amount1Out"
            ELSE "amount0Out"
        END AS token_a_amount_raw,
        CASE
            WHEN "amount0In" = 0 THEN "amount1In"
            ELSE "amount0In"
        END AS token_b_amount_raw
    FROM
        quickswap."UniswapV2Pair_evt_Swap" t
        INNER JOIN quickswap."UniswapV2Factory_evt_PairCreated" f ON f.pair = t.contract_address
    WHERE
        t.evt_block_time >= NOW() - INTERVAL '7d'
),
arbs AS (
    SELECT
        DISTINCT(t.to) AS address,
        'arbitrage bot' AS name,
        'dapp usage' AS TYPE,
        'balancerlabs' AS author
    FROM
        balancer_trades t1
        INNER JOIN sushi_trades t2 ON t1.tx_hash = t2.tx_hash
        AND t1.token_a_address = t2.token_b_address
        AND t1.token_b_address = t2.token_a_address
        INNER JOIN polygon.transactions t ON t.hash = t1.tx_hash
        AND t.block_time >= NOW() - INTERVAL '7d'
    UNION
    ALL
    SELECT
        DISTINCT(t.to) AS address,
        'arbitrage bot' AS name,
        'dapp usage' AS TYPE,
        'balancerlabs' AS author
    FROM
        balancer_trades t1
        INNER JOIN quick_trades t2 ON t1.tx_hash = t2.tx_hash
        AND t1.token_a_address = t2.token_b_address
        AND t1.token_b_address = t2.token_a_address
        INNER JOIN polygon.transactions t ON t.hash = t1.tx_hash
        AND t.block_time >= NOW() - INTERVAL '7d'
)
INSERT INTO
    dune_user_generated.balancer_arb_bots
SELECT
    DISTINCT address,
    author,
    name,
    TYPE
FROM
    arbs