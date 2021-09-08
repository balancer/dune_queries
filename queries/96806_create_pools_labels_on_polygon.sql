DROP TABLE IF EXISTS dune_user_generated.balancer_pools;

CREATE TABLE dune_user_generated.balancer_pools (
    address bytea,
    label text,
    TYPE text,
    author text
);

WITH erc20_tokens AS (
    SELECT
        DISTINCT contract_address,
        symbol,
        decimals
    FROM
        dune_user_generated.prices_usd
),
pools AS (
    SELECT
        c."poolId" AS pool_id,
        unnest(cc.tokens) AS token_address,
        unnest(cc.weights) / 1e18 AS normalized_weight,
        cc.symbol,
        'WP' AS pool_type
    FROM
        balancer_v2."Vault_evt_PoolRegistered" c
        INNER JOIN balancer_v2."WeightedPoolFactory_call_create" cc ON c.evt_tx_hash = cc.call_tx_hash
    UNION
    ALL
    SELECT
        c."poolId" AS pool_id,
        unnest(cc.tokens) AS token_address,
        unnest(cc.weights) / 1e18 AS normalized_weight,
        cc.symbol,
        'WP2T' AS pool_type
    FROM
        balancer_v2."Vault_evt_PoolRegistered" c
        INNER JOIN balancer_v2."WeightedPool2TokensFactory_call_create" cc ON c.evt_tx_hash = cc.call_tx_hash
    UNION
    ALL
    SELECT
        c."poolId" AS pool_id,
        unnest(cc.tokens) AS token_address,
        0 AS normalized_weight,
        cc.symbol,
        'SP' AS pool_type
    FROM
        balancer_v2."Vault_evt_PoolRegistered" c
        INNER JOIN balancer_v2."StablePoolFactory_call_create" cc ON c.evt_tx_hash = cc.call_tx_hash
    UNION
    ALL
    SELECT
        c."poolId" AS pool_id,
        unnest(cc.tokens) AS token_address,
        0 AS normalized_weight,
        cc.symbol,
        'LBP' AS pool_type
    FROM
        balancer_v2."Vault_evt_PoolRegistered" c
        INNER JOIN balancer_v2."LiquidityBootstrappingPoolFactory_call_create" cc ON c.evt_tx_hash = cc.call_tx_hash
),
settings AS (
    SELECT
        pool_id,
        coalesce(t.symbol, '?') AS token_symbol,
        normalized_weight,
        p.symbol AS pool_symbol,
        p.pool_type
    FROM
        pools p
        LEFT JOIN erc20_tokens t ON p.token_address = t.contract_address
)
INSERT INTO
    dune_user_generated.balancer_pools
SELECT
    SUBSTRING(pool_id FOR 20) AS address,
    CASE
        WHEN pool_type IN ('SP', 'LBP') THEN lower(pool_symbol)
        ELSE lower(
            CONCAT(
                string_agg(token_symbol, '/'),
                ' ',
                string_agg(cast(norm_weight AS text), '/')
            )
        )
    END AS label,
    'balancer_v2_pool' AS TYPE,
    'balancerlabs' AS author
FROM
    (
        SELECT
            s1.pool_id,
            token_symbol,
            pool_symbol,
            cast(100 * normalized_weight AS integer) AS norm_weight,
            pool_type
        FROM
            settings s1
        ORDER BY
            1 ASC,
            3 DESC,
            2 ASC
    ) s
GROUP BY
    pool_id,
    pool_symbol,
    pool_type;