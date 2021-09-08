DROP TABLE IF EXISTS dune_user_generated.balancer_v2_view_liquidity;

CREATE TABLE dune_user_generated.balancer_v2_view_liquidity (
    DAY timestamp,
    pool_id bytea,
    pool_symbol text,
    token_address bytea,
    token_symbol text,
    usd_amount numeric
);

WITH pool_labels AS (
    SELECT
        address AS pool_id,
        name AS pool_symbol
    FROM
        (
            SELECT
                address,
                name,
                ROW_NUMBER() OVER (
                    PARTITION BY address
                    ORDER BY
                        MAX(updated_at) DESC
                ) AS num
            FROM
                labels.labels
            WHERE
                "type" IN ('balancer_v2_pool')
            GROUP BY
                1,
                2
        ) l
    WHERE
        num = 1
),
prices AS (
    SELECT
        date_trunc('day', MINUTE) AS DAY,
        contract_address AS token,
        AVG(price) AS price
    FROM
        prices.usd
    GROUP BY
        1,
        2
),
dex_prices_1 AS (
    SELECT
        date_trunc('day', HOUR) AS DAY,
        contract_address AS token,
        (
            PERCENTILE_DISC(0.5) WITHIN GROUP (
                ORDER BY
                    median_price
            )
        ) AS price,
        SUM(sample_size) AS sample_size
    FROM
        dex.view_token_prices
    GROUP BY
        1,
        2
    HAVING
        sum(sample_size) > 3
),
dex_prices AS (
    SELECT
        *,
        LEAD(DAY, 1, NOW()) OVER (
            PARTITION BY token
            ORDER BY
                DAY
        ) AS day_of_next_change
    FROM
        dex_prices_1
),
swaps_changes AS (
    SELECT
        DAY,
        pool,
        token,
        SUM(COALESCE(delta, 0)) AS delta
    FROM
        (
            SELECT
                date_trunc('day', evt_block_time) AS DAY,
                "poolId" AS pool,
                "tokenIn" AS token,
                "amountIn" AS delta
            FROM
                balancer_v2."Vault_evt_Swap"
            UNION
            ALL
            SELECT
                date_trunc('day', evt_block_time) AS DAY,
                "poolId" AS pool,
                "tokenOut" AS token,
                - "amountOut" AS delta
            FROM
                balancer_v2."Vault_evt_Swap"
        ) swaps
    GROUP BY
        1,
        2,
        3
),
balances_changes AS (
    SELECT
        date_trunc('day', evt_block_time) AS DAY,
        "poolId" AS pool,
        UNNEST(tokens) AS token,
        UNNEST(deltas) AS delta
    FROM
        balancer_v2."Vault_evt_PoolBalanceChanged"
),
internal_changes AS (
    SELECT
        date_trunc('day', evt_block_time) AS DAY,
        NULL :: bytea AS pool,
        token,
        SUM(COALESCE(delta, 0)) AS delta
    FROM
        balancer_v2."Vault_evt_InternalBalanceChanged"
    GROUP BY
        1,
        2,
        3
),
managed_changes AS (
    SELECT
        date_trunc('day', evt_block_time) AS DAY,
        "poolId" AS pool,
        token,
        "cashDelta" + "managedDelta" AS delta
    FROM
        balancer_v2."Vault_evt_PoolBalanceManaged"
),
daily_delta_balance AS (
    SELECT
        DAY,
        pool,
        token,
        SUM(COALESCE(amount, 0)) AS amount
    FROM
        (
            SELECT
                DAY,
                pool,
                token,
                SUM(COALESCE(delta, 0)) AS amount
            FROM
                balances_changes
            GROUP BY
                1,
                2,
                3
            UNION
            ALL
            SELECT
                DAY,
                pool,
                token,
                delta AS amount
            FROM
                swaps_changes
            UNION
            ALL
            SELECT
                DAY,
                pool,
                token,
                delta AS amount
            FROM
                internal_changes
            UNION
            ALL
            SELECT
                DAY,
                pool,
                token,
                delta AS amount
            FROM
                managed_changes
        ) balance
    GROUP BY
        1,
        2,
        3
),
cumulative_balance AS (
    SELECT
        DAY,
        pool,
        token,
        LEAD(DAY, 1, NOW()) OVER (
            PARTITION BY token,
            pool
            ORDER BY
                DAY
        ) AS day_of_next_change,
        SUM(amount) OVER (
            PARTITION BY pool,
            token
            ORDER BY
                DAY ROWS BETWEEN UNBOUNDED PRECEDING
                AND CURRENT ROW
        ) AS cumulative_amount
    FROM
        daily_delta_balance
),
weekly_delta_balance_by_token AS (
    SELECT
        DAY,
        pool,
        token,
        cumulative_amount,
        (
            cumulative_amount - COALESCE(
                LAG(cumulative_amount, 1) OVER (
                    PARTITION BY pool,
                    token
                    ORDER BY
                        DAY
                ),
                0
            )
        ) AS amount
    FROM
        (
            SELECT
                DAY,
                pool,
                token,
                SUM(cumulative_amount) AS cumulative_amount
            FROM
                cumulative_balance b
            WHERE
                extract(
                    dow
                    FROM
                        DAY
                ) = 1
            GROUP BY
                1,
                2,
                3
        ) foo
),
calendar AS (
    SELECT
        generate_series(
            '2021-04-21' :: timestamp,
            CURRENT_DATE,
            '1 day' :: INTERVAL
        ) AS DAY
),
cumulative_usd_balance AS (
    SELECT
        c.day,
        b.pool,
        b.token,
        cumulative_amount / 10 ^ t.decimals * COALESCE(p1.price, p2.price, 0) AS amount_usd
    FROM
        calendar c
        LEFT JOIN cumulative_balance b ON b.day <= c.day
        AND c.day < b.day_of_next_change
        LEFT JOIN erc20.tokens t ON t.contract_address = b.token
        LEFT JOIN prices p1 ON p1.day = b.day
        AND p1.token = b.token
        LEFT JOIN dex_prices p2 ON p2.day <= c.day
        AND c.day < p2.day_of_next_change
        AND p2.token = b.token
),
pools_tokens_weights AS (
    SELECT
        c."poolId" AS pool_id,
        unnest(cc.tokens) AS token_address,
        unnest(cc.weights) / 1e18 AS normalized_weight
    FROM
        balancer_v2."Vault_evt_PoolRegistered" c
        INNER JOIN balancer_v2."WeightedPoolFactory_call_create" cc ON c.evt_tx_hash = cc.call_tx_hash
    UNION
    ALL
    SELECT
        c."poolId" AS pool_id,
        unnest(cc.tokens) AS token_address,
        unnest(cc.weights) / 1e18 AS normalized_weight
    FROM
        balancer_v2."Vault_evt_PoolRegistered" c
        INNER JOIN balancer_v2."WeightedPool2TokensFactory_call_create" cc ON c.evt_tx_hash = cc.call_tx_hash
),
pool_liquidity_estimates AS (
    SELECT
        b.day,
        b.pool,
        SUM(b.amount_usd) / COALESCE(SUM(w.normalized_weight), 1) AS liquidity
    FROM
        cumulative_usd_balance b
        LEFT JOIN pools_tokens_weights w ON b.pool = w.pool_id
        AND b.token = w.token_address
    GROUP BY
        1,
        2
),
balancer_liquidity AS (
    SELECT
        b.day,
        b.pool,
        pool_symbol,
        token AS token_address,
        symbol AS token_symbol,
        coalesce(amount_usd, liquidity * normalized_weight) AS usd_amount
    FROM
        pool_liquidity_estimates b
        LEFT JOIN cumulative_usd_balance c ON c.day = b.day
        AND c.pool = b.pool
        LEFT JOIN pools_tokens_weights w ON b.pool = w.pool_id
        AND w.token_address = c.token
        LEFT JOIN erc20.tokens t ON t.contract_address = c.token
        LEFT JOIN pool_labels p ON p.pool_id = SUBSTRING(b.pool :: text, 0, 43) :: bytea
)
INSERT INTO
    dune_user_generated.balancer_v2_view_liquidity
SELECT
    *
FROM
    balancer_liquidity