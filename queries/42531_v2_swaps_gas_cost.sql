WITH txns AS (
    SELECT
        DISTINCT hash,
        tx.gas_used
    FROM
        balancer_v2."Vault_evt_Swap" s
        INNER JOIN ethereum.transactions tx ON s.evt_tx_hash = tx.hash
    WHERE
        tx.block_time > (CURRENT_DATE - '1 day' :: INTERVAL)
        AND s.evt_block_time > (CURRENT_DATE - '1 day' :: INTERVAL)
        AND tx."to" = '\xba12222222228d8ba445958a75a0704d566bf2c8'
),
swaps AS (
    SELECT
        evt_tx_hash,
        count(1) AS n_swaps
    FROM
        balancer_v2."Vault_evt_Swap" s
    WHERE
        s.evt_block_time > (CURRENT_DATE - '1 day' :: INTERVAL)
    GROUP BY
        1
),
gas_per_swap AS (
    SELECT
        txns.hash,
        gas_used,
        n_swaps,
        gas_used / n_swaps AS gas_per_swap
    FROM
        txns
        INNER JOIN swaps ON swaps.evt_tx_hash = txns.hash
    WHERE
        gas_used < 1000000
),
percentiles AS (
    SELECT
        PERCENTILE_CONT(0.01) WITHIN GROUP (
            ORDER BY
                gas_per_swap
        ) AS p01_gas_per_swap,
        PERCENTILE_CONT(0.05) WITHIN GROUP (
            ORDER BY
                gas_per_swap
        ) AS p05_gas_per_swap,
        PERCENTILE_CONT(0.1) WITHIN GROUP (
            ORDER BY
                gas_per_swap
        ) AS p10_gas_per_swap,
        PERCENTILE_CONT(0.25) WITHIN GROUP (
            ORDER BY
                gas_per_swap
        ) AS p25_gas_per_swap,
        PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY
                gas_per_swap
        ) AS p50_gas_per_swap,
        PERCENTILE_CONT(0.75) WITHIN GROUP (
            ORDER BY
                gas_per_swap
        ) AS p75_gas_per_swap,
        PERCENTILE_CONT(0.90) WITHIN GROUP (
            ORDER BY
                gas_per_swap
        ) AS p90_gas_per_swap
    FROM
        gas_per_swap
)
SELECT
    *,
    CASE
        WHEN gas_per_swap <= (
            SELECT
                p01_gas_per_swap
            FROM
                percentiles
        ) THEN 'p01'
        WHEN gas_per_swap BETWEEN (
            SELECT
                p01_gas_per_swap
            FROM
                percentiles
        )
        AND (
            SELECT
                p05_gas_per_swap
            FROM
                percentiles
        ) THEN 'p05'
        WHEN gas_per_swap BETWEEN (
            SELECT
                p05_gas_per_swap
            FROM
                percentiles
        )
        AND (
            SELECT
                p10_gas_per_swap
            FROM
                percentiles
        ) THEN 'p10'
        WHEN gas_per_swap BETWEEN (
            SELECT
                p10_gas_per_swap
            FROM
                percentiles
        )
        AND (
            SELECT
                p25_gas_per_swap
            FROM
                percentiles
        ) THEN 'p25'
        WHEN gas_per_swap BETWEEN (
            SELECT
                p25_gas_per_swap
            FROM
                percentiles
        )
        AND (
            SELECT
                p50_gas_per_swap
            FROM
                percentiles
        ) THEN 'p50'
        WHEN gas_per_swap BETWEEN (
            SELECT
                p50_gas_per_swap
            FROM
                percentiles
        )
        AND (
            SELECT
                p75_gas_per_swap
            FROM
                percentiles
        ) THEN 'p75'
        WHEN gas_per_swap BETWEEN (
            SELECT
                p75_gas_per_swap
            FROM
                percentiles
        )
        AND (
            SELECT
                p90_gas_per_swap
            FROM
                percentiles
        ) THEN 'p90'
        WHEN gas_per_swap >= (
            SELECT
                p90_gas_per_swap
            FROM
                percentiles
        ) THEN 'p100'
    END AS percentile
FROM
    gas_per_swap