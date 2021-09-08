WITH weighted_pools AS (
    SELECT
        date_trunc('week', evt_block_time) AS week,
        COUNT(contract_address) AS pools
    FROM
        balancer_v2."WeightedPoolFactory_evt_PoolCreated"
    GROUP BY
        week
),
weighted_2tokens_pools AS (
    SELECT
        date_trunc('week', evt_block_time) AS week,
        COUNT(contract_address) AS pools
    FROM
        balancer_v2."WeightedPool2TokensFactory_evt_PoolCreated"
    GROUP BY
        week
),
stable_pools AS (
    SELECT
        date_trunc('week', evt_block_time) AS week,
        COUNT(contract_address) AS pools
    FROM
        balancer_v2."StablePoolFactory_evt_PoolCreated"
    GROUP BY
        week
)
SELECT
    w.week,
    COALESCE(w.pools, 0) AS "Weighted (V2)",
    COALESCE(w2.pools, 0) AS "2 Tokens (V2)",
    COALESCE(st.pools, 0) AS "Stable (V2)"
FROM
    weighted_pools w FULL
    OUTER JOIN weighted_2tokens_pools w2 ON w2.week = w.week FULL
    OUTER JOIN stable_pools st ON st.week = w.week
WHERE
    (
        w.week >= '{{2. Start date}}'
        AND w.week <= '{{3. End date}}'
    )
    OR (
        w2.week >= '{{2. Start date}}'
        AND w2.week <= '{{3. End date}}'
    )
    OR (
        st.week >= '{{2. Start date}}'
        AND st.week <= '{{3. End date}}'
    )