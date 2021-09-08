WITH total_tvl AS (
    SELECT
        *
    FROM
        dune_user_generated.balancer_v1_view_liquidity
    UNION
    ALL
    SELECT
        *
    FROM
        dune_user_generated.balancer_v2_view_liquidity_draft
),
top_pools AS (
    SELECT
        pool_id,
        UPPER(pool_symbol) AS pool_symbol,
        SUM(usd_amount) AS tvl
    FROM
        total_tvl
    WHERE
        DAY = CURRENT_DATE - '1 day' :: INTERVAL
    GROUP BY
        1,
        2
    ORDER BY
        3 DESC NULLS LAST
    LIMIT
        5
)
SELECT
    DAY,
    COALESCE(t.pool_symbol, 'Others') AS symbol,
    SUM(usd_amount) AS tvl
FROM
    total_tvl l
    LEFT JOIN top_pools t ON t.pool_id = l.pool_id
GROUP BY
    1,
    2