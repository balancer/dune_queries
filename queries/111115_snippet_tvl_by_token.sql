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
last_tvl_by_token AS (
    SELECT
        token_address,
        token_symbol,
        SUM(usd_amount) AS tvl
    FROM
        total_tvl
    WHERE
        DAY = CURRENT_DATE - '1 day' :: INTERVAL
    GROUP BY
        1,
        2
),
top_tokens AS (
    SELECT
        token_address,
        token_symbol,
        tvl
    FROM
        last_tvl_by_token
    ORDER BY
        3 DESC NULLS LAST
    LIMIT
        5
)
SELECT
    DAY,
    COALESCE(t.token_symbol, 'Others') AS symbol,
    SUM(usd_amount) AS tvl
FROM
    total_tvl l
    LEFT JOIN top_tokens t ON t.token_address = l.token_address
GROUP BY
    1,
    2