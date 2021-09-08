WITH revenues_volume AS (
    SELECT
        date_trunc('week', block_time) AS week,
        SUM(usd_amount * swap_fee) AS revenues,
        SUM(usd_amount) AS volume,
        COUNT(*) AS n_swaps
    FROM
        balancer.view_trades
    GROUP BY
        1
),
cumulative_metrics AS (
    SELECT
        week,
        n_swaps,
        volume,
        revenues,
        SUM(n_swaps) OVER (
            ORDER BY
                week
        ) AS cumulative_swaps,
        SUM(volume) OVER (
            ORDER BY
                week
        ) AS cumulative_volume,
        SUM(revenues) OVER (
            ORDER BY
                week
        ) AS cumulative_revenues
    FROM
        revenues_volume
)
SELECT
    *
FROM
    cumulative_metrics
ORDER BY
    1