WITH swaps AS (
    SELECT
        date_trunc('month', d.block_time) AS MONTH,
        t.symbol AS token,
        SUM(usd_amount) AS volume
    FROM
        dex."trades" d
        LEFT JOIN erc20.tokens t ON d.token_b_address = t.contract_address
    WHERE
        project = 'Balancer'
    GROUP BY
        1,
        2
),
ranking AS (
    SELECT
        token,
        ROW_NUMBER() OVER (
            ORDER BY
                SUM(volume) DESC NULLS LAST
        ) AS position
    FROM
        swaps
    WHERE
        MONTH = date_trunc('month', CURRENT_DATE - INTERVAL '1' MONTH)
    GROUP BY
        1
)
SELECT
    s.month,
    CASE
        WHEN r.position <= 7 THEN s.token
        ELSE 'Others'
    END AS token,
    SUM(s.volume) AS volume
FROM
    swaps s
    LEFT JOIN ranking r ON r.token = s.token
GROUP BY
    1,
    2
ORDER BY
    1,
    2