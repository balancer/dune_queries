WITH swaps AS (
    SELECT
        date_trunc('week', d.block_time) AS week,
        t.symbol AS token,
        SUM(usd_amount) AS volume
    FROM
        dex."trades" d
        LEFT JOIN erc20.tokens t ON d.token_b_address = t.contract_address
    WHERE
        project = 'Balancer'
        AND block_time >= '{{2. Start date}}'
        AND block_time <= '{{3. End date}}'
    GROUP BY
        1,
        2
),
total_volume AS (
    SELECT
        week,
        'Total' AS token,
        SUM(volume) AS volume
    FROM
        swaps
    GROUP BY
        1
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
        week = date_trunc('week', CURRENT_DATE - INTERVAL '1 week')
    GROUP BY
        1
)
SELECT
    s.week,
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
UNION
ALL
SELECT
    *
FROM
    total_volume