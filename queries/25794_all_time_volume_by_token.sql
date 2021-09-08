-- Volume (token breakdown) per hour (last 24 hours)
-- Visualization: bar chart (stacked)
WITH swaps AS (
    SELECT
        sum(usd_amount) AS volume,
        d.token_b_address AS address,
        t.symbol AS token
    FROM
        dex.trades d
        LEFT JOIN erc20.tokens t ON t.contract_address = d.token_b_address
    WHERE
        project = 'Balancer'
        AND (
            '{{4. Version}}' = 'Both'
            OR version = SUBSTRING('{{4. Version}}', 2)
        )
    GROUP BY
        2,
        3
    UNION
    ALL
    SELECT
        sum(usd_amount) AS volume,
        d.token_a_address AS address,
        t.symbol AS token
    FROM
        dex.trades d
        LEFT JOIN erc20.tokens t ON t.contract_address = d.token_a_address
    WHERE
        project = 'Balancer'
        AND (
            '{{4. Version}}' = 'Both'
            OR version = SUBSTRING('{{4. Version}}', 2)
        )
    GROUP BY
        2,
        3
)
SELECT
    COALESCE(
        s.token,
        CONCAT(SUBSTRING(s.address :: text, 3, 6), '...')
    ) AS token,
    CONCAT(
        '<a target="_blank" href="https://etherscan.io/token/0',
        SUBSTRING(s.address :: text, 2, 42),
        '">0',
        SUBSTRING(s.address :: text, 2, 42),
        '</a>'
    ) AS address,
    sum(s.volume) / 2 AS volume
FROM
    swaps s
GROUP BY
    1,
    2
ORDER BY
    3 DESC NULLS LAST