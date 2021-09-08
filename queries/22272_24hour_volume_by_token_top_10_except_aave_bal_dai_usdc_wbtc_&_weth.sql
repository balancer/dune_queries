-- Volume (token breakdown without AAVE, BAL, DAI, USDC, WBTC & WETH) per hour (last 24 hours)
-- Visualization: bar chart (stacked)
WITH swaps AS (
    SELECT
        date_trunc('hour', block_time) AS HOUR,
        sum(usd_amount) AS volume,
        d.token_b_address AS address,
        t.symbol AS token
    FROM
        dex.trades d
        LEFT JOIN erc20.tokens t ON t.contract_address = d.token_b_address
    WHERE
        project = 'Balancer'
        AND date_trunc('hour', block_time) > date_trunc('hour', NOW() - INTERVAL '1 day')
        AND (
            '{{4. Version}}' = 'Both'
            OR version = SUBSTRING('{{4. Version}}', 2)
        )
        AND d.token_b_address NOT IN (
            '\x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9',
            '\xba100000625a3754423978a60c9317c58a424e3d',
            '\x6b175474e89094c44da98b954eedeac495271d0f',
            '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
        )
    GROUP BY
        1,
        3,
        4
    UNION
    ALL
    SELECT
        date_trunc('hour', block_time) AS HOUR,
        sum(usd_amount) AS volume,
        d.token_a_address AS address,
        t.symbol AS token
    FROM
        dex.trades d
        LEFT JOIN erc20.tokens t ON t.contract_address = d.token_a_address
    WHERE
        project = 'Balancer'
        AND date_trunc('hour', block_time) > date_trunc('hour', NOW() - INTERVAL '1 day')
        AND (
            '{{4. Version}}' = 'Both'
            OR version = SUBSTRING('{{4. Version}}', 2)
        )
        AND d.token_a_address NOT IN (
            '\x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9',
            '\xba100000625a3754423978a60c9317c58a424e3d',
            '\x6b175474e89094c44da98b954eedeac495271d0f',
            '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
        )
    GROUP BY
        1,
        3,
        4
),
ranking AS (
    SELECT
        token,
        address,
        sum(volume) / 2,
        ROW_NUMBER() OVER (
            ORDER BY
                sum(volume) DESC NULLS LAST
        ) AS position
    FROM
        swaps
    GROUP BY
        1,
        2
)
SELECT
    s.hour,
    sum(s.volume) / 2 AS volume,
    s.address,
    COALESCE(
        s.token,
        CONCAT(SUBSTRING(s.address :: text, 3, 6), '...')
    ) AS token
FROM
    swaps s
    LEFT JOIN ranking r ON r.address = s.address
WHERE
    r.position <= 10
GROUP BY
    1,
    3,
    4