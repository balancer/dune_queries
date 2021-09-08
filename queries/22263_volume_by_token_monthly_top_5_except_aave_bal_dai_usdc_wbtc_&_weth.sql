-- Volume (token breakdown without AAVE, BAL, DAI, USDC, WBTC & WETH) per month (full)
-- Visualization: bar chart (stacked)
WITH swaps AS (
    SELECT
        date_trunc('month', d.block_time) AS MONTH,
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
        AND d.token_b_address NOT IN (
            '\x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9',
            '\xba100000625a3754423978a60c9317c58a424e3d',
            '\x6b175474e89094c44da98b954eedeac495271d0f',
            '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
        )
        AND block_time >= '{{2. Start date}}'
        AND block_time <= '{{3. End date}}'
    GROUP BY
        1,
        3,
        4
    UNION
    ALL
    SELECT
        date_trunc('month', d.block_time) AS MONTH,
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
        AND d.token_a_address NOT IN (
            '\x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9',
            '\xba100000625a3754423978a60c9317c58a424e3d',
            '\x6b175474e89094c44da98b954eedeac495271d0f',
            '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
        )
        AND block_time >= '{{2. Start date}}'
        AND block_time <= '{{3. End date}}'
    GROUP BY
        1,
        3,
        4
)
SELECT
    *
FROM
    (
        SELECT
            s.month,
            COALESCE(
                s.token,
                CONCAT(SUBSTRING(s.address :: text, 3, 6), '...')
            ) AS token,
            s.address,
            ROW_NUMBER() OVER (
                PARTITION BY MONTH
                ORDER BY
                    SUM(volume) DESC NULLS LAST
            ) AS position,
            sum(s.volume) / 2 AS volume
        FROM
            swaps s
        GROUP BY
            1,
            2,
            3
        ORDER BY
            1,
            3
    ) ranking
WHERE
    position <= 5