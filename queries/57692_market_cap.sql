WITH daily_minted AS (
    SELECT
        date_trunc('day', evt_block_time) AS DAY,
        SUM(value / 1e18) AS value
    FROM
        erc20."ERC20_evt_Transfer"
    WHERE
        contract_address = '\xba100000625a3754423978a60c9317c58a424e3d'
        AND "from" = '\x0000000000000000000000000000000000000000'
    GROUP BY
        1
),
cumulative_minted AS (
    SELECT
        DAY,
        LEAD(DAY, 1, NOW()) OVER (
            ORDER BY
                DAY
        ) AS day_of_next_change,
        SUM(value) OVER (
            ORDER BY
                DAY
        ) AS cumulative_value
    FROM
        daily_minted
),
calendar AS (
    SELECT
        generate_series(
            '2020-09-01' :: timestamp,
            CURRENT_DATE,
            '1 day' :: INTERVAL
        ) AS DAY
),
supply AS (
    SELECT
        c.day,
        cumulative_value AS total_supply
    FROM
        calendar c
        LEFT JOIN cumulative_minted m ON m.day <= c.day
        AND c.day < m.day_of_next_change
)
SELECT
    p.day,
    price,
    total_supply,
    price * total_supply AS market_cap
FROM
    (
        SELECT
            date_trunc('day', MINUTE) AS DAY,
            AVG(price) AS price
        FROM
            prices.usd
        WHERE
            contract_address = '\xba100000625a3754423978a60c9317c58a424e3d'
        GROUP BY
            1
        ORDER BY
            1 DESC
    ) p
    JOIN supply s ON p.day = s.day
ORDER BY
    1 DESC