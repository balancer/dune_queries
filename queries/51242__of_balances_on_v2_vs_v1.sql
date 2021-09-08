WITH tokens AS (
    SELECT
        'WETH' AS symbol,
        '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' :: bytea AS address
    UNION
    ALL
    SELECT
        'WBTC' AS symbol,
        '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599' :: bytea AS address
    UNION
    ALL
    SELECT
        'BAL' AS symbol,
        '\xba100000625a3754423978a60c9317c58a424e3d' :: bytea AS address
    UNION
    ALL
    SELECT
        'DAI' AS symbol,
        '\x6b175474e89094c44da98b954eedeac495271d0f' :: bytea AS address
)
SELECT
    DAY,
    CASE
        WHEN pool = '\xBA12222222228d8Ba445958a75a0704d566BF2C8' THEN 'V2'
        ELSE 'V1'
    END AS version,
    SUM(cumulative_amount) AS balance
FROM
    balancer."view_balances" VIEW
    INNER JOIN tokens ON tokens.symbol = '{{Token}}'
    AND tokens.address = VIEW.token
    AND DAY >= '2021-05-10'
GROUP BY
    1,
    2