-- Volume (source breakdown) per day (last 7 days)
-- Visualization: bar chart (stacked)
WITH prices AS (
    SELECT
        date_trunc('day', MINUTE) AS DAY,
        contract_address AS token,
        decimals,
        AVG(price) AS price
    FROM
        prices.usd
    WHERE
        date_trunc('day', MINUTE) > date_trunc('day', NOW() - INTERVAL '1 week')
    GROUP BY
        1,
        2,
        3
),
swaps AS (
    SELECT
        date_trunc('day', d.block_time) AS DAY,
        tx_to AS channel,
        COUNT(*) AS txns,
        sum(usd_amount) AS volume
    FROM
        dex.trades d
    WHERE
        project = 'Balancer'
        AND date_trunc('day', d.block_time) > NOW() - INTERVAL '1 week'
        AND (
            '{{4. Version}}' = 'Both'
            OR version = SUBSTRING('{{4. Version}}', 2)
        )
    GROUP BY
        1,
        2
),
labels AS (
    SELECT
        address,
        name
    FROM
        labels.labels
    WHERE
        (
            "type" = 'balancer_source'
            OR "name" = 'arbitrage bot'
        )
        AND "author" = 'balancerlabs'
),
distinct_labels AS (
    SELECT
        *
    FROM
        (
            SELECT
                address,
                name,
                COUNT(*)
            FROM
                labels
            GROUP BY
                address,
                name
            HAVING
                COUNT(*) > 1
        ) d
    WHERE
        "name" <> 'arbitrage bot'
    UNION
    ALL
    SELECT
        address,
        name,
        COUNT(*)
    FROM
        labels
    GROUP BY
        address,
        name
    HAVING
        COUNT(*) = 1
),
channels AS (
    SELECT
        channel,
        sum(coalesce(volume, 00)) AS volume
    FROM
        swaps
    GROUP BY
        1
),
trade_count AS (
    SELECT
        date_trunc('day', block_time) AS DAY,
        tx_to AS channel,
        COUNT(1) AS daily_trades
    FROM
        dex.trades
    WHERE
        trader_a != '\x0000000000000000000000000000000000000000'
        AND project = 'Balancer'
    GROUP BY
        1,
        2
),
heavy_traders AS (
    SELECT
        channel,
        DAY,
        daily_trades
    FROM
        trade_count
    WHERE
        daily_trades >= 100
),
channel_classifier AS (
    SELECT
        c.channel,
        CASE
            WHEN l.name IS NOT NULL THEN l.name
            WHEN f.pool IS NOT NULL THEN 'BPool direct'
            WHEN c.channel IN (
                SELECT
                    channel
                FROM
                    heavy_traders
            ) THEN 'heavy trader'
            WHEN c.channel IN (
                SELECT
                    channel
                FROM
                    channels
                WHERE
                    volume IS NOT NULL
                ORDER BY
                    volume DESC
                LIMIT
                    10
            ) THEN concat('0x', encode(c.channel, 'hex'))
            ELSE 'others'
        END AS class
    FROM
        channels c
        LEFT JOIN balancer."BFactory_evt_LOG_NEW_POOL" f ON f.pool = c.channel
        LEFT JOIN labels l ON l.address = c.channel
)
SELECT
    DAY,
    s.channel,
    CONCAT(
        '<a target="_blank" href="https://etherscan.io/address/0',
        SUBSTRING(s.channel :: text, 2, 42),
        '">',
        'https://etherscan.io/address/0',
        SUBSTRING(s.channel :: text, 2, 42),
        '</a>'
    ) AS etherscan,
    s.txns AS trades,
    sum(volume) AS "Volume"
FROM
    swaps s
    INNER JOIN channel_classifier c ON s.channel = c.channel
WHERE
    c.class = 'heavy trader'
GROUP BY
    1,
    2,
    3,
    4
ORDER BY
    DAY DESC,
    "Volume" DESC