-- Volume (source breakdown) per week
-- Visualization: bar chart (stacked)
WITH prices AS (
    SELECT
        date_trunc('day', MINUTE) AS DAY,
        contract_address AS token,
        decimals,
        AVG(price) AS price
    FROM
        prices.usd
    GROUP BY
        1,
        2,
        3
),
swaps AS (
    SELECT
        date_trunc('day', evt_block_time) AS DAY,
        t.to AS channel,
        COUNT(*) AS txns,
        SUM(
            COALESCE(
                ("amountIn" / 10 ^ p1.decimals) * p1.price,
                ("amountOut" / 10 ^ p2.decimals) * p2.price
            )
        ) AS volume
    FROM
        balancer_v2."Vault_evt_Swap" s
        JOIN polygon.transactions t ON t.hash = s.evt_tx_hash
        AND t.block_time > NOW() - INTERVAL '7d'
        LEFT JOIN prices p1 ON p1.day = date_trunc('day', evt_block_time)
        AND p1.token = s."tokenIn"
        LEFT JOIN prices p2 ON p2.day = date_trunc('day', evt_block_time)
        AND p2.token = s."tokenOut"
    GROUP BY
        1,
        2
),
manual_labels AS (
    SELECT
        address,
        name
    FROM
        dune_user_generated.balancer_manual_labels
    WHERE
        "type" = 'balancer_source'
        AND "author" = 'balancerlabs'
),
arb_bots AS (
    SELECT
        address,
        name
    FROM
        dune_user_generated.balancer_arb_bots
    WHERE
        "name" = 'arbitrage bot'
        AND "author" = 'balancerlabs'
        AND address NOT IN (
            SELECT
                address
            FROM
                manual_labels
        )
),
distinct_labels AS (
    SELECT
        *
    FROM
        manual_labels
    UNION
    ALL
    SELECT
        *
    FROM
        arb_bots
),
channels AS (
    SELECT
        channel,
        SUM(COALESCE(volume, 00)) AS volume
    FROM
        swaps
    GROUP BY
        1
),
heavy_traders AS (
    SELECT
        channel,
        DAY,
        txns AS daily_trades
    FROM
        swaps
    WHERE
        txns >= 100
),
channel_classifier AS (
    SELECT
        c.channel,
        l.name,
        CASE
            WHEN l.name IS NOT NULL THEN l.name
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
            ) THEN CONCAT(
                SUBSTRING(concat('0x', encode(c.channel, 'hex')), 0, 13),
                '...'
            )
            ELSE 'others'
        END AS class
    FROM
        channels c
        LEFT JOIN distinct_labels l ON l.address = c.channel
)
SELECT
    *
FROM
    (
        SELECT
            DAY,
            s.channel AS "Address",
            CONCAT(
                '<a target="_blank" href="https://polygonscan.com/address/0',
                SUBSTRING(s.channel :: text, 2, 42),
                '">',
                'https://polygonscan.com/address/0',
                SUBSTRING(s.channel :: text, 2, 42),
                '</a>'
            ) AS etherscan,
            s.txns AS trades,
            sum(volume) AS "Volume",
            ROW_NUMBER() OVER (
                PARTITION BY DAY
                ORDER BY
                    sum(volume) DESC NULLS LAST
            ) AS position
        FROM
            swaps s
            INNER JOIN channel_classifier c ON s.channel = c.channel
        WHERE
            c.class = 'arbitrage bot'
        GROUP BY
            1,
            2,
            3,
            4
        ORDER BY
            DAY DESC,
            "Volume" DESC
    ) ranking
WHERE
    position <= 5