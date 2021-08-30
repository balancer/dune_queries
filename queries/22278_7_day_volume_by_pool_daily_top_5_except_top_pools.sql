-- Volume (pool breakdown) per day (top 5 pools of each day) (7 days)
-- Visualization: bar chart (stacked)

WITH swaps AS (
    SELECT
        date_trunc('day', d.block_time) AS day,
        sum(usd_amount) AS volume,
        d.exchange_contract_address AS address,
        COUNT(DISTINCT trader_a) AS traders
    FROM dex.trades d
    WHERE project = 'Balancer'
    AND ('{{4. Version}}' = 'Both' OR version = SUBSTRING('{{4. Version}}', 2))
    AND date_trunc('day', d.block_time) > date_trunc('day', now() - interval '1 week')
    AND exchange_contract_address NOT IN ('\x8b6e6e7b5b3801fed2cafd4b22b8a16c2f2db21a',
                                    '\x1eff8af5d577060ba4ac8a29a13525bb0ee2a3d5',
                                    '\x59a19d8c652fa0284f44113d0ff9aba70bd46fb4',
                                    '\xc697051d1c6296c24ae3bcef39aca743861d9a81',
                                    '\x0b09dea16768f0799065c475be02919503cb2a3500020000000000000000001a')
    GROUP BY 1,3
),

    labels AS (
    SELECT * FROM (SELECT
            address,
            name,
            ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
        FROM labels.labels
        WHERE "type" IN ('balancer_pool', 'balancer_v2_pool')
        GROUP BY 1, 2) l
        WHERE num = 1
)

SELECT * FROM (
    SELECT
        s.address,
        CONCAT(SUBSTRING(UPPER(l.name), 0, 15), ' (', SUBSTRING(s.address::text, 3, 8), ')') AS pool,
        day,
        s.traders,
        ROW_NUMBER() OVER (PARTITION BY day ORDER BY SUM(volume) DESC NULLS LAST) AS position,
        ROUND(sum(s.volume), 2) AS volume
    FROM swaps s
    LEFT JOIN labels l ON l.address = SUBSTRING(s.address::text, 0, 43)::bytea
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3, 4
) ranking
WHERE position <= 5
AND volume > 0