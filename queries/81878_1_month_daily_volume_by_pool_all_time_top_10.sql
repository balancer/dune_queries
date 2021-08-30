WITH swaps AS (
        SELECT
            date_trunc('day', d.block_time) AS day,
            sum(usd_amount) AS volume,
            d.exchange_contract_address AS address,
            COUNT(DISTINCT trader_a) AS traders
        FROM dex.trades d
        WHERE project = 'Balancer'
        AND ('{{4. Version}}' = 'Both' OR version = SUBSTRING('{{4. Version}}', 2))
        AND date_trunc('day', d.block_time) > date_trunc('day', now() - interval '1 month')
        AND ('{{1. Pool ID}}' = 'All' OR exchange_contract_address = CONCAT('\', SUBSTRING('{{1. Pool ID}}', 2))::bytea)
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
),

    ranking AS (
        SELECT
            address,
            ROW_NUMBER() OVER (ORDER BY SUM(volume) DESC NULLS LAST) AS position
        FROM swaps
        GROUP BY 1
)

SELECT
    s.day,
    s.address,
    CONCAT(SUBSTRING(UPPER(l.name), 0, 15), ' (', SUBSTRING(s.address::text, 3, 8), ')') AS pool,
    r.position,
    s.traders,
    ROUND(sum(s.volume), 2) AS volume
FROM swaps s
LEFT JOIN ranking r ON r.address = s.address
LEFT JOIN labels l ON SUBSTRING(s.address::text, 0, 43)::bytea = l.address
WHERE position <= 10
AND volume > 0
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5