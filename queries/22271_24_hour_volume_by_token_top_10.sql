-- Volume (token breakdown) per hour (last 24 hours)
-- Visualization: bar chart (stacked)

WITH swaps AS (
        SELECT
            date_trunc('hour', block_time) AS hour,
            sum(usd_amount) AS volume,
            d.token_b_address AS address,
            t.symbol AS token
        FROM dex.trades d
        LEFT JOIN erc20.tokens t ON t.contract_address = d.token_b_address
        WHERE project = 'Balancer'
        AND date_trunc('hour', block_time) > date_trunc('hour', now() - interval '1 day')
        AND ('{{4. Version}}' = 'Both' OR version = SUBSTRING('{{4. Version}}', 2))
        GROUP BY 1, 3, 4
        
        UNION ALL
        
        SELECT
            date_trunc('hour', block_time) AS hour,
            sum(usd_amount) AS volume,
            d.token_a_address AS address,
            t.symbol AS token
        FROM dex.trades d
        LEFT JOIN erc20.tokens t ON t.contract_address = d.token_a_address
        WHERE project = 'Balancer'
        AND date_trunc('hour', block_time) > date_trunc('hour', now() - interval '1 day')
        AND ('{{4. Version}}' = 'Both' OR version = SUBSTRING('{{4. Version}}', 2))
        GROUP BY 1, 3, 4
),

    ranking AS (
        SELECT
            token,
            address,
            sum(volume)/2,
            ROW_NUMBER() OVER (ORDER BY sum(volume) DESC NULLS LAST) AS position
        FROM swaps
        GROUP BY 1, 2
)

SELECT
    s.hour,
    sum(s.volume)/2 AS volume,
    s.address,
    COALESCE(s.token, CONCAT(SUBSTRING(s.address::text, 3, 6), '...')) AS token
FROM swaps s
LEFT JOIN ranking r ON r.address = s.address
WHERE r.position <= 10
GROUP BY 1,3,4
