-- Balancer LBP stats
-- Visualization: counters
WITH lbp_info AS (
    SELECT
        *
    FROM
        balancer.view_lbps
    WHERE
        name = '{{LBP}}'
)
SELECT
    MAX(block_time) - MIN(block_time) AS duration,
    COUNT(DISTINCT trader_a) AS participants,
    (
        SELECT
            COUNT(*)
        FROM
            dex."trades" t
            INNER JOIN lbp_info l ON l.pool = t.exchange_contract_address
        WHERE
            project = 'Balancer'
    ) AS txns,
    (
        SELECT
            SUM(usd_amount)
        FROM
            dex."trades" t
            INNER JOIN lbp_info l ON l.pool = t.exchange_contract_address
        WHERE
            project = 'Balancer'
            AND block_time <= l.final_time
    ) AS volume
FROM
    dex."trades" t
    INNER JOIN lbp_info l ON l.pool = t.exchange_contract_address
    AND l.token_sold = t.token_a_address
WHERE
    project = 'Balancer'
    AND block_time <= l.final_time
    AND block_time >= l.initial_time