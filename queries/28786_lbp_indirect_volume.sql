WITH lbp_info AS (
    SELECT
        *
    FROM
        balancer.view_lbps
    WHERE
        name = '{{LBP}}'
),
lbp_volume AS (
    SELECT
        date_trunc('day', a.block_time) AS block_time,
        SUM(a.usd_amount) AS direct_spent
    FROM
        dex.trades a
        INNER JOIN lbp_info l ON a.token_a_address = l.token_sold
        OR a.token_b_address = l.token_sold
        AND l.pool = a.exchange_contract_address
        AND a.project = 'Balancer'
    WHERE
        a.block_time BETWEEN l.initial_time
        AND l.final_time
    GROUP BY
        1
),
token_purchases AS (
    SELECT
        date_trunc('day', a.block_time) AS block_time,
        sum(a.usd_amount) AS direct_spent,
        sum(b.usd_amount) AS indirect_spent
    FROM
        dex.trades a
        INNER JOIN lbp_info l ON l.pool = a.exchange_contract_address
        INNER JOIN dex.trades b ON a.exchange_contract_address = l.pool
        AND a.token_a_address = l.token_sold
        AND b.token_a_address <> l.token_sold
    WHERE
        a.tx_hash = b.tx_hash
        AND a.project = 'Balancer'
        AND b.project = 'Balancer'
        AND a.block_time BETWEEN l.initial_time
        AND l.final_time
    GROUP BY
        1
),
token_sales AS (
    SELECT
        date_trunc('day', a.block_time) AS block_time,
        sum(a.usd_amount) AS direct_spent,
        sum(b.usd_amount) AS indirect_spent
    FROM
        dex.trades a
        INNER JOIN lbp_info l ON l.pool = a.exchange_contract_address
        INNER JOIN dex.trades b ON l.pool = a.exchange_contract_address
        AND a.token_b_address = l.token_sold
        AND b.token_b_address <> l.token_sold
    WHERE
        a.tx_hash = b.tx_hash
        AND a.project = 'Balancer'
        AND b.project = 'Balancer'
        AND a.block_time BETWEEN l.initial_time
        AND l.final_time
    GROUP BY
        1
)
SELECT
    COALESCE(
        z.block_time,
        COALESCE(p.block_time, s.block_time)
    ) AS block_time,
    COALESCE(z.direct_spent, 0) lbp_volume,
    COALESCE(p.indirect_spent, 0) + COALESCE(s.indirect_spent, 0) AS side_volume
FROM
    token_purchases p FULL
    OUTER JOIN token_sales s ON p.block_time = s.block_time FULL
    OUTER JOIN lbp_volume z ON p.block_time = z.block_time -- select max(block_time) from dex.trades