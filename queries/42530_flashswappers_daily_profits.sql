WITH foo AS (
    SELECT
        unnest("output_assetDeltas") AS delta,
        call_tx_hash AS tx_hash
    FROM
        balancer_v2."Vault_call_batchSwap" b
    WHERE
        call_success -- ON  a.evt_tx_hash = b.call_tx_hash
),
flashswaps AS (
    SELECT
        tx_hash
    FROM
        (
            SELECT
                MAX(delta) AS vMax,
                tx_hash
            FROM
                foo
            GROUP BY
                2
        ) t
    WHERE
        vMax <= 0
),
proceeds AS (
    SELECT
        call_tx_hash AS tx_hash,
        call_block_time AS time,
        unnest(assets) AS token_address,
        - unnest("output_assetDeltas") AS proceeds
    FROM
        balancer_v2."Vault_call_batchSwap" s
    WHERE
        call_tx_hash IN (
            SELECT
                *
            FROM
                flashswaps
        )
),
proceeds_price AS (
    SELECT
        s.*,
        p.price,
        p.decimals
    FROM
        proceeds s
        INNER JOIN prices.usd p ON date_trunc('minute', s.time) = p.minute
        AND s.token_address = p.contract_address
),
revenue AS (
    SELECT
        tx_hash,
        time,
        SUM(price * proceeds / (10 ^ decimals)) AS revenue
    FROM
        proceeds_price
    GROUP BY
        1,
        2
),
revenue_cost AS (
    SELECT
        tx_hash,
        time,
        revenue,
        p.price * t.gas_used * t.gas_price / (10 ^ 18) AS cost
    FROM
        revenue r
        INNER JOIN ethereum.transactions t ON t.hash = r.tx_hash
        AND t.block_time >= (
            SELECT
                MIN(time)
            FROM
                revenue
        )
        INNER JOIN prices."layer1_usd" p ON p.minute = date_trunc('minute', r.time)
        AND p.symbol = 'ETH'
)
SELECT
    date_trunc('day', time),
    SUM(revenue - cost) AS profit
FROM
    revenue_cost
GROUP BY
    1
ORDER BY
    1