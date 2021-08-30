WITH labels AS (
        SELECT * FROM (SELECT
            address,
            name,
            ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
        FROM labels.labels
        WHERE "type" = 'balancer_pool'
        GROUP BY 1, 2) l
        WHERE num = 1
    ),

    volume AS (
        SELECT date_trunc('day', block_time) AS day, exchange_contract_address AS pool, SUM(usd_amount) AS volume
        FROM dex.trades
        WHERE project = 'Balancer'
        GROUP BY 1, 2
    ),
    
    liquidity AS (
        SELECT *
        FROM balancer."view_pools_liquidity"
    ),
    
    weights1 AS (
        SELECT
            ROW_NUMBER() OVER (PARTITION BY pool_id ORDER BY normalized_weight) AS idx,
            pool_id,
            normalized_weight AS weight1
        FROM balancer."view_pools_tokens_weights"
    ),
    
    weights2 AS (
        SELECT
            ROW_NUMBER() OVER (PARTITION BY pool_id ORDER BY normalized_weight) AS idx,
            pool_id,
            normalized_weight AS weight2
        FROM balancer."view_pools_tokens_weights"
    ),
    
    weights AS (
        SELECT
            w1.pool_id,
            weight1 AS w_i,
            weight2 AS w_j
        FROM weights1 w1
        CROSS JOIN weights2 w2
        WHERE w1.pool_id = w2.pool_id
        AND w1.idx < w2.idx
    ),
    
    ratio_factors AS (
        SELECT
            pool_id AS pool,
             ROUND(SUM((2*w_i*w_j/(w_i+w_j))^2) / SUM(w_i*w_j), {{Ratio factor decimals}})::text AS ratio_factor
    FROM weights
    GROUP BY pool_id
    ),
    
    liquidity_volume AS  (
        SELECT l.day, l.pool, CONCAT(SUBSTRING(UPPER(la.name), 0, 15), ' (', SUBSTRING(l.pool::text, 3, 8), ')') AS symbol, r.ratio_factor::text AS ratio_factor, liquidity, volume 
        FROM liquidity l
        JOIN volume v ON v.pool = l.pool AND v.day = l.day
        LEFT JOIN labels la ON l.pool = la.address
        left join ratio_factors r ON l.pool = r.pool
    ),
    
    last_liquidity_volume AS (
        SELECT *,
        CASE WHEN '{{Ratio factor}}' <> 'none' THEN (CASE WHEN ratio_factor = '{{Ratio factor}}' THEN ratio_factor
        ELSE 'Others' END) ELSE ratio_factor END AS class
        FROM liquidity_volume
        WHERE day = date_trunc('day', CURRENT_DATE - '24h'::interval)
    )
    
SELECT *
FROM last_liquidity_volume
ORDER BY 4