WITH prices AS (
        SELECT date_trunc('day', minute) AS day, contract_address AS token, AVG(price) AS price
        FROM prices.usd
        GROUP BY 1, 2
    ),
    
    dex_prices_1 AS (
        SELECT date_trunc('day', hour) AS day, 
        contract_address AS token, 
        (PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY median_price)) AS price,
        SUM(sample_size) as sample_size
        FROM dex.view_token_prices
        GROUP BY 1, 2
        HAVING sum(sample_size) > 2
    ),
    
    dex_prices AS (
        SELECT *, LEAD(day, 1, now()) OVER (PARTITION BY token ORDER BY day) AS day_of_next_change
        FROM dex_prices_1
    ),
    
    pools AS (
        SELECT '\xfae2809935233d4bfe8a56c2355c4a2e7d1fff1a'::bytea as pools
        -- FROM balancer."BFactory_evt_LOG_NEW_POOL"
    ),
    
    joins AS (
        SELECT date_trunc('day', e.evt_block_time) AS day, e.dst AS lp, p.pools AS pool, SUM(e.amt)/1e18 AS amount
        FROM balancer."BPool_evt_Transfer" e
        INNER JOIN pools p ON e."contract_address" = p.pools
        WHERE e.dst NOT IN ('\x0000000000000000000000000000000000000000', '\x9424b1412450d0f8fc2255faf6046b98213b76bd')
        GROUP BY 1, 2, 3
    ),
    
    exits AS (
        SELECT date_trunc('day', e.evt_block_time) AS day, e.src AS lp, p.pools AS pool, -SUM(e.amt)/1e18 AS amount
        FROM balancer."BPool_evt_Transfer" e
        INNER JOIN pools p ON e."contract_address" = p.pools
        WHERE e.src NOT IN ('\x0000000000000000000000000000000000000000', '\x9424b1412450d0f8fc2255faf6046b98213b76bd')
        GROUP BY 1, 2, 3
    ),
    
    daily_delta_bpt_by_pool AS (
        SELECT day, lp, pool, SUM(COALESCE(amount, 0)) as amount FROM 
        (SELECT *
        FROM joins j 
        UNION ALL
        SELECT * 
        FROM exits e) foo
        GROUP BY 1, 2, 3
    ),
    
    cumulative_bpt_by_pool AS (
        SELECT day, lp, pool, amount, 
        LEAD(day::timestamp, 1, CURRENT_DATE::timestamp) OVER (PARTITION BY lp, pool ORDER BY day) AS next_day,
        SUM(amount) OVER (PARTITION BY lp, pool ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS amount_bpt
        FROM daily_delta_bpt_by_pool
    ),
    
    calendar AS (
        SELECT generate_series('2020-03-31'::timestamp, CURRENT_DATE, '1 day'::interval) AS day
    ),
    
    running_cumulative_bpt_by_pool as (
        SELECT c.day, lp, pool, amount_bpt
        FROM cumulative_bpt_by_pool b
        JOIN calendar c on b.day <= c.day AND c.day < b.next_day
    ),
    
    daily_total_bpt AS (
        SELECT day, pool, SUM(amount_bpt) AS total_bpt
        FROM running_cumulative_bpt_by_pool
        GROUP BY 1, 2
    ),
    
    lp_share_by_pool AS (
        SELECT c.day, c.lp, c.pool, c.amount_bpt/d.total_bpt AS share
        FROM running_cumulative_bpt_by_pool c
        INNER JOIN daily_total_bpt d ON d.day = c.day AND d.pool = c.pool
        WHERE d.total_bpt > 0
    ), 
    
    cumulative_usd_balance_by_token AS (
        SELECT b.pool, b.day, b.token, 
        cumulative_amount /10 ^ t.decimals * p1.price AS amount_usd_from_api,
        cumulative_amount /10 ^ t.decimals * p2.price AS amount_usd_from_dex
        FROM balancer.view_balances b
        LEFT JOIN erc20.tokens t ON t.contract_address = b.token
        LEFT JOIN prices p1 ON p1.day = b.day AND p1.token = b.token
        LEFT JOIN dex_prices p2 ON p2.day <= b.day AND b.day < p2.day_of_next_change AND p2.token = b.token
    ),
    
    pool_liquidity_estimates AS (
        SELECT 
            b.*, 
            b.amount_usd_from_api / w.normalized_weight AS liquidity_from_api,
            b.amount_usd_from_dex / w.normalized_weight AS liquidity_from_dex
        FROM cumulative_usd_balance_by_token b INNER JOIN
        balancer.view_pools_tokens_weights w
        ON b.pool = w.pool_id
        AND b.token = w.token_address
        AND (b.amount_usd_from_api > 0 OR b.amount_usd_from_dex > 0)
        AND w.normalized_weight > 0
    ),
    
    estimated_pool_liquidity as (
        SELECT 
            pool, 
            day, 
            COALESCE(AVG(liquidity_from_api), AVG(liquidity_from_dex)) AS liquidity
        FROM pool_liquidity_estimates
        GROUP BY 1, 2
    ),
    
    daily_lp_tvl AS (
        SELECT p.day, p.lp, SUM(p.share * d.liquidity) AS tvl
        FROM lp_share_by_pool p
        INNER JOIN estimated_pool_liquidity d ON d.day = p.day AND d.pool = p.pool
        GROUP BY 1, 2
    ),
    
    top_lps AS (
        SELECT day, lp, tvl
        FROM daily_lp_tvl 
        WHERE day <= CURRENT_DATE
        ORDER BY 1 DESC, 3 DESC 
        LIMIT 5
    )

SELECT p.day, COALESCE(t.lp::text, 'Others') AS lp, SUM(p.tvl) AS "TVL"
FROM daily_lp_tvl p
LEFT JOIN top_lps t ON t.lp = p.lp
GROUP BY 1, 2