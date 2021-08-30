WITH pools AS (
        SELECT pool as pools
        FROM balancer."BFactory_evt_LOG_NEW_POOL"
    ),
    
    prices AS (
        SELECT date_trunc('day', minute) AS day, contract_address AS token, AVG(price) AS price
        FROM prices.usd
        GROUP BY 1, 2
    ),
    
    dex_prices_1 AS (
        select date_trunc('day', hour) AS day, 
        contract_address AS token, 
        (PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY median_price)) AS price,
        sum(sample_size) as sample_size
        FROM dex.view_token_prices
        GROUP BY 1, 2
        HAVING sum(sample_size) > 2
    ),
    
    dex_prices AS (
        select *, LEAD(day, 1, now()) OVER (PARTITION BY token ORDER BY day) AS day_of_next_change
        from dex_prices_1
    ),
    
    joins AS (
        SELECT p.pools as pool, date_trunc('day', e.evt_block_time) AS day, e.contract_address AS token, sum(value) as amount
        FROM erc20."ERC20_evt_Transfer" e
        INNER JOIN pools p ON e."to" = p.pools
        GROUP BY 1, 2, 3
    ),

    exits AS (
        SELECT p.pools as pool, date_trunc('day', e.evt_block_time) AS day, e.contract_address AS token, -sum(value) as amount
        FROM erc20."ERC20_evt_Transfer" e
        INNER JOIN pools p ON e."from" = p.pools   
        GROUP BY 1, 2, 3
    ),
    
    daily_delta_balance_by_token AS (
        SELECT pool, day, token, SUM(COALESCE(amount, 0)) as amount FROM 
        (SELECT *
        FROM joins j 
        UNION ALL
        SELECT * 
        FROM exits e) foo
        GROUP BY 1, 2, 3
    ),
    
    cumulative_balance_by_token AS (
        SELECT 
            pool, 
            token, 
            day, 
            LEAD(day, 1, now()) OVER (PARTITION BY token, pool ORDER BY day) AS day_of_next_change,
            amount as change, 
            SUM(amount) OVER (PARTITION BY pool, token ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
        FROM daily_delta_balance_by_token
    ),
    
    calendar AS (
        SELECT generate_series('2020-01-01'::timestamp, CURRENT_DATE, '1 day'::interval) AS day
    ),
    
    cumulative_usd_balance_by_token AS (
        SELECT b.pool, c.day, b.token, 
        cumulative_amount /10 ^ t.decimals * p1.price AS amount_usd_from_api,
        cumulative_amount /10 ^ t.decimals * p2.price AS amount_usd_from_dex
        FROM calendar c
        LEFT JOIN cumulative_balance_by_token b ON b.day <= c.day AND c.day < b.day_of_next_change
        LEFT JOIN erc20.tokens t ON t.contract_address = b.token
        LEFT JOIN prices p1 ON p1.day = c.day AND p1.token = b.token
        LEFT JOIN dex_prices p2 ON p2.day <= c.day AND c.day < p2.day_of_next_change AND p2.token = b.token
    ),
    
    pool_liquidity_estimates AS (
        SELECT 
            b.*, 
            b.amount_usd_from_api / w.normalized_weight AS liquidity_from_api,
            b.amount_usd_from_dex / w.normalized_weight AS liquidity_from_dex
        FROM cumulative_usd_balance_by_token b INNER JOIN
        balancer.view_pools_tokens_weights w
        ON b.pool = w.pool_address
        AND b.token = w.token_address
        AND (b.amount_usd_from_api > 0 OR b.amount_usd_from_dex > 0)
        AND w.normalized_weight > 0
    ),
    
    estimated_pool_liquidity as (
        SELECT 
            pool, 
            day, 
            coalesce(avg(liquidity_from_api),avg(liquidity_from_dex)) AS liquidity
        FROM pool_liquidity_estimates
        GROUP BY 1, 2
    ),
    
    top_pools AS (
        SELECT day, pool as pool, SUBSTRING(d.label, 0, 25) AS symbol, liquidity
        FROM estimated_pool_liquidity p
        INNER JOIN dune_user_generated."balancer_pools" d ON d.address = p.pool
        WHERE day <= CURRENT_DATE
        ORDER BY 1 DESC, 4 DESC 
        LIMIT 5
    )
    
SELECT p.day, COALESCE(t.symbol, 'Others') AS pool, SUM(p.liquidity) AS "TVL"
FROM estimated_pool_liquidity p
LEFT JOIN top_pools t ON t.pool = p.pool
GROUP BY 1, 2