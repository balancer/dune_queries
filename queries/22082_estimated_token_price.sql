DROP TABLE dune_user_generated.balancer_estimated_tokens_price;

CREATE TABLE dune_user_generated.balancer_estimated_tokens_price (
    day timestamp,
    token bytea,
    price numeric
);

WITH pools AS (
        SELECT pool as pools
        FROM balancer."BFactory_evt_LOG_NEW_POOL"
    ),
    
    prices AS (
        SELECT date_trunc('day', minute) AS day, contract_address AS token, AVG(price) AS price
        FROM prices.usd
        GROUP BY 1, 2
    ),
    
    dex_prices AS (
        select date_trunc('day', hour) AS day, contract_address AS token, (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_price)) AS price
        FROM dex.view_token_prices
        GROUP BY 1, 2
    ),
    
    weekly_delta_balance_by_token AS (
        SELECT day, pool, token, cumulative_amount, (cumulative_amount - COALESCE(LAG(cumulative_amount, 1) OVER (PARTITION BY pool, token ORDER BY day), 0)) AS amount
        FROM (SELECT day, pool, token, SUM(cumulative_amount) AS cumulative_amount
        FROM balancer.view_balances b
        WHERE extract(dow from day) = 1
        GROUP BY 1, 2, 3) foo
    ),
    
    cumulative_usd_balance_by_token AS (
        SELECT b.pool, b.day, b.token, b.cumulative_amount,
        cumulative_amount /10 ^ t.decimals * p1.price AS amount_usd_from_api,
        cumulative_amount /10 ^ t.decimals * p2.price AS amount_usd_from_dex
        FROM balancer.view_balances b
        LEFT JOIN erc20.tokens t ON t.contract_address = b.token
        LEFT JOIN prices p1 ON p1.day = b.day AND p1.token = b.token
        LEFT JOIN dex_prices p2 ON p2.day = b.day AND p2.token = b.token
    ),
    
    pool_liquidity_estimates AS (
        SELECT 
            b.*, w.normalized_weight,
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
            token,
            day, 
            coalesce(avg(liquidity_from_api),avg(liquidity_from_dex)) AS liquidity
        FROM pool_liquidity_estimates
        GROUP BY 1, 2, 3
    ),
    
    estimated_token_liquidity AS (
        SELECT b.day, token, SUM(liquidity * normalized_weight) AS liquidity
        FROM estimated_pool_liquidity b
        INNER JOIN balancer.view_pools_tokens_weights w
        ON b.pool = w.pool_address
        AND b.token = w.token_address
        AND w.normalized_weight > 0
        GROUP BY 1, 2
    ),
    
    cumulative_amount_by_token AS (
        SELECT day, token, SUM(cumulative_amount/10^decimals) AS cumulative_amount
        FROM weekly_delta_balance_by_token b
        LEFT JOIN erc20.tokens t ON t.contract_address = b.token
        GROUP BY 1, 2
    ),
    
    estimated_token_price AS (
        SELECT l.day, l.token, liquidity/cumulative_amount AS price
        FROM estimated_token_liquidity l 
        INNER JOIN cumulative_amount_by_token c ON c.token = l.token AND c.day = l.day
    )
    
INSERT INTO dune_user_generated.balancer_estimated_tokens_price
SELECT * FROM estimated_token_price