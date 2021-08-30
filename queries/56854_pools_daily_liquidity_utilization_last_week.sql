WITH labels AS (
        SELECT 'WBTC/WETH 50/50' AS label, '1' AS version,
            '\x1eff8af5d577060ba4ac8a29a13525bb0ee2a3d5' AS address
        UNION ALL
        SELECT 'WBTC/WETH 50/50' AS label, '2' AS version,
            '\xa6f548df93de924d73be7d25dc02554c6bd66db500020000000000000000000e' AS address
        UNION ALL
        SELECT 'BAL/WETH 50/50' AS label, '1' AS version,
            '\x59a19d8c652fa0284f44113d0ff9aba70bd46fb4' AS address
        UNION ALL
        SELECT 'BAL/WETH 50/50' AS label, '2' AS version,
            '\x5c6ee304399dbdb9c8ef030ab642b10820db8f56000200000000000000000014' AS address
        UNION ALL
        SELECT 'USDC/WETH 50/50' AS label, '1' AS version,
            '\x8a649274e4d777ffc6851f13d23a86bbfa2f2fbf' AS address
        UNION ALL
        SELECT 'USDC/WETH 50/50' AS label, '2' AS version,
            '\x96646936b91d6b9d7d0c47c496afbf3d6ec7b6f8000200000000000000000019' AS address
        UNION ALL
        SELECT 'BAL/WETH 80/20' AS label, '1' AS version,
            '\x59a19d8c652fa0284f44113d0ff9aba70bd46fb4' AS address
        UNION ALL
        SELECT 'BAL/WETH 80/20' AS label, '2' AS version,
            '\x5c6ee304399dbdb9c8ef030ab642b10820db8f56000200000000000000000014' AS address
        UNION ALL
        SELECT 'LINK/WETH 50/50' AS label, '1' AS version,
            '\x77f5f3dfd95d0b061c9ef47c4f4ca4681d807776' AS address
        UNION ALL
        SELECT 'LINK/WETH 50/50' AS label, '2' AS version,
            '\xe99481dc77691d8e2456e5f3f61c1810adfc1503000200000000000000000018' AS address
        UNION ALL
        SELECT 'MKR/WETH 60/40' AS label, '1' AS version,
            '\x9866772a9bdb4dc9d2c5a4753e8658b8b0ca1fc3' AS address
        UNION ALL
        SELECT 'MKR/WETH 60/40' AS label, '2' AS version,
            '\xaac98ee71d4f8a156b6abaa6844cdb7789d086ce00020000000000000000001b' AS address
),

    prices AS (
        SELECT date_trunc('day', minute) AS day, contract_address AS token, AVG(price) AS price
        FROM prices.usd
        WHERE minute > '2021-04-20'
        GROUP BY 1, 2
    ),
    
    dex_prices_1 AS (
        SELECT date_trunc('day', hour) AS day, 
        contract_address AS token, 
        (PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY median_price)) AS price,
        SUM(sample_size) as sample_size
        FROM dex.view_token_prices
        WHERE hour > '2021-04-20'
        GROUP BY 1, 2
        HAVING sum(sample_size) > 2
    ),
    
    dex_prices AS (
        SELECT *, LEAD(day, 1, now()) OVER (PARTITION BY token ORDER BY day) AS day_of_next_change
        FROM dex_prices_1
    ),
    
    events_v2 AS (
        SELECT date_trunc('day', evt_block_time) AS day, "poolId" AS pool, "tokenIn" AS token, "amountIn" AS delta
        FROM balancer_v2."Vault_evt_Swap"
        
        UNION ALL
        
        SELECT date_trunc('day', evt_block_time) AS day, "poolId" AS pool, "tokenOut" AS token, -"amountOut" AS delta
        FROM balancer_v2."Vault_evt_Swap"
        
        UNION ALL 
        
        SELECT date_trunc('day', evt_block_time) AS day, '\xBA12222222228d8Ba445958a75a0704d566BF2C8'::bytea AS pool, token, SUM(COALESCE(delta, 0)) AS delta 
        FROM balancer_v2."Vault_evt_InternalBalanceChanged"
        GROUP BY 1, 2, 3
        
        UNION ALL
        
        SELECT date_trunc('day', evt_block_time) AS day, "poolId" AS pool, UNNEST(tokens) AS token, UNNEST(deltas) AS delta 
        FROM balancer_v2."Vault_evt_PoolBalanceChanged"
        
        UNION ALL 
        
        SELECT date_trunc('day', evt_block_time) AS day, "poolId" AS pool, token, "cashDelta" + "managedDelta" AS delta
        FROM balancer_v2."Vault_evt_PoolBalanceManaged"
    
    ),
    
    daily_delta_balance AS (
        SELECT day, pool, token, SUM(COALESCE(delta, 0)) AS amount 
        FROM events_v2
        GROUP BY 1, 2, 3
    ),
    
    cumulative_balance AS (
        SELECT 
            day,
            pool, 
            token,
            LEAD(day, 1, now()) OVER (PARTITION BY token, pool ORDER BY day) AS day_of_next_change,
            SUM(amount) OVER (PARTITION BY pool, token ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
        FROM daily_delta_balance
    ),
    
    calendar AS (
        SELECT generate_series('2021-04-21'::timestamp, CURRENT_DATE, '1 day'::interval) AS day
    ),
    
    cumulative_usd_balance AS (
        SELECT c.day, b.pool, b.token, cumulative_amount,
        cumulative_amount / 10 ^ t.decimals * p1.price AS amount_usd_from_api,
        cumulative_amount /10 ^ t.decimals * p2.price AS amount_usd_from_dex
        FROM calendar c
        LEFT JOIN cumulative_balance b ON b.day <= c.day AND c.day < b.day_of_next_change
        LEFT JOIN erc20.tokens t ON t.contract_address = b.token
        LEFT JOIN prices p1 ON p1.day = b.day AND p1.token = b.token
        LEFT JOIN dex_prices p2 ON p2.day <= c.day AND c.day < p2.day_of_next_change AND p2.token = b.token
    ),
    
    estimated_pool_liquidity as (
        SELECT 
            day, 
            pool, 
            SUM(COALESCE(amount_usd_from_api, amount_usd_from_dex)) AS liquidity
        FROM cumulative_usd_balance
        GROUP BY 1, 2
    ),
    
    volume AS (
        SELECT
            t.version,
            exchange_contract_address,
            date_trunc('day', block_time) AS day,
            SUM(usd_amount) AS volume
        FROM dex.trades t
        INNER JOIN labels l ON t.exchange_contract_address::text = l.address AND t.version = l.version
        WHERE project = 'Balancer'
        AND date_trunc('day', block_time) > date_trunc('day', CURRENT_DATE - '1 week'::interval)
        AND l.label = '{{Pool}}'
        GROUP BY 1, 2, 3
    ),
    
    liquidity AS (
        SELECT
            '1' AS version, day, SUM(liquidity) AS liquidity
        FROM balancer."view_pools_liquidity" b
        INNER JOIN labels l ON b.pool::text = l.address AND l.version = '1'
        WHERE date_trunc('day', day) > date_trunc('day', CURRENT_DATE - '1 week'::interval)
        AND l.label = '{{Pool}}'
        GROUP BY 1, 2
        
        UNION ALL
        
        SELECT '2' AS version, day, SUM(liquidity) AS liquidity
        FROM estimated_pool_liquidity b
        INNER JOIN labels l ON b.pool::text = l.address AND l.version = '2'
        WHERE date_trunc('day', day) > date_trunc('day', CURRENT_DATE - '1 week'::interval)
        AND l.label = '{{Pool}}'
        GROUP BY 1, 2
    ),
    
  liquidity_volume AS  (
        SELECT l.day, CONCAT('V', l.version) AS version,
        volume,
        liquidity, COALESCE(volume/liquidity,0) AS liquidity_utilization 
        FROM liquidity l
        JOIN volume v ON v.day = l.day AND v.version = l.version
    )
    
SELECT * FROM liquidity_volume