WITH prices AS (
    SELECT date_trunc('day', minute) AS day, contract_address AS token, AVG(price) AS price
    FROM prices.usd
    WHERE minute >= GREATEST('{{2. Start date}}', '2021-04-20'::timestamptz)
    AND minute <= '{{3. End date}}'
    GROUP BY 1, 2
),

dex_prices_1 AS (
    SELECT date_trunc('day', hour) AS day,
    contract_address AS token,
    (PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY median_price)) AS price,
    SUM(sample_size) as sample_size
    FROM dex.view_token_prices
    WHERE hour >= GREATEST('{{2. Start date}}', '2021-04-20'::timestamptz)
    AND hour <= '{{3. End date}}'
    GROUP BY 1, 2
    HAVING sum(sample_size) > 3
),

dex_prices AS (
    SELECT *, LEAD(day, 1, now()) OVER (PARTITION BY token ORDER BY day) AS day_of_next_change
    FROM dex_prices_1
),

cumulative_usd_balance_by_token AS (
    SELECT b.pool, b.day, b.token, b.cumulative_amount,
    cumulative_amount /10 ^ t.decimals * p1.price AS amount_usd_from_api,
    cumulative_amount /10 ^ t.decimals * p2.price AS amount_usd_from_dex
    FROM balancer.view_balances b
    LEFT JOIN erc20.tokens t ON t.contract_address = b.token
    LEFT JOIN prices p1 ON p1.day = b.day AND p1.token = b.token
    LEFT JOIN dex_prices p2 ON p2.day <= b.day AND b.day < p2.day_of_next_change AND p2.token = b.token
),

estimated_pool_liquidity AS (
    SELECT
        day,
        pool,
        token,
        COALESCE(amount_usd_from_api, amount_usd_from_dex) AS amount_usd
    FROM cumulative_usd_balance_by_token
),

tvl_v1 AS (
    SELECT 'V1' as version, p.day, SUM(p.liquidity) AS tvl
    FROM balancer."view_pools_liquidity" p
    WHERE pool <> '\xBA12222222228d8Ba445958a75a0704d566BF2C8'
    AND day <= '{{3. End date}}'
    GROUP BY 1, 2
),

tvl_v2 AS (
    SELECT 'V2' as version, day, SUM(amount_usd) AS tvl
    FROM estimated_pool_liquidity
    WHERE pool = '\xBA12222222228d8Ba445958a75a0704d566BF2C8'
    GROUP BY 1, 2
)

SELECT v1.day, v1.tvl AS "V1", v2.tvl AS "V2", COALESCE(v1.tvl, 0) + COALESCE(v2.tvl, 0) AS "Total"
FROM tvl_v1 v1 FULL OUTER JOIN tvl_v2 v2 ON v1.day = v2.day WHERE v1.day >= '{{2. Start date}}'