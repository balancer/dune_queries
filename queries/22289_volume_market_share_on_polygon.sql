WITH prices AS (
        SELECT date_trunc('day', minute) AS day, contract_address AS token, decimals, AVG(price) AS price
        FROM prices.usd
        GROUP BY 1, 2, 3
    ), 
    
    swaps AS (
        SELECT 
            date_trunc('week', evt_block_time) AS week,
            'Balancer' AS project,
            SUM(COALESCE(("amountIn" / 10 ^ p1.decimals) * p1.price, ("amountOut" / 10 ^ p2.decimals) * p2.price)) AS volume
        FROM balancer_v2."Vault_evt_Swap" s
        LEFT JOIN prices p1 ON p1.day = date_trunc('day', evt_block_time) AND p1.token = s."tokenIn"
        LEFT JOIN prices p2 ON p2.day = date_trunc('day', evt_block_time) AND p2.token = s."tokenOut"
        WHERE evt_block_time >= now() - '2 months'::interval   
        AND evt_block_time <= '{{3. End date}}'
        GROUP BY 1
        
        UNION ALL
        
        SELECT 
            date_trunc('week', evt_block_time) AS week,
            'Quickswap' AS project,
            SUM(COALESCE((token_a_amount_raw / 10 ^ p1.decimals) * p1.price, (token_b_amount_raw / 10 ^ p2.decimals) * p2.price)) AS volume
        FROM (
            SELECT 
                t.evt_block_time,
                CASE WHEN "amount0Out" = 0 THEN f.token1 ELSE f.token0 END AS token_a_address,
                CASE WHEN "amount0In" = 0 THEN f.token1 ELSE f.token0 END AS token_b_address,
                CASE WHEN "amount0Out" = 0 THEN "amount1Out" ELSE "amount0Out" END AS token_a_amount_raw,
                CASE WHEN "amount0In" = 0 THEN "amount1In" ELSE "amount0In" END AS token_b_amount_raw
            FROM quickswap."UniswapV2Pair_evt_Swap" t
            INNER JOIN quickswap."UniswapV2Factory_evt_PairCreated" f ON f.pair = t.contract_address
            WHERE t.evt_block_time >= now() - '2 months'::interval
            AND t.evt_block_time <= '{{3. End date}}'
        ) s
        LEFT JOIN dune_user_generated.prices_usd p1 ON p1.minute = date_trunc('day', evt_block_time) AND p1.contract_address = s.token_a_address
        LEFT JOIN dune_user_generated.prices_usd p2 ON p2.minute = date_trunc('day', evt_block_time) AND p2.contract_address = s.token_b_address
        GROUP BY 1
        
        UNION ALL
        
        SELECT 
            date_trunc('week', evt_block_time) AS week,
            'Sushiswap' AS project,
            SUM(COALESCE((token_a_amount_raw / 10 ^ p1.decimals) * p1.price, (token_b_amount_raw / 10 ^ p2.decimals) * p2.price)) AS volume
        FROM (
            SELECT 
                t.evt_block_time,
                CASE WHEN "amount0Out" = 0 THEN f.token1 ELSE f.token0 END AS token_a_address,
                CASE WHEN "amount0In" = 0 THEN f.token1 ELSE f.token0 END AS token_b_address,
                CASE WHEN "amount0Out" = 0 THEN "amount1Out" ELSE "amount0Out" END AS token_a_amount_raw,
                CASE WHEN "amount0In" = 0 THEN "amount1In" ELSE "amount0In" END AS token_b_amount_raw
            FROM sushi."UniswapV2Pair_evt_Swap" t
            INNER JOIN sushi."UniswapV2Factory_evt_PairCreated" f ON f.pair = t.contract_address
            WHERE t.evt_block_time >= now() - '2 months'::interval
            AND t.evt_block_time <= '{{3. End date}}'
        ) s
        LEFT JOIN dune_user_generated.prices_usd p1 ON p1.minute = date_trunc('day', evt_block_time) AND p1.contract_address = s.token_a_address
        LEFT JOIN dune_user_generated.prices_usd p2 ON p2.minute = date_trunc('day', evt_block_time) AND p2.contract_address = s.token_b_address
        GROUP BY 1
    )

SELECT * 
FROM swaps
