-- Volume (source breakdown) per week
-- Visualization: bar chart (stacked)
WITH swaps AS (
    SELECT
        date_trunc('week', d.block_time) AS week,
        tx_to AS channel,
        COUNT(*) AS txns,
        sum(usd_amount) AS volume
    FROM
        dex.trades d
    WHERE
        project = 'Balancer'
        AND (
            '{{4. Version}}' = 'Both'
            OR version = SUBSTRING('{{4. Version}}', 2)
        )
        AND block_time >= '{{2. Start date}}'
        AND block_time <= '{{3. End date}}'
        AND (
            '{{1. Pool ID}}' = 'All'
            OR exchange_contract_address = CONCAT(
                '\', SUBSTRING(' { { 1.Pool ID } } ', 2))::bytea)
        GROUP BY 1, 2
    ),
    
    manual_labels AS (
        SELECT
            address,  
            name
        FROM labels.labels
        WHERE "type" = ' balancer_source '
        AND "author" = ' balancerlabs '
    ),
    
    arb_bots AS (
        SELECT
            address,  
            name
        FROM labels.labels
        WHERE "name" = ' arbitrage bot '
        AND "author" = ' balancerlabs '
        AND address NOT IN (SELECT address from manual_labels)
    ),
    
    distinct_labels AS (
        SELECT * FROM manual_labels
        union all
        SELECT * FROM arb_bots
    ),
    
    channels AS (
        SELECT channel, sum(coalesce(volume,00)) as volume from swaps group by 1
    ),
    
    trade_count AS (
        SELECT
            date_trunc(' DAY ', block_time) AS day,
            tx_to AS channel,
            COUNT(1) AS daily_trades
        FROM dex.trades
        WHERE trader_a != ' \ x0000000000000000000000000000000000000000 '
        AND project = ' Balancer '
        GROUP BY 1,2
        ),
        
    heavy_traders AS (
        SELECT
            channel, day, daily_trades
        FROM trade_count
        WHERE daily_trades >= 100
        ),
    
    channel_classifier AS (
        SELECT c.channel, l.name,
            CASE WHEN l.name IS NOT NULL THEN l.name
            WHEN f.pool IS NOT NULL THEN ' BPool direct '
            WHEN c.channel IN (SELECT channel FROM heavy_traders) THEN ' heavy trader '
            WHEN c.channel IN (select channel from channels where volume is not null order by volume desc limit 10) THEN CONCAT(SUBSTRING(concat(' 0x ', encode(c.channel, ' hex ')), 0, 13), '...')
            ELSE ' others ' END AS class
        FROM channels c
        LEFT JOIN balancer."BFactory_evt_LOG_NEW_POOL" f ON f.pool = c.channel
        LEFT JOIN distinct_labels l ON l.address = c.channel
    )
    
SELECT
    week,
    c.class,
    sum(txns) AS "Swaps",
    sum(volume)/sum(txns) AS "Avg. Volume per Swap",
    sum(volume) AS "Volume"
FROM swaps s 
INNER JOIN channel_classifier c ON s.channel = c.channel
GROUP BY 1, 2
ORDER BY "Volume" DESC