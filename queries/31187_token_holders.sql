WITH addresses AS (
        SELECT "to" AS adr
        FROM erc20."ERC20_evt_Transfer" tr
        WHERE contract_address = '\xba100000625a3754423978a60c9317c58a424e3d'
), 
    
    transfers AS (
        SELECT  
            day,
            address, 
            token_address,
            SUM(amount) AS amount
        FROM (
            SELECT  date_trunc('day', evt_block_time) AS day,
                    "to" AS address,
                    tr.contract_address AS token_address,
                    value AS amount
            FROM erc20."ERC20_evt_Transfer" tr
            WHERE contract_address = '\xba100000625a3754423978a60c9317c58a424e3d'
    
            UNION ALL
            
            SELECT  date_trunc('day', evt_block_time) AS day,
                    "from" AS address,
                    tr.contract_address AS token_address,
                    -value AS amount
            FROM erc20."ERC20_evt_Transfer" tr
            WHERE contract_address = '\xba100000625a3754423978a60c9317c58a424e3d'
    ) t
   GROUP BY 1, 2, 3
 ),
 
    balances_with_gap_days AS (
        SELECT  
            t.day, 
            address, 
            SUM(amount) OVER (PARTITION BY address ORDER BY t.day) AS balance, 
            LEAD(day, 1, now()) OVER (PARTITION BY address ORDER BY t.day) AS next_day
        FROM transfers t
),
    
    days AS (
        SELECT generate_series('2020-01-01'::timestamp, date_trunc('day', NOW()), '1 day') AS day
), 
    
    balance_all_days AS (
        SELECT  d.day,
                address,
                SUM(balance/10^0) AS balance
        FROM balances_with_gap_days b
        INNER JOIN days d ON b.day <= d.day AND d.day < b.next_day
        GROUP BY 1, 2
        ORDER BY 1, 2
)

SELECT  b.day, COUNT(DISTINCT address) AS "BAL Holders"
FROM balance_all_days b
WHERE balance > 0
GROUP BY 1