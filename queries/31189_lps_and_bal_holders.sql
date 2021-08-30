WITH transfers AS (
    SELECT
    evt_tx_hash AS tx_hash,
    tr."from" AS address,
    -tr.value AS amount
    FROM erc20."ERC20_evt_Transfer" tr
    WHERE contract_address =  '\xba100000625a3754423978a60c9317c58a424e3d'
UNION ALL
    SELECT
    evt_tx_hash AS tx_hash,
    tr."to" AS address,
    tr.value AS amount
    FROM erc20."ERC20_evt_Transfer" tr 
    where contract_address = '\xba100000625a3754423978a60c9317c58a424e3d'
),
transferAmounts AS (
    SELECT address,
    sum(amount)/1e18 as poolholdings FROM transfers
    WHERE address IN (
        SELECT DISTINCT(caller) FROM balancer."BPool_evt_LOG_JOIN"
    )
    GROUP BY 1
)


SELECT
    CASE 
        WHEN poolholdings BETWEEN 0 AND 1 THEN '< 1' 
        WHEN poolholdings BETWEEN 1 AND 10 THEN '< 10' 
        WHEN poolholdings BETWEEN 10 AND 100 THEN '< 100' 
        WHEN poolholdings BETWEEN 100 AND 1000 THEN '< 1000' 
        WHEN poolholdings BETWEEN 1000 AND 10000 THEN '< 10000' 
        WHEN poolholdings BETWEEN 10000 AND 100000 THEN '< 100000' 
        WHEN poolholdings BETWEEN 100000 AND 1000000 THEN '< 1000000' 
        ELSE '> 1000000'
    END AS "BAL",
    COUNT(address) as "# LPs"
FROM transferAmounts
WHERE poolholdings > 0
GROUP BY 1
