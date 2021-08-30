WITH transfers AS (
    SELECT
    evt_tx_hash AS tx_hash,
    tr."from" AS address,
    -tr.value AS amount,
    contract_address
     FROM erc20."ERC20_evt_Transfer" tr
     WHERE contract_address =  '\xba100000625a3754423978a60c9317c58a424e3d'
UNION ALL
    SELECT
    evt_tx_hash AS tx_hash,
    tr."to" AS address,
    tr.value AS amount,
      contract_address
     FROM erc20."ERC20_evt_Transfer" tr 
     where contract_address = '\xba100000625a3754423978a60c9317c58a424e3d'
),
transferAmounts AS (
    SELECT address,
    
    sum(amount)/1e18 as poolholdings FROM transfers 
    
    GROUP BY 1
    ORDER BY 2 DESC
)

SELECT COUNT(DISTINCT uniques) uniques FROM (
SELECT 
DISTINCT address as uniques
FROM transferAmounts
WHERE poolholdings > 0
INTERSECT
SELECT DISTINCT caller as uniques
FROM
balancer."BPool_evt_LOG_JOIN") a