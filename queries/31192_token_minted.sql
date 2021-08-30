WITH minted AS (
        SELECT date_trunc('week', evt_block_time) AS week, SUM(tr.value/1e18) AS minted
        FROM erc20."ERC20_evt_Transfer" tr
        WHERE contract_address = '\xba100000625a3754423978a60c9317c58a424e3d'
        AND tr."from" = '\x0000000000000000000000000000000000000000'
        GROUP BY week
    ),
    
    cumulative AS (
        SELECT week, SUM(minted) OVER (ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative
        FROM minted
    )
    
SELECT m.week, m.minted, c.cumulative FROM minted m
JOIN cumulative c ON c.week = m.week