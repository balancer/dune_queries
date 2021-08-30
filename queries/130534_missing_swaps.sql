SELECT 
    date_trunc('day', evt_block_time) AS day,
    COUNT(*) AS n_trades
FROM balancer_v2."Vault_evt_Swap"
GROUP BY 1
ORDER BY 1 DESC