SELECT
    date_trunc('day', evt_block_time) AS DAY,
    user :: bytea,
    SUM(ABS(delta))
FROM
    balancer_v2."Vault_evt_InternalBalanceChanged"
GROUP BY
    1,
    2