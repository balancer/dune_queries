SELECT
    date_trunc('day', call_block_time) AS DAY,
    COUNT(DISTINCT (funds ->> 'sender')) AS traders
FROM
    balancer_v2."Vault_call_swap"
WHERE
    call_success
    AND call_block_time >= '{{2. Start date}}'
    AND call_block_time <= '{{3. End date}}'
GROUP BY
    1