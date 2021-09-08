SELECT
  COUNT(DISTINCT (funds ->> 'sender')) AS traders
FROM
  balancer_v2."Vault_call_swap" s
WHERE
  call_success