SELECT
  name,
  CONCAT(
    '<a href="https://duneanalytics.com/balancerlabs/balancer-lbp?LBP=',
    name,
    '">view stats</a>'
  ) AS stats,
  CONCAT(
    '<a target="_blank" href="https://pools.balancer.exchange/#/pool/0',
    SUBSTRING(address :: text, 2, 42),
    '">view pool</a>'
  ) AS pool,
  CONCAT(
    '<a target="_blank" href="https://etherscan.io/address/0',
    SUBSTRING(address :: text, 2, 42),
    '">0',
    SUBSTRING(address :: text, 2, 42),
    '</a>'
  ) AS etherscan
FROM
  labels.labels
WHERE
  "type" = 'balancer_lbp'
  AND author IN ('balancerlabs', 'markusbkoch', 'mangool')
ORDER BY
  updated_at DESC