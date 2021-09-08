SELECT
  date_trunc('day', MINUTE) AS DAY,
  AVG(price) AS "Price"
FROM
  prices.usd
WHERE
  contract_address = '\xba100000625a3754423978a60c9317c58a424e3d'
GROUP BY
  1