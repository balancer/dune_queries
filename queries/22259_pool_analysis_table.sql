WITH labels AS (
  SELECT
    *
  FROM
    (
      SELECT
        address,
        name,
        ROW_NUMBER() OVER (
          PARTITION BY address
          ORDER BY
            MAX(updated_at) DESC
        ) AS num
      FROM
        labels.labels
      WHERE
        "type" = 'balancer_v2_pool'
      GROUP BY
        1,
        2
    ) l
  WHERE
    num = 1
),
mainnet_rewards AS (
  SELECT
    pool_id,
    SUM(amount) AS amount,
    SUM(usd_amount) AS usd_amount
  FROM
    dune_user_generated.balancer_liquidity_mining
  WHERE
    chain_id = '1'
    AND DAY >= '{{2. Start date}}'
    AND DAY <= '{{3. End date}}'
  GROUP BY
    1
),
swaps AS (
  SELECT
    exchange_contract_address AS pool_id,
    SUM(usd_amount * swap_fee) AS revenues,
    SUM(usd_amount) AS volume
  FROM
    balancer.view_trades d
  WHERE
    version = '2'
    AND block_time >= '{{2. Start date}}'
    AND block_time <= '{{3. End date}}'
  GROUP BY
    1
),
prices AS (
  SELECT
    date_trunc('day', MINUTE) AS DAY,
    contract_address AS token,
    AVG(price) AS price
  FROM
    prices.usd
  WHERE
    MINUTE > '2021-04-20'
  GROUP BY
    1,
    2
),
calendar AS (
  SELECT
    generate_series(
      '2020/06/01' :: timestamptz,
      NOW(),
      '1 day' :: INTERVAL
    ) AS DAY
),
dex_prices_1 AS (
  SELECT
    date_trunc('day', HOUR) AS DAY,
    contract_address AS token,
    (
      PERCENTILE_DISC(0.5) WITHIN GROUP (
        ORDER BY
          median_price
      )
    ) AS price,
    SUM(sample_size) AS sample_size
  FROM
    dex.view_token_prices
  WHERE
    HOUR > '2021-04-20'
  GROUP BY
    1,
    2
  HAVING
    sum(sample_size) > 3
),
dex_prices AS (
  SELECT
    *,
    LEAD(DAY, 1, NOW()) OVER (
      PARTITION BY token
      ORDER BY
        DAY
    ) AS day_of_next_change
  FROM
    dex_prices_1
),
swaps_changes AS (
  SELECT
    DAY,
    pool,
    token,
    SUM(COALESCE(delta, 0)) AS delta
  FROM
    (
      SELECT
        date_trunc('day', evt_block_time) AS DAY,
        "poolId" AS pool,
        "tokenIn" AS token,
        "amountIn" AS delta
      FROM
        balancer_v2."Vault_evt_Swap"
      UNION
      ALL
      SELECT
        date_trunc('day', evt_block_time) AS DAY,
        "poolId" AS pool,
        "tokenOut" AS token,
        - "amountOut" AS delta
      FROM
        balancer_v2."Vault_evt_Swap"
    ) swaps
  GROUP BY
    1,
    2,
    3
),
internal_changes AS (
  SELECT
    date_trunc('day', evt_block_time) AS DAY,
    '\xBA12222222228d8Ba445958a75a0704d566BF2C8' :: bytea AS pool,
    token,
    SUM(COALESCE(delta, 0)) AS delta
  FROM
    balancer_v2."Vault_evt_InternalBalanceChanged"
  GROUP BY
    1,
    2,
    3
),
balances_changes AS (
  SELECT
    date_trunc('day', evt_block_time) AS DAY,
    "poolId" AS pool,
    UNNEST(tokens) AS token,
    UNNEST(deltas) AS delta
  FROM
    balancer_v2."Vault_evt_PoolBalanceChanged"
),
managed_changes AS (
  SELECT
    date_trunc('day', evt_block_time) AS DAY,
    "poolId" AS pool,
    token,
    "managedDelta" AS delta
  FROM
    balancer_v2."Vault_evt_PoolBalanceManaged"
),
daily_delta_balance AS (
  SELECT
    DAY,
    pool,
    token,
    SUM(COALESCE(amount, 0)) AS amount
  FROM
    (
      SELECT
        DAY,
        pool,
        token,
        SUM(COALESCE(delta, 0)) AS amount
      FROM
        balances_changes
      GROUP BY
        1,
        2,
        3
      UNION
      ALL
      SELECT
        DAY,
        pool,
        token,
        delta AS amount
      FROM
        swaps_changes
      UNION
      ALL
      SELECT
        DAY,
        pool,
        token,
        delta AS amount
      FROM
        internal_changes
      UNION
      ALL
      SELECT
        DAY,
        pool,
        token,
        delta AS amount
      FROM
        managed_changes
    ) balance
  WHERE
    DAY < '{{3. End date}}'
  GROUP BY
    1,
    2,
    3
),
cumulative_balance AS (
  SELECT
    DAY,
    pool,
    token,
    LEAD(DAY, 1, NOW()) OVER (
      PARTITION BY token,
      pool
      ORDER BY
        DAY
    ) AS day_of_next_change,
    SUM(amount) OVER (
      PARTITION BY pool,
      token
      ORDER BY
        DAY ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW
    ) AS cumulative_amount
  FROM
    daily_delta_balance
),
weekly_delta_balance_by_token AS (
  SELECT
    DAY,
    pool,
    token,
    cumulative_amount,
    (
      cumulative_amount - COALESCE(
        LAG(cumulative_amount, 1) OVER (
          PARTITION BY pool,
          token
          ORDER BY
            DAY
        ),
        0
      )
    ) AS amount
  FROM
    (
      SELECT
        DAY,
        pool,
        token,
        SUM(cumulative_amount) AS cumulative_amount
      FROM
        cumulative_balance b
      WHERE
        extract(
          dow
          FROM
            DAY
        ) = 1
      GROUP BY
        1,
        2,
        3
    ) foo
),
cumulative_usd_balance AS (
  SELECT
    c.day,
    b.pool,
    b.token,
    cumulative_amount,
    cumulative_amount / 10 ^ t.decimals * p1.price AS amount_usd_from_api,
    cumulative_amount / 10 ^ t.decimals * p2.price AS amount_usd_from_dex
  FROM
    calendar c
    LEFT JOIN cumulative_balance b ON b.day <= c.day
    AND c.day < b.day_of_next_change
    LEFT JOIN erc20.tokens t ON t.contract_address = b.token
    LEFT JOIN prices p1 ON p1.day = b.day
    AND p1.token = b.token
    LEFT JOIN dex_prices p2 ON p2.day <= c.day
    AND c.day < p2.day_of_next_change
    AND p2.token = b.token
),
estimated_pool_liquidity AS (
  SELECT
    DAY,
    pool,
    SUM(
      COALESCE(amount_usd_from_api, amount_usd_from_dex)
    ) AS liquidity
  FROM
    cumulative_usd_balance
  GROUP BY
    1,
    2
),
avg_pool_liquidity AS (
  SELECT
    pool,
    AVG(liquidity) AS tvl
  FROM
    estimated_pool_liquidity
  WHERE
    DAY >= '{{2. Start date}}'
  GROUP BY
    1
)
SELECT
  CONCAT(SUBSTRING(UPPER(l.name), 0, 16)) AS composition,
  COALESCE(amount, 0) AS amount,
  t.tvl,
  s.volume,
  s.volume / t.tvl AS utilization_ratio,
  s.revenues,
  t.tvl / r.usd_amount AS tvl_ratio,
  s.volume / r.usd_amount AS volume_ratio,
  s.revenues / r.usd_amount AS revenues_ratio
FROM
  swaps s
  LEFT JOIN avg_pool_liquidity t ON t.pool = s.pool_id
  LEFT JOIN mainnet_rewards r ON r.pool_id = s.pool_id
  LEFT JOIN labels l ON l.address = SUBSTRING(s.pool_id, 0, 21)
WHERE
  amount > 0
ORDER BY
  2 DESC,
  3 DESC