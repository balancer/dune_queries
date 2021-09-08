SELECT
  dow,
  sum(cont) AS cont
FROM
  (
    SELECT
      concat(
        EXTRACT(
          DOW
          FROM
            evt_block_time
        ),
        to_char(evt_block_time, 'day')
      ) AS dow,
      count(1) AS cont
    FROM
      balancer."BPool_evt_LOG_EXIT"
    GROUP BY
      1
    UNION
    ALL
    SELECT
      concat(
        EXTRACT(
          DOW
          FROM
            evt_block_time
        ),
        to_char(evt_block_time, 'day')
      ) AS dow,
      count(1) AS cont
    FROM
      balancer."BPool_evt_LOG_JOIN"
    GROUP BY
      1 -- union all
      -- select concat(EXTRACT(DOW FROM evt_block_time),to_char(evt_block_time,'day')) as dow, count(1) as cont
      -- from balancer_v2."Vault_evt_PoolBalanceChanged"
      -- group by 1
  ) t
GROUP BY
  1