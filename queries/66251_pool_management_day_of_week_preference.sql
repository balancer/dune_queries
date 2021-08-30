select dow , sum(cont) as cont from (
select concat(EXTRACT(DOW FROM evt_block_time),to_char(evt_block_time,'day')) as dow, count(1) as cont
from balancer."BPool_evt_LOG_EXIT"
group by 1

union all

select concat(EXTRACT(DOW FROM evt_block_time),to_char(evt_block_time,'day')) as dow, count(1) as cont
from balancer."BPool_evt_LOG_JOIN"
group by 1

-- union all

-- select concat(EXTRACT(DOW FROM evt_block_time),to_char(evt_block_time,'day')) as dow, count(1) as cont
-- from balancer_v2."Vault_evt_PoolBalanceChanged"
-- group by 1
) t
group by 1