-- Forked from https://duneanalytics.com/queries/9798/19495

WITH pools AS (
        SELECT pool as pools
        FROM balancer."BFactory_evt_LOG_NEW_POOL"
    ),
    
    joins AS (
        SELECT date_trunc('day', e.evt_block_time) AS day, e.dst AS lp, p.pools AS pool, SUM(e.amt)/1e18 AS amount
        FROM balancer."BPool_evt_Transfer" e
        INNER JOIN pools p ON e."contract_address" = p.pools
        WHERE e.dst NOT IN ('\x0000000000000000000000000000000000000000', '\x9424b1412450d0f8fc2255faf6046b98213b76bd')
        GROUP BY 1, 2, 3
    ),
    
    exits AS (
        SELECT date_trunc('day', e.evt_block_time) AS day, e.src AS lp, p.pools AS pool, -SUM(e.amt)/1e18 AS amount
        FROM balancer."BPool_evt_Transfer" e
        INNER JOIN pools p ON e."contract_address" = p.pools
        WHERE e.src NOT IN ('\x0000000000000000000000000000000000000000', '\x9424b1412450d0f8fc2255faf6046b98213b76bd')
        GROUP BY 1, 2, 3
    ),
    
    daily_delta_bpt_by_pool AS (
        SELECT day, lp, pool, SUM(COALESCE(amount, 0)) as amount FROM 
        (SELECT *
        FROM joins j 
        UNION ALL
        SELECT * 
        FROM exits e) foo
        GROUP BY 1, 2, 3
    ),
    
    cumulative_bpt_by_pool AS (
        SELECT day, lp, pool, amount, 
        LEAD(day::timestamp, 1, CURRENT_DATE::timestamp) OVER (PARTITION BY lp, pool ORDER BY day) AS next_day,
        SUM(amount) OVER (PARTITION BY lp, pool ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS amount_bpt
        FROM daily_delta_bpt_by_pool
    ),
    
    calendar AS (
        SELECT generate_series('2020-01-01'::timestamp, CURRENT_DATE, '1 day'::interval) AS day
    ),
    
    running_cumulative_bpt_by_pool as (
        SELECT c.day, lp, pool, amount_bpt
        FROM cumulative_bpt_by_pool b
        JOIN calendar c on b.day <= c.day AND c.day < b.next_day
    ),

    lp AS (
    SELECT date_trunc('month', day) AS month, lp
    FROM running_cumulative_bpt_by_pool
    WHERE amount_bpt > 0 
    GROUP BY 1, 2
   )
, mau AS (
        SELECT DISTINCT
            month, -- differentiate only by month, but convert it back to a date  '2020-08-01 00:00:00'
            lp as address
        FROM lp
    ) 
    , new_user AS (
        SELECT address,
               MIN(month) AS first_month
        FROM mau
        GROUP BY 1
    )
    , user_status AS
    (
        SELECT
            COALESCE(mau_now.month,DATE_TRUNC('month', mau_prev.month + '45 days'::INTERVAL)) AS month,
            COALESCE(mau_now.address, mau_prev.address) as user_address,
            CASE WHEN
                    nu.address IS NOT NULL
                    THEN 1 ELSE 0 END AS if_new,                                            -- new user                             √ active
            CASE WHEN
                    nu.address IS NULL                  -- not new user (existing or not yet joined)
                    AND mau_prev.address IS NOT NULL    -- active last month
                    AND mau_now.address IS NULL         -- inactive this month
                    THEN 1 ELSE 0 END AS if_churned,                                        -- we lost this user this month         x inactive
            CASE
                WHEN
                    nu.address IS NULL                  -- not new user (existing or not yet joined)
                    AND mau_prev.address IS NOT NULL    -- active last month
                    AND mau_now.address IS NOT NULL     -- active this month
                    THEN 1 ELSE 0 END AS if_retained,                                       -- we retained this user this month     √ active
            CASE
                WHEN
                    nu.address IS NULL                  -- not new user (existing or not yet joined)
                    AND mau_prev.address IS NULL        -- inactive last month
                    AND mau_now.address IS NOT NULL     -- active this month
                    THEN 1 ELSE 0 END AS if_resurrected,                                    -- this user returned this month        √ active
            CASE WHEN
                    mau_now.address IS NOT NULL
                    THEN 1 ELSE 0 END AS if_active                                         -- active flag for completence check: passed check √
                                                                                          -- sum(if_new + if_retained + if_resurrected)=sum(if_active) group by month
                                                                                          -- sum(if_churned + if_active)=count(distinct user_address) group by month
                                                                                          -- sum(if_new + if_churned + if_retained + if_resurrected)=1 group by month, user_address
        FROM mau mau_now
        FULL JOIN mau mau_prev ON
            mau_prev.month = DATE_TRUNC('month', mau_now.month - '5 days'::INTERVAL)
            AND mau_prev.address = mau_now.address
        LEFT JOIN new_user nu ON nu.address = mau_now.address AND nu.first_month = mau_now.month
        WHERE
            COALESCE(mau_now.month,DATE_TRUNC('month', mau_prev.month + '45 days'::INTERVAL)) < CURRENT_DATE
    )
    , user_status_pivot as (
        select month, user_address,
            case
                when sum(if_new)        =   1 then 'new'
                when sum(if_churned)    =   1 then 'churned'
                when sum(if_retained)   =   1 then 'retained'
                when sum(if_resurrected)=   1 then 'returned'
                else null end as status
        from user_status
        group by 1,2
    ) , result as (select month, status, count(distinct user_address) as count from user_status_pivot group by 1,2)
    select month, status, case when status ='churned' then -1*count else count end as count  from result




