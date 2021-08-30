WITH trades AS (
        SELECT date_trunc('month', block_time)::date AS MONTH,
              date(block_time) AS date,
              tx_to AS trader
        FROM dex.trades
        WHERE project = 'Balancer'
   ),
   
   mau AS (
        SELECT DISTINCT
            month, -- differentiate only by month, but convert it back to a date  '2020-08-01 00:00:00'
            trader as address
        FROM trades
    ),
    
    new_user AS (
        SELECT address,
               MIN(month) AS first_month
        FROM mau
        GROUP BY 1
    ),
    
    user_status AS (
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
    ),
    
    user_status_pivot as (
        select month, user_address,
            case
                when sum(if_new)        =   1 then 'new'
                when sum(if_churned)    =   1 then 'churned'
                when sum(if_retained)   =   1 then 'retained'
                when sum(if_resurrected)=   1 then 'returned'
                else null end as status
        from user_status
        group by 1,2
    ),
    
    result as (
        select month, status, count(distinct user_address) as count
        from user_status_pivot
        group by 1,2
)

select month, status, case when status ='churned' then -1*count else count end as count
from result