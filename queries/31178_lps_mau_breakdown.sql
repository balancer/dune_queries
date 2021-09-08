-- Forked from https://duneanalytics.com/queries/9798/19495
WITH pools AS (
    SELECT
        pool AS pools
    FROM
        balancer."BFactory_evt_LOG_NEW_POOL"
),
joins AS (
    SELECT
        date_trunc('day', e.evt_block_time) AS DAY,
        e.dst AS lp,
        p.pools AS pool,
        SUM(e.amt) / 1e18 AS amount
    FROM
        balancer."BPool_evt_Transfer" e
        INNER JOIN pools p ON e."contract_address" = p.pools
    WHERE
        e.dst NOT IN (
            '\x0000000000000000000000000000000000000000',
            '\x9424b1412450d0f8fc2255faf6046b98213b76bd'
        )
    GROUP BY
        1,
        2,
        3
),
exits AS (
    SELECT
        date_trunc('day', e.evt_block_time) AS DAY,
        e.src AS lp,
        p.pools AS pool,
        - SUM(e.amt) / 1e18 AS amount
    FROM
        balancer."BPool_evt_Transfer" e
        INNER JOIN pools p ON e."contract_address" = p.pools
    WHERE
        e.src NOT IN (
            '\x0000000000000000000000000000000000000000',
            '\x9424b1412450d0f8fc2255faf6046b98213b76bd'
        )
    GROUP BY
        1,
        2,
        3
),
daily_delta_bpt_by_pool AS (
    SELECT
        DAY,
        lp,
        pool,
        SUM(COALESCE(amount, 0)) AS amount
    FROM
        (
            SELECT
                *
            FROM
                joins j
            UNION
            ALL
            SELECT
                *
            FROM
                exits e
        ) foo
    GROUP BY
        1,
        2,
        3
),
cumulative_bpt_by_pool AS (
    SELECT
        DAY,
        lp,
        pool,
        amount,
        LEAD(DAY :: timestamp, 1, CURRENT_DATE :: timestamp) OVER (
            PARTITION BY lp,
            pool
            ORDER BY
                DAY
        ) AS next_day,
        SUM(amount) OVER (
            PARTITION BY lp,
            pool
            ORDER BY
                DAY ROWS BETWEEN UNBOUNDED PRECEDING
                AND CURRENT ROW
        ) AS amount_bpt
    FROM
        daily_delta_bpt_by_pool
),
calendar AS (
    SELECT
        generate_series(
            '2020-01-01' :: timestamp,
            CURRENT_DATE,
            '1 day' :: INTERVAL
        ) AS DAY
),
running_cumulative_bpt_by_pool AS (
    SELECT
        c.day,
        lp,
        pool,
        amount_bpt
    FROM
        cumulative_bpt_by_pool b
        JOIN calendar c ON b.day <= c.day
        AND c.day < b.next_day
),
lp AS (
    SELECT
        date_trunc('month', DAY) AS MONTH,
        lp
    FROM
        running_cumulative_bpt_by_pool
    WHERE
        amount_bpt > 0
    GROUP BY
        1,
        2
),
mau AS (
    SELECT
        DISTINCT MONTH,
        -- differentiate only by month, but convert it back to a date  '2020-08-01 00:00:00'
        lp AS address
    FROM
        lp
),
new_user AS (
    SELECT
        address,
        MIN(MONTH) AS first_month
    FROM
        mau
    GROUP BY
        1
),
user_status AS (
    SELECT
        COALESCE(
            mau_now.month,
            DATE_TRUNC('month', mau_prev.month + '45 days' :: INTERVAL)
        ) AS MONTH,
        COALESCE(mau_now.address, mau_prev.address) AS user_address,
        CASE
            WHEN nu.address IS NOT NULL THEN 1
            ELSE 0
        END AS if_new,
        -- new user                             √ active
        CASE
            WHEN nu.address IS NULL -- not new user (existing or not yet joined)
            AND mau_prev.address IS NOT NULL -- active last month
            AND mau_now.address IS NULL -- inactive this month
            THEN 1
            ELSE 0
        END AS if_churned,
        -- we lost this user this month         x inactive
        CASE
            WHEN nu.address IS NULL -- not new user (existing or not yet joined)
            AND mau_prev.address IS NOT NULL -- active last month
            AND mau_now.address IS NOT NULL -- active this month
            THEN 1
            ELSE 0
        END AS if_retained,
        -- we retained this user this month     √ active
        CASE
            WHEN nu.address IS NULL -- not new user (existing or not yet joined)
            AND mau_prev.address IS NULL -- inactive last month
            AND mau_now.address IS NOT NULL -- active this month
            THEN 1
            ELSE 0
        END AS if_resurrected,
        -- this user returned this month        √ active
        CASE
            WHEN mau_now.address IS NOT NULL THEN 1
            ELSE 0
        END AS if_active -- active flag for completence check: passed check √
        -- sum(if_new + if_retained + if_resurrected)=sum(if_active) group by month
        -- sum(if_churned + if_active)=count(distinct user_address) group by month
        -- sum(if_new + if_churned + if_retained + if_resurrected)=1 group by month, user_address
    FROM
        mau mau_now FULL
        JOIN mau mau_prev ON mau_prev.month = DATE_TRUNC('month', mau_now.month - '5 days' :: INTERVAL)
        AND mau_prev.address = mau_now.address
        LEFT JOIN new_user nu ON nu.address = mau_now.address
        AND nu.first_month = mau_now.month
    WHERE
        COALESCE(
            mau_now.month,
            DATE_TRUNC('month', mau_prev.month + '45 days' :: INTERVAL)
        ) < CURRENT_DATE
),
user_status_pivot AS (
    SELECT
        MONTH,
        user_address,
        CASE
            WHEN sum(if_new) = 1 THEN 'new'
            WHEN sum(if_churned) = 1 THEN 'churned'
            WHEN sum(if_retained) = 1 THEN 'retained'
            WHEN sum(if_resurrected) = 1 THEN 'returned'
            ELSE NULL
        END AS STATUS
    FROM
        user_status
    GROUP BY
        1,
        2
),
result AS (
    SELECT
        MONTH,
        STATUS,
        count(DISTINCT user_address) AS count
    FROM
        user_status_pivot
    GROUP BY
        1,
        2
)
SELECT
    MONTH,
    STATUS,
    CASE
        WHEN STATUS = 'churned' THEN -1 * count
        ELSE count
    END AS count
FROM
    result