BEGIN;

DROP MATERIALIZED VIEW IF EXISTS balancer.view_balances;

CREATE MATERIALIZED VIEW balancer.view_balances AS (
    WITH pools AS (
        SELECT
            pool AS pools
        FROM
            balancer."BFactory_evt_LOG_NEW_POOL"
    ),
    joins AS (
        SELECT
            p.pools AS pool,
            date_trunc('day', e.evt_block_time) AS DAY,
            e.contract_address AS token,
            sum(value) AS amount
        FROM
            erc20."ERC20_evt_Transfer" e
            INNER JOIN pools p ON e."to" = p.pools
        GROUP BY
            1,
            2,
            3
    ),
    exits AS (
        SELECT
            p.pools AS pool,
            date_trunc('day', e.evt_block_time) AS DAY,
            e.contract_address AS token,
            - sum(value) AS amount
        FROM
            erc20."ERC20_evt_Transfer" e
            INNER JOIN pools p ON e."from" = p.pools
        GROUP BY
            1,
            2,
            3
    ),
    daily_delta_balance_by_token AS (
        SELECT
            pool,
            DAY,
            token,
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
    cumulative_balance_by_token AS (
        SELECT
            pool,
            token,
            DAY,
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
            daily_delta_balance_by_token
    ),
    calendar AS (
        SELECT
            generate_series(
                '2020-01-01' :: timestamp,
                CURRENT_DATE,
                '1 day' :: INTERVAL
            ) AS DAY
    ),
    running_cumulative_balance_by_token AS (
        SELECT
            c.day,
            pool,
            token,
            cumulative_amount
        FROM
            cumulative_balance_by_token b
            JOIN calendar c ON b.day <= c.day
            AND c.day < b.day_of_next_change
    )
    SELECT
        *
    FROM
        running_cumulative_balance_by_token
);

INSERT INTO
    cron.job(schedule, command)
VALUES
    (
        '*/6 * * * *',
        $ $ REFRESH MATERIALIZED VIEW CONCURRENTLY balancer.view_balances $ $
    ) ON CONFLICT (command) DO
UPDATE
SET
    schedule = EXCLUDED.schedule;

COMMIT;