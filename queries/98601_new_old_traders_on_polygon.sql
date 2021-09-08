SELECT
    ssq.time,
    new_users AS "New",
    (unique_users - new_users) AS "Old"
FROM
    (
        SELECT
            sq.time,
            COUNT(*) AS new_users
        FROM
            (
                SELECT
                    (funds ->> 'sender') AS unique_users,
                    CASE
                        WHEN '{{Aggregation}}' = 'Daily' THEN MIN(date_trunc('day', call_block_time))
                        ELSE MIN(date_trunc('week', call_block_time))
                    END AS time
                FROM
                    balancer_v2."Vault_call_swap"
                WHERE
                    call_success
                GROUP BY
                    1
                ORDER BY
                    1
            ) sq
        GROUP BY
            1
    ) ssq
    LEFT JOIN (
        SELECT
            CASE
                WHEN '{{Aggregation}}' = 'Daily' THEN date_trunc('day', call_block_time)
                ELSE date_trunc('week', call_block_time)
            END AS time,
            COUNT(DISTINCT (funds ->> 'sender')) AS unique_users
        FROM
            balancer_v2."Vault_call_swap"
        WHERE
            call_success
        GROUP BY
            1
        ORDER BY
            1
    ) t2 ON t2.time = ssq.time
ORDER BY
    1 DESC