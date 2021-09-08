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
)
SELECT
    CONCAT(SUBSTRING(UPPER(name), 0, 16)) AS composition
FROM
    labels
WHERE
    address = CONCAT(
        '\', SUBSTRING(' { { 1.Pool ID } } ', 2, 41))::bytea