WITH labels AS (
    SELECT
        address,
        label AS name
    FROM
        dune_user_generated."balancer_pools"
    WHERE
        "type" = 'balancer_v2_pool'
    GROUP BY
        1,
        2
)
SELECT
    CONCAT(SUBSTRING(UPPER(name), 0, 16)) AS composition
FROM
    labels
WHERE
    address = CONCAT(
        '\', SUBSTRING(' { { 1.Pool ID } } ', 2, 41))::bytea