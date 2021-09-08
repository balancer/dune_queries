WITH transactions AS (
    SELECT
        block_time,
        tx_success
    FROM
        ethereum."traces"
    WHERE
        "to" IN (
            '\x3e66b66fd1d0b02fda6c811da9e0547970db2f21',
            '\x6317c5e82a06e1d8bf200d21f4510ac2c038ac81'
        )
        OR (
            "to" = '\xba12222222228d8ba445958a75a0704d566bf2c8'
            AND SUBSTRING(INPUT, 1, 4) IN ('\x52bbbe29', '\x945bcec9')
        ) -- V2 Vault 
),
successful_transactions AS (
    SELECT
        CASE
            WHEN '{{Aggregation}}' = 'Daily' THEN date_trunc('day', transactions.block_time)
            WHEN '{{Aggregation}}' = 'Weekly' THEN date_trunc('week', transactions.block_time)
        END AS "date",
        count(*) AS COUNT
    FROM
        transactions
    WHERE
        transactions.tx_success = TRUE
    GROUP BY
        1
),
failed_transactions AS (
    SELECT
        CASE
            WHEN '{{Aggregation}}' = 'Daily' THEN date_trunc('day', transactions.block_time)
            WHEN '{{Aggregation}}' = 'Weekly' THEN date_trunc('week', transactions.block_time)
        END AS "date",
        count(*) AS COUNT
    FROM
        transactions
    WHERE
        transactions.tx_success = FALSE
    GROUP BY
        1
)
SELECT
    successful_transactions."date" AS "date",
    successful_transactions.count AS "Successful",
    failed_transactions.count AS "Failed"
FROM
    successful_transactions
    LEFT JOIN failed_transactions ON successful_transactions."date" = failed_transactions."date"