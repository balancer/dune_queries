WITH supply AS (
    SELECT
        date_trunc('day', evt_block_time) AS DAY,
        SUM(value / 1e18) AS mint
    FROM
        erc20."ERC20_evt_Transfer"
    WHERE
        contract_address = '\xba100000625a3754423978a60c9317c58a424e3d'
        AND "from" = '\x0000000000000000000000000000000000000000'
    GROUP BY
        1
)
SELECT
    DAY,
    SUM(mint) OVER (
        ORDER BY
            DAY
    )
FROM
    supply