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
),
pools AS (
    SELECT
        pool
    FROM
        balancer_v2."WeightedPoolFactory_evt_PoolCreated"
    UNION
    ALL
    SELECT
        pool
    FROM
        balancer_v2."WeightedPool2TokensFactory_evt_PoolCreated"
)
SELECT
    CONCAT(SUBSTRING(UPPER(l.name), 0, 15)) AS composition,
    CONCAT(
        '<a href="https://duneanalytics.com/balancerlabs/Balancer-V2-LP-Revenues?Pool%20address=0',
        SUBSTRING(pool :: text, 2),
        '">view stats</a>'
    ) AS stats,
    CONCAT(
        '<a target="_blank" href="https://app.balancer.fi/#/pool/0',
        SUBSTRING(r."poolId" :: text, 2),
        '">view pool</a>'
    ) AS pool,
    CONCAT(
        '<a target="_blank" href="https://etherscan.io/address/0',
        SUBSTRING(pool :: text, 2, 42),
        '">0',
        SUBSTRING(pool :: text, 2, 42),
        '</a>'
    ) AS etherscan
FROM
    pools p
    INNER JOIN balancer_v2."Vault_evt_PoolRegistered" r ON p.pool = r."poolAddress"
    LEFT JOIN labels l ON l.address = p.pool