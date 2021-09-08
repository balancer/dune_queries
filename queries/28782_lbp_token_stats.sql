WITH lbp_info AS (
    SELECT
        *
    FROM
        balancer.view_lbps
    WHERE
        name = '{{LBP}}'
),
lbp_token_prices AS (
    SELECT
        date_trunc('hour', block_time) AS HOUR,
        l.token_sold AS token,
        AVG(
            usd_amount /(token_a_amount_raw / 10 ^ COALESCE(decimals, 18))
        ) AS price
    FROM
        dex.trades d
        INNER JOIN lbp_info l ON l.pool = d.exchange_contract_address
        AND l.token_sold = d.token_a_address
        LEFT JOIN erc20.tokens t ON t.contract_address = l.token_sold
    WHERE
        project = 'Balancer'
    GROUP BY
        1,
        2
),
prices AS (
    SELECT
        date_trunc('hour', MINUTE) AS HOUR,
        contract_address AS token,
        AVG(price) AS price
    FROM
        prices.usd
    GROUP BY
        1,
        2
),
joins AS (
    SELECT
        e."to" AS pool,
        date_trunc('hour', e.evt_block_time) AS HOUR,
        e.contract_address AS token,
        sum(value) AS amount
    FROM
        erc20."ERC20_evt_Transfer" e
        INNER JOIN lbp_info l ON l.pool = e."to"
    WHERE
        date_trunc('hour', e.evt_block_time) < date_trunc('hour', l.final_time)
    GROUP BY
        1,
        2,
        3
),
exits AS (
    SELECT
        e."from" AS pool,
        date_trunc('hour', e.evt_block_time) AS HOUR,
        e.contract_address AS token,
        - sum(value) AS amount
    FROM
        erc20."ERC20_evt_Transfer" e
        INNER JOIN lbp_info l ON l.pool = e."from"
    WHERE
        date_trunc('hour', e.evt_block_time) < date_trunc('hour', l.final_time)
    GROUP BY
        1,
        2,
        3
),
daily_delta_balance_by_token AS (
    SELECT
        pool,
        HOUR,
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
        HOUR,
        amount,
        SUM(amount) OVER (
            PARTITION BY pool,
            token
            ORDER BY
                HOUR ROWS BETWEEN UNBOUNDED PRECEDING
                AND CURRENT ROW
        ) AS cumulative_amount
    FROM
        daily_delta_balance_by_token
),
cumulative_usd_balance_by_token AS (
    SELECT
        b.hour,
        b.pool,
        b.token,
        b.cumulative_amount,
        COALESCE(
            b.cumulative_amount * p.price,
            b.cumulative_amount * t.price
        ) AS cumulative_amount_usd
    FROM
        cumulative_balance_by_token b
        INNER JOIN lbp_info l ON l.pool = b.pool
        LEFT JOIN lbp_token_prices t ON t.token = b.token
        AND t.hour = b.hour
        LEFT JOIN prices p ON p.token = b.token
        AND p.hour = b.hour
),
foo AS (
    SELECT
        HOUR,
        token,
        symbol,
        cumulative_amount / 10 ^ COALESCE(decimals, 18) AS amount,
        cumulative_amount_usd / 10 ^ COALESCE(decimals, 18) AS amount_usd
    FROM
        cumulative_usd_balance_by_token b
        INNER JOIN lbp_info l ON l.pool = b.pool
        LEFT JOIN erc20.tokens t ON t.contract_address = b.token
    WHERE
        cumulative_amount_usd > 0
    ORDER BY
        2
),
sales_stats AS (
    SELECT
        *
    FROM
        (
            SELECT
                DISTINCT ON (1) token,
                HOUR,
                amount AS initial_amount,
                amount_usd AS initial_amount_usd
            FROM
                foo
            ORDER BY
                1,
                2
        ) f
        JOIN (
            SELECT
                DISTINCT ON (1) token,
                HOUR,
                amount AS final_amount,
                amount_usd AS final_amount_usd
            FROM
                foo
            ORDER BY
                1,
                2 DESC
        ) l USING (token)
)
SELECT
    (final_amount_usd - initial_amount_usd) AS money_raised,
    (
        SELECT
            (initial_amount - final_amount)
        FROM
            sales_stats s
            INNER JOIN lbp_info l ON l.token_sold = s.token
    ) AS token_sold
FROM
    sales_stats s
    INNER JOIN lbp_info l ON l.token_sold != s.token