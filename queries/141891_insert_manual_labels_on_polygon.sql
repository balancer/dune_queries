DROP TABLE IF EXISTS dune_user_generated.balancer_manual_labels;

CREATE TABLE dune_user_generated.balancer_manual_labels (
    address bytea,
    author text,
    name text,
    TYPE text
);

INSERT INTO
    dune_user_generated.balancer_manual_labels
VALUES
    (
        '\xBA12222222228d8Ba445958a75a0704d566BF2C8',
        'balancerlabs',
        'vault',
        'balancer_source'
    ),
    (
        '\x11111112542d85b3ef69ae05771c2dccff4faa26',
        'balancerlabs',
        '1inch',
        'balancer_source'
    ),
    (
        '\xdef1c0ded9bec7f1a1670819833240f027b25eff',
        'balancerlabs',
        'matcha',
        'balancer_source'
    );

SELECT
    *
FROM
    dune_user_generated.balancer_manual_labels