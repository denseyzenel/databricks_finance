WITH customer_branch AS (
    SELECT
        customer_id,
        branch_code
    FROM {{ source('silver', 'customers') }}
),

account_agg AS (
    SELECT
        customer_id,
        COUNT(account_id) AS total_accounts,
        SUM(balance) AS total_balance
    FROM {{ source('silver', 'accounts') }}
    GROUP BY customer_id
),

txn_agg AS (
    SELECT
        a.customer_id,
        COUNT(t.txn_id) AS total_transactions,
        SUM(t.amount) AS total_transaction_amount
    FROM {{ source('silver', 'transactions') }} t
    JOIN {{ source('silver', 'accounts') }} a
        ON t.account_id = a.account_id
    GROUP BY a.customer_id
)

SELECT
    b.branch_code,
    b.branch_name,
    COUNT(DISTINCT cb.customer_id) AS total_customers,
    SUM(a.total_accounts) AS total_accounts,
    SUM(a.total_balance) AS total_deposits,
    SUM(t.total_transactions) AS total_transactions,
    SUM(t.total_transaction_amount) AS total_transaction_amount
FROM {{ source('silver', 'branches') }} b
LEFT JOIN customer_branch cb
    ON b.branch_code = cb.branch_code
LEFT JOIN account_agg a
    ON cb.customer_id = a.customer_id
LEFT JOIN txn_agg t
    ON cb.customer_id = t.customer_id
GROUP BY
    b.branch_code,
    b.branch_name
