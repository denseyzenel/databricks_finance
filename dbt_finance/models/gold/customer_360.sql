WITH account_agg AS (
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
),

credit_latest AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY bureau_pull_date DESC
            ) AS rn
        FROM {{ source('silver', 'credit_bureau_reports') }}
    )
    WHERE rn = 1
)

SELECT
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    b.branch_name,
    COALESCE(a.total_accounts, 0) AS total_accounts,
    COALESCE(a.total_balance, 0) AS total_balance,
    COALESCE(t.total_transactions, 0) AS total_transactions,
    COALESCE(t.total_transaction_amount, 0) AS total_transaction_amount,
    TRY_CAST(cr.credit_score AS DOUBLE) AS credit_score,
    cr.risk_grade,
    TRY_CAST(cr.external_active_loans AS INT) AS external_active_loans,
    TRY_CAST(cr.external_overdue_amount AS DECIMAL(18, 2)) AS external_overdue_amount,
    CASE
        WHEN a.total_balance >= 500000 THEN 'HIGH_VALUE'
        WHEN a.total_balance >= 100000 THEN 'MEDIUM_VALUE'
        ELSE 'LOW_VALUE'
    END AS customer_segment
FROM {{ source('silver', 'customers') }} c
LEFT JOIN account_agg a
    ON c.customer_id = a.customer_id
LEFT JOIN txn_agg t
    ON c.customer_id = t.customer_id
LEFT JOIN {{ source('silver', 'branches') }} b
    ON c.branch_code = b.branch_code
LEFT JOIN credit_latest cr
    ON c.customer_id = cr.customer_id
