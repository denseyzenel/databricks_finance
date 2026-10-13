WITH txn_daily AS (
    SELECT
        DATE(txn_timestamp) AS txn_date,
        COUNT(txn_id) AS total_transactions,
        SUM(amount) AS total_transaction_amount
    FROM {{ source('silver', 'transactions') }}
    GROUP BY DATE(txn_timestamp)
),

customer_metrics AS (
    SELECT COUNT(DISTINCT customer_id) AS total_customers
    FROM {{ source('silver', 'customers') }}
),

account_metrics AS (
    SELECT
        COUNT(account_id) AS total_accounts,
        SUM(balance) AS total_balance
    FROM {{ source('silver', 'accounts') }}
),

credit_metrics AS (
    SELECT
        AVG(TRY_CAST(credit_score AS DOUBLE)) AS avg_credit_score,
        SUM(CASE WHEN risk_grade = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_customers
    FROM {{ source('silver', 'credit_bureau_reports') }}
)

SELECT
    t.txn_date,
    cm.total_customers,
    am.total_accounts,
    am.total_balance,
    t.total_transactions,
    t.total_transaction_amount,
    cr.avg_credit_score,
    cr.high_risk_customers
FROM txn_daily t
CROSS JOIN customer_metrics cm
CROSS JOIN account_metrics am
CROSS JOIN credit_metrics cr
