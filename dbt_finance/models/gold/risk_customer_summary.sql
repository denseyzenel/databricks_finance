SELECT
    risk_grade,
    COUNT(customer_id) AS total_customers,
    AVG(TRY_CAST(credit_score AS DOUBLE)) AS avg_credit_score,
    SUM(TRY_CAST(external_active_loans AS INT)) AS total_external_loans,
    SUM(TRY_CAST(external_overdue_amount AS DECIMAL(18, 2))) AS total_overdue_amount
FROM {{ source('silver', 'credit_bureau_reports') }}
GROUP BY risk_grade
