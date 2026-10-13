SELECT
    DATE(t.txn_timestamp) AS txn_date,
    pg.gateway_name,
    pg.device_type,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN pg.gateway_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_transactions,
    SUM(CASE WHEN pg.gateway_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_transactions,
    AVG(TRY_CAST(pg.processing_time_ms AS DOUBLE)) AS avg_processing_time_ms
FROM {{ source('silver', 'transactions') }} t
JOIN {{ source('silver', 'payment_gateway_logs') }} pg
    ON CAST(t.txn_id AS STRING) = CAST(pg.txn_id AS STRING)
GROUP BY
    DATE(t.txn_timestamp),
    pg.gateway_name,
    pg.device_type
