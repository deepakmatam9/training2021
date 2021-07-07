CREATE TABLE IF NOT EXISTS {TARGET_DATASET}.{TARGET_TABLE}
(
    store_number INT64,
    store_name STRING,
    total_sales_dollars FLOAT64,
    job_datetime DATETIME
);
INSERT INTO {TARGET_DATASET}.{TARGET_TABLE}
SELECT
store_number,
store_name,
SUM(sale_dollars) AS total_sales_dollars,
CURRENT_DATETIME() AS job_datetime
FROM {STAGING_DATASET}.{STAGING_TABLE}
GROUP BY store_number, store_name;
