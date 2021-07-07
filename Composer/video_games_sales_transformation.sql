CREATE TABLE IF NOT EXISTS {TARGET_DATASET}.{TARGET_TABLE}
(
    platform STRING,
    total_global_sales FLOAT64,
    job_datetime DATETIME
);
INSERT INTO {TARGET_DATASET}.{TARGET_TABLE}
SELECT
platform,
SUM(Global_sales) AS total_global_sales,
CURRENT_DATETIME() AS job_datetime
FROM {STAGING_DATASET}.{STAGING_TABLE}
GROUP BY platform;