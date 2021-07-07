CREATE TABLE IF NOT EXISTS training2021.staging_video_games_sales
(
    rank INT64,
    name STRING,
    platform STRING,
    release_year INT64,
    genre STRING,
    publisher STRING,
    NA_sales FLOAT64,
    EU_sales FLOAT64,
    JP_sales FLOAT64,
    Other_sales FLOAT64,
    Global_sales FLOAT64
);

CREATE TABLE training2021.video_games_sales_analysis
(
    platform STRING,
    total_global_sales FLOAT64,
    job_datetime DATETIME
);


