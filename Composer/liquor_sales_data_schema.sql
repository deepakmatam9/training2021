CREATE TABLE IF NOT EXISTS training2021.staging_liquor_sales
(
  invoice_and_item_number STRING,
  date_col DATE,
  store_number INT64,
  store_name STRING,
  address STRING,
  city STRING,
  zip_code INT64,
  store_location GEOGRAPHY,
  country_number INT64,
  country STRING,
  category INT64,
  category_name STRING,
  vendor_number INT64,
  vendor_name STRING,
  item_number INT64,
  item_description STRING,
  pack INT64,
  bottle_volume_ml INT64,
  state_bottle_cost FLOAT64,
  state_bottle_retail FLOAT64,
  bottles_sold	INT64,
  sale_dollars FLOAT64,
  volume_sold_liters FLOAT64,
  volume_sold_gallons FLOAT64
);


CREATE TABLE training2021.liquor_sales_analysis
(
    store_number INT64,
    store_name STRING,
    total_sales_dollars FLOAT64,
    job_datetime DATETIME
);