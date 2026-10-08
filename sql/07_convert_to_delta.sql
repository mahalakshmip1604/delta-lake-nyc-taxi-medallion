-- sql/07_convert_to_delta.sql
USE SCHEMA nyc_taxi_demo;

-- Create a small Parquet table (data is from 2016, so we filter by that range)
CREATE OR REPLACE TABLE parquet_subset
USING PARQUET
AS
SELECT pickup_date, payment_type, fare_amount
FROM silver_trips
WHERE pickup_date >= DATE'2016-02-15';

-- Before: format should be 'parquet'
DESCRIBE DETAIL parquet_subset;

-- Upgrade in place
CONVERT TO DELTA parquet_subset;

-- After: format should be 'delta'
DESCRIBE DETAIL parquet_subset;
DESCRIBE HISTORY parquet_subset;