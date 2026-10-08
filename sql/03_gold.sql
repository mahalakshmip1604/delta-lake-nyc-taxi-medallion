-- sql/03_gold.sql
USE SCHEMA nyc_taxi_demo;

CREATE OR REPLACE VIEW gold_daily_revenue
COMMENT 'Daily trips, revenue and distance by payment type'
AS
SELECT
  pickup_date,
  payment_type,
  COUNT(*)                        AS trips,
  ROUND(SUM(fare_amount), 2)      AS total_fare_usd,
  ROUND(SUM(trip_distance_mi), 2) AS total_distance_mi,
  ROUND(AVG(fare_amount), 2)      AS avg_fare_usd
FROM silver_trips
GROUP BY pickup_date, payment_type;

-- Explore
SELECT * FROM gold_daily_revenue ORDER BY pickup_date, payment_type LIMIT 20;

-- Revenue per day (for the line chart)
SELECT pickup_date, ROUND(SUM(total_fare_usd), 2) AS revenue
FROM gold_daily_revenue GROUP BY pickup_date ORDER BY pickup_date;