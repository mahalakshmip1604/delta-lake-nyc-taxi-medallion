-- sql/02_silver.sql
USE SCHEMA nyc_taxi_demo;

CREATE OR REPLACE TABLE silver_trips
COMMENT 'Cleaned, typed trips. One row per valid taxi trip.'
AS
SELECT
  CAST(tpep_pickup_datetime  AS TIMESTAMP)               AS pickup_ts,
  CAST(tpep_dropoff_datetime AS TIMESTAMP)               AS dropoff_ts,
  CAST(tpep_pickup_datetime  AS DATE)                    AS pickup_date,
  CAST(pickup_zip            AS INT)                     AS pickup_zip,
  CAST(dropoff_zip           AS INT)                     AS dropoff_zip,
  CAST(trip_distance         AS DOUBLE)                  AS trip_distance_mi,
  CAST(fare_amount           AS DECIMAL(10,2))           AS fare_amount,
  -- DEMO ONLY: the sample has no payment_type, so we derive a stable 1..4 value.
  -- 1=Credit card, 2=Cash, 3=No charge, 4=Dispute
  CAST(pmod(hash(tpep_pickup_datetime, pickup_zip, dropoff_zip), 4) + 1 AS INT) AS payment_type,
  ROUND((unix_timestamp(tpep_dropoff_datetime)
       - unix_timestamp(tpep_pickup_datetime)) / 60.0, 2) AS trip_duration_min
FROM bronze_trips
WHERE tpep_pickup_datetime  IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND tpep_dropoff_datetime > tpep_pickup_datetime   -- time must move forward
  AND trip_distance > 0                              -- no zero/negative distance
  AND fare_amount   > 0                              -- no zero/negative fares
  AND pickup_zip IS NOT NULL
  AND dropoff_zip IS NOT NULL;