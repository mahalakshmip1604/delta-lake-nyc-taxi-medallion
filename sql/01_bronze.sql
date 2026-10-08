-- sql/01_bronze.sql
USE SCHEMA nyc_taxi_demo;

CREATE OR REPLACE TABLE bronze_trips
COMMENT 'Raw copy of samples.nyctaxi.trips. No cleaning applied.'
AS
SELECT
  *,
  current_timestamp()          AS _ingested_at,   -- when we loaded it
  'samples.nyctaxi.trips'      AS _source_table   -- where it came from
FROM samples.nyctaxi.trips;

-- Verify: counts must match the source exactly
SELECT
  (SELECT COUNT(*) FROM samples.nyctaxi.trips) AS source_rows,
  (SELECT COUNT(*) FROM bronze_trips)          AS bronze_rows;