-- sql/00_setup.sql
USE CATALOG workspace;                       -- change to your catalog
CREATE SCHEMA IF NOT EXISTS nyc_taxi_demo
  COMMENT 'Delta Lake mini-project: NYC taxi medallion demo';
USE SCHEMA nyc_taxi_demo;

-- Look at the first 20 rows
SELECT * FROM samples.nyctaxi.trips LIMIT 20;

-- See columns & types
DESCRIBE TABLE EXTENDED samples.nyctaxi.trips;

-- How big is it, and what dates does it cover?
SELECT COUNT(*) AS row_count,
       MIN(tpep_pickup_datetime) AS first_trip,
       MAX(tpep_pickup_datetime) AS last_trip
FROM samples.nyctaxi.trips;