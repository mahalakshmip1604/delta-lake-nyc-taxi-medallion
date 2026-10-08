-- sql/99_cleanup.sql
USE SCHEMA nyc_taxi_demo;
DROP VIEW  IF EXISTS gold_daily_revenue;
DROP TABLE IF EXISTS parquet_subset;
DROP TABLE IF EXISTS silver_trips_demo;
DROP TABLE IF EXISTS silver_trips;
DROP TABLE IF EXISTS bronze_trips;
DROP SCHEMA IF EXISTS nyc_taxi_demo;