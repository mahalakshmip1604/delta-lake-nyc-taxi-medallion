-- sql/05_time_travel.sql
USE SCHEMA nyc_taxi_demo;

-- 1. Commit history (newest first). Note the version numbers.
DESCRIBE HISTORY silver_trips;
-- Typically: version 0 = CREATE TABLE AS SELECT, version 1 = UPDATE

-- 2. Read the BEFORE state (version 0)
SELECT payment_type, COUNT(*) AS trips
FROM silver_trips VERSION AS OF 0
WHERE pickup_date = DATE'2016-02-01'
GROUP BY payment_type ORDER BY payment_type;

-- 3. Read the CURRENT state (no VERSION clause)
SELECT payment_type, COUNT(*) AS trips
FROM silver_trips
WHERE pickup_date = DATE'2016-02-01'
GROUP BY payment_type ORDER BY payment_type;

-- 4. Row-level diff: which rows changed?
SELECT cur.pickup_ts, cur.pickup_zip, cur.dropoff_zip,
       old.payment_type AS old_payment_type,
       cur.payment_type AS new_payment_type
FROM silver_trips cur
JOIN silver_trips VERSION AS OF 0 old
  ON  cur.pickup_ts = old.pickup_ts
  AND cur.pickup_zip = old.pickup_zip
  AND cur.dropoff_zip = old.dropoff_zip
  AND cur.fare_amount = old.fare_amount
WHERE cur.payment_type <> old.payment_type
LIMIT 20;