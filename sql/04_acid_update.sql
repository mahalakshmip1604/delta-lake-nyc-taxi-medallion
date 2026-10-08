-- sql/04_acid_update.sql
USE SCHEMA nyc_taxi_demo;

-- 1. Pick a day that has payment_type = 3 rows
SELECT pickup_date, COUNT(*) AS rows_with_type_3
FROM silver_trips WHERE payment_type = 3
GROUP BY pickup_date ORDER BY rows_with_type_3 DESC LIMIT 5;

-- 2. Inspect the BEFORE state for that day (replace the date with one from above)
SELECT payment_type, COUNT(*) AS trips
FROM silver_trips WHERE pickup_date = DATE'2016-02-01'
GROUP BY payment_type ORDER BY payment_type;

-- 3. The correction (single atomic transaction)
UPDATE silver_trips
SET    payment_type = 1
WHERE  payment_type = 3
  AND  pickup_date  = DATE'2016-02-01';

-- 4. AFTER: type 3 should be gone for that day
SELECT payment_type, COUNT(*) AS trips
FROM silver_trips WHERE pickup_date = DATE'2016-02-01'
GROUP BY payment_type ORDER BY payment_type;

-- 5. Gold is a view, so simply re-query: it already reflects the fix
SELECT * FROM gold_daily_revenue
WHERE pickup_date = DATE'2016-02-01' ORDER BY payment_type;