
# NYC Taxi Lakehouse with Delta Lake (Bronze → Silver → Gold)

Hands-on mini-project on **Databricks**. Build a medallion pipeline over the NYC Taxi sample data and try the core Delta Lake features yourself: **ACID updates, time travel, schema enforcement, and `CONVERT TO DELTA`.**


|                    |                                                                        |
| ------------------ | ---------------------------------------------------------------------- |
| **Skills**   | SQL, a little PySpark                                                  |
| **Platform** | Databricks (SQL Editor + Notebook). Works on Free Edition / serverless |

---

## Table of Contents

1. [What you&#39;ll build](#1-what-youll-build)
2. [Key concepts (read first)](#2-key-concepts-read-first)
3. [Prerequisites &amp; setup](#3-prerequisites--setup)
4. [Repository layout](#4-repository-layout)
5. [Step 0 – Inspect the source](#step-0--inspect-the-source)
6. [Step 1 – Bronze](#step-1--bronze-raw-copy-into-delta)
7. [Step 2 – Silver](#step-2--silver-clean--typed--renamed)
8. [Step 3 – Gold](#step-3--gold-business-aggregates)
9. [Step 4 – ACID UPDATE](#step-4--delta-acid-update)
10. [Step 5 – Time travel](#step-5--delta-time-travel)
11. [Step 6 – Schema enforcement](#step-6--delta-schema-enforcement)
12. [Step 7 – CONVERT TO DELTA](#step-7--convert-parquet-to-delta)
13. [Cleanup](#cleanup)
14. [Troubleshooting](#troubleshooting)
15. [Production notes](#production-notes)
16. [Glossary](#glossary)

---

## 1. What you'll 

```
samples.nyctaxi.trips          (shared, read-only sample data)
        │  copy
        ▼
BRONZE  bronze_trips           raw copy + ingestion metadata
        │  clean, rename, cast, filter
        ▼
SILVER  silver_trips           trusted, analytics-friendly table
        │  aggregate
        ▼
GOLD    gold_daily_revenue     view for dashboards (revenue by day / payment type)
```

**Layers in plain English**

| Layer            | Purpose                | Rule of thumb                               |
| ---------------- | ---------------------- | ------------------------------------------- |
| **Bronze** | Raw data, kept as-is   | Never "fix" data here; it's your safety net |
| **Silver** | Cleaned and typed      | Good names, correct types, bad rows removed |
| **Gold**   | Business-ready numbers | Small, aggregated, fast for dashboards      |

---

## 2. Key concepts (read first)

- **Delta Lake**: a storage layer that adds a transaction log (`_delta_log`) on top of Parquet files. That log is what enables everything below.
- **ACID**: Atomic, Consistent, Isolated, Durable. An `UPDATE` either fully succeeds or fully fails, and readers never see half-finished changes.
- **Time travel**: every change creates a new table *version*. You can query or restore old versions.
- **Schema enforcement**: Delta rejects writes whose columns/types don't match the table. This protects you from silently corrupting data.
- **Managed table**: Databricks controls where the files live. Dropping the table drops the data.
- **Medallion architecture**: the Bronze/Silver/Gold layering pattern above.

---

## 3. Prerequisites & setup

1. A Databricks workspace (the free edition is enough).
2. Access to the sample table `samples.nyctaxi.trips`.
3. A SQL warehouse (for `.sql` files) and a notebook attached to compute (for the `.py` notebook).
4. Permission to create a schema in a catalog you own.

> **Which catalog?** Run `SELECT current_catalog();` and use that. On Free Edition it is usually `workspace`. Replace the catalog in the `USE CATALOG` line below if yours differs.

### Important: the sample data

Run **Step 0** before anything else. The sample table `samples.nyctaxi.trips` is a small slice of yellow-taxi data from **early 2016** and, in most workspaces, has only these columns:

`tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, fare_amount, pickup_zip, dropoff_zip`

It does **not** contain `payment_type` or `passenger_count`. Two consequences, both handled in this project:

1. **Dates are 2016.** Examples use 2016 dates, not "last 14 days".
2. **`payment_type` is synthetic.** Silver generates a deterministic demo value (1–4) so Steps 3–5 have something to analyse. This is **demo only**. If your table has a real `payment_type`, use it instead (see the note in Step 2).

---

## 4. Repository layout

```
nyc-taxi-delta-lakehouse/
├── README.md
├── sql/
│   ├── 00_setup.sql
│   ├── 01_bronze.sql
│   ├── 02_silver.sql
│   ├── 03_gold.sql
│   ├── 04_acid_update.sql
│   ├── 05_time_travel.sql
│   ├── 07_convert_to_delta.sql
│   └── 99_cleanup.sql
└── notebooks/
    └── 06_schema_enforcement.py
```

Run the files in numeric order. Each file is also copy-pasted in the steps below.

---

## Step 0 – Inspect the source

**What:** Look before you build. Always check column names, types and row counts first.
**Why:** Real data rarely matches what you assume. This step catches mismatches early.

```sql
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
```

**Expected:** about 21–22 thousand rows, with dates in early 2016.
**You learned:** how to profile a source table.

---

## Step 1 – Bronze: raw copy into Delta

**What:** Copy the shared sample into a table **you own**.
**Why:** The sample is read-only and shared. A table you own supports `UPDATE`, `DELETE`, time travel, and is safe to experiment on.

```sql
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
```

**Expected:** `source_rows = bronze_rows`.
**Notes:** The two `_` columns are *lineage metadata*, a common production habit. Tables created with `CREATE TABLE ... AS SELECT` in Databricks are Delta by default.
**You learned:** you now control the data (ACID, time travel, updates) without touching the shared sample.

---

## Step 2 – Silver: clean, typed, renamed

**What:** Rename columns to meaningful names, enforce data types, and drop obviously bad rows.
**Why:** Analysts should never have to guess what `tpep_dropoff_datetime` means or worry about negative fares.

```sql
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
```

> **Have a real `payment_type` column?** Replace the `pmod(hash(...))` line with
> `CAST(payment_type AS INT) AS payment_type`, and add `passenger_count`, `total_amount`, etc. as needed.

**Quick checks**

```sql
-- How many rows did the quality filters remove?
SELECT
  (SELECT COUNT(*) FROM bronze_trips) AS bronze_rows,
  (SELECT COUNT(*) FROM silver_trips) AS silver_rows,
  (SELECT COUNT(*) FROM bronze_trips) - (SELECT COUNT(*) FROM silver_trips) AS rows_removed;

-- Confirm the data types
DESCRIBE TABLE silver_trips;

-- Group by pickup date and look at the first 20 days
SELECT pickup_date, COUNT(*) AS trips
FROM silver_trips
GROUP BY pickup_date
ORDER BY pickup_date
LIMIT 20;
```

**Expected:** `silver_rows <= bronze_rows`; `pickup_ts` is `timestamp`, `fare_amount` is `decimal(10,2)`.
**You learned:** Silver = trusted data. Bronze stays untouched, so you can always rebuild Silver.

---

## Step 3 – Gold: business aggregates

**What:** A **view** summarising revenue and distance per day and payment type.
**Why:** Dashboards should read small, pre-aggregated data. A view stores no data, so it always reflects the latest Silver.

```sql
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
```

**Visualise** (SQL Editor → **+ Visualization**):

- **Line chart:** `pickup_date` (X) vs `revenue` (Y)
- **Bar chart:** `payment_type` (X) vs `trips` (Y). Filter to one month.

**You learned:** Gold is for consumption: small, aggregated, business-friendly.

---

## Step 4 – Delta ACID UPDATE

**Scenario:** On one day, trips were mis-coded as `3` (No charge) but should be `1` (Credit card). Fix it in place.

```sql
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
```

**Expected:** the row count for type `1` grows by exactly the count that type `3` had; total rows are unchanged.
**Why it's safe:** Delta writes new files and commits one entry to the log. If it fails midway, nothing changes. Readers see either the old or new version, never a mix.
**You learned:** in-place changes with ACID guarantees; downstream queries pick up the latest version.

---

## Step 5 – Delta Time Travel

**What:** Read the table *as it was* before the update.

```sql
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
```

**Optional – undo the change completely:**

```sql
RESTORE TABLE silver_trips TO VERSION AS OF 0;
```

`RESTORE` is itself a new commit, so history is preserved.

> **Retention:** old files are kept until `VACUUM` removes them (default 7 days). After that, those versions can no longer be read. Don't treat time travel as a long-term backup.

**You learned:** point-in-time reads to audit and cross-check changes.

---

## Step 6 – Delta Schema Enforcement

**What:** Try to write data with an unexpected column and watch Delta block it.
**Where:** Notebook `notebooks/06_schema_enforcement.py` (Python).

To keep `silver_trips` clean, the demo runs on a small **scratch copy**.

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Step 6: Schema enforcement

# COMMAND ----------
from pyspark.sql import functions as F

spark.sql("USE CATALOG workspace")        # change to your catalog
spark.sql("USE SCHEMA nyc_taxi_demo")

# Scratch copy so we never pollute silver_trips
spark.sql("""
  CREATE OR REPLACE TABLE silver_trips_demo AS
  SELECT * FROM silver_trips LIMIT 100
""")
print("Columns before:", spark.table("silver_trips_demo").columns)

# COMMAND ----------
# 1) BAD APPEND: same rows + an unexpected extra column
df_bad = (spark.table("silver_trips_demo").limit(3)
          .withColumn("extra", F.lit("extra_col")))

try:
    df_bad.write.format("delta").mode("append").saveAsTable("silver_trips_demo")
    print("Unexpected: the write succeeded")
except Exception as e:
    print("Blocked by schema enforcement, as designed:\n")
    print(str(e)[:600])

# COMMAND ----------
# 2) FIX A: you did NOT intend the new column -> drop it and append
df_bad.drop("extra").write.format("delta").mode("append").saveAsTable("silver_trips_demo")
print("Rows now:", spark.table("silver_trips_demo").count())    # expect 103

# COMMAND ----------
# 3) FIX B: you DID intend to evolve the schema -> opt in explicitly
(df_bad.write.format("delta")
       .mode("append")
       .option("mergeSchema", "true")
       .saveAsTable("silver_trips_demo"))
print("Columns after:", spark.table("silver_trips_demo").columns)   # now includes 'extra'

# COMMAND ----------
# 4) Evidence in the log
display(spark.sql("DESCRIBE HISTORY silver_trips_demo"))
```

**Expected:** step 1 raises an `AnalysisException` mentioning a schema mismatch (`extra` not in the table). Fix A works. Fix B works and adds the column, with older rows getting `NULL` for it.

**Rule to remember:**

- Unintended mismatch → Delta **rejects** it (good!).
- Intended change → opt in with `mergeSchema`.
- Wrong *types* (e.g. text into a `DOUBLE` column) are also rejected.

**You learned:** safe appends, with schema changes that are always a deliberate choice.

---

## Step 7 – Convert Parquet to Delta

**What:** Upgrade a plain Parquet table to Delta **in place** (no data rewrite).
**Why:** Teams often inherit Parquet tables. `CONVERT TO DELTA` adds a transaction log over the existing files, instantly unlocking ACID and time travel.

### Option A: Hive-metastore / non-Unity-Catalog workspaces

```sql
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
```

### Option B: Unity Catalog workspaces (path-based)

Unity Catalog managed tables must be Delta, so creating a *managed* Parquet table can fail. Instead, write Parquet files to a storage path you own, then convert that path. This requires an **external location / external volume** you can write to. Behaviour varies by workspace type, so check your docs if it errors.

```python
path = "/Volumes/<catalog>/nyc_taxi_demo/<your_volume>/parquet_subset"   # <- edit

(spark.table("silver_trips")
      .where("pickup_date >= '2016-02-15'")
      .select("pickup_date", "payment_type", "fare_amount")
      .write.mode("overwrite").parquet(path))

spark.sql(f"CONVERT TO DELTA parquet.`{path}`")

display(spark.sql(f"DESCRIBE DETAIL delta.`{path}`"))
display(spark.sql(f"DESCRIBE HISTORY delta.`{path}`"))
```

**Expected:** `format` changes from `parquet` to `delta`, and `DESCRIBE HISTORY` shows a `CONVERT` operation.
**Note:** Partitioned Parquet needs `PARTITIONED BY (col TYPE)` in the `CONVERT` statement.
**You learned:** the upgrade pattern, with no copy and no downtime.

---

## Cleanup

```sql
-- sql/99_cleanup.sql
USE SCHEMA nyc_taxi_demo;
DROP VIEW  IF EXISTS gold_daily_revenue;
DROP TABLE IF EXISTS parquet_subset;
DROP TABLE IF EXISTS silver_trips_demo;
DROP TABLE IF EXISTS silver_trips;
DROP TABLE IF EXISTS bronze_trips;
DROP SCHEMA IF EXISTS nyc_taxi_demo;
```

---

## Troubleshooting

| Symptom                                            | Likely cause                       | Fix                                                                      |
| -------------------------------------------------- | ---------------------------------- | ------------------------------------------------------------------------ |
| `Table or view not found: samples.nyctaxi.trips` | Sample catalog unavailable         | Check Catalog Explorer; use another taxi dataset and adjust column names |
| `UNRESOLVED_COLUMN ... payment_type`             | Reading the sample directly        | Use`silver_trips`, where it is derived                                 |
| `UPDATE` changes 0 rows                          | Chosen date has no type-3 rows     | Re-run the "pick a day" query                                            |
| `VERSION AS OF 0` fails                          | History trimmed or table recreated | Run`DESCRIBE HISTORY` and use a listed version                         |
| Step 7 fails on a managed Parquet table            | Unity Catalog                      | Use Option B                                                             |
| Permission denied on`CREATE SCHEMA`              | No rights in that catalog          | Switch catalog or ask an admin                                           |

---

## Production notes

This is a learning project. For a real pipeline, add:

- **Incremental loads** (`MERGE INTO`, Auto Loader or Lakeflow/DLT) instead of `CREATE OR REPLACE`.
- **Constraints**: `ALTER TABLE ... ADD CONSTRAINT fare_positive CHECK (fare_amount > 0)`.
- **Orchestration**: Databricks Workflows/Jobs to run Bronze → Silver → Gold on a schedule.
- **Maintenance**: `OPTIMIZE` and scheduled `VACUUM` (respect retention).
- **Partitioning/clustering**: liquid clustering on `pickup_date` for large tables.
- **Testing & CI**: assertions on row counts and nulls; deploy via Databricks Asset Bundles.
- **Governance**: Unity Catalog permissions, table comments, and lineage.
- **Gold as a table** (materialized view) once data volume grows.

---

## Glossary

| Term                      | Meaning                                            |
| ------------------------- | -------------------------------------------------- |
| **Parquet**         | Columnar file format Delta is built on             |
| **Transaction log** | `_delta_log` folder recording every change       |
| **Version**         | A numbered snapshot of the table after each commit |
| **Schema**          | Column names + data types                          |
| **mergeSchema**     | Option that allows deliberate schema evolution     |
| **VACUUM**          | Deletes old, unreferenced data files               |
| **Medallion**       | Bronze/Silver/Gold layering pattern                |
