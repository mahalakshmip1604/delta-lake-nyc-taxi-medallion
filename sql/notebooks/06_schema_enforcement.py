# Databricks notebook source
# MAGIC %md
# MAGIC # Step 6: Delta schema enforcement
# MAGIC Runs on a scratch copy so `silver_trips` stays clean.

# COMMAND ----------
from pyspark.sql import SparkSession, functions as F

# In Databricks, `spark` and `display` already exist; these lines reuse them.
# Locally (VS Code), they make the names defined so Pylance stops warning.
spark = SparkSession.builder.getOrCreate()
display = globals().get("display", lambda df: df.show(truncate=False))

spark.sql("USE CATALOG workspace")   # change to your catalog
spark.sql("USE SCHEMA nyc_taxi_demo")

spark.sql("CREATE OR REPLACE TABLE silver_trips_demo AS SELECT * FROM silver_trips LIMIT 100")
print("Columns before:", spark.table("silver_trips_demo").columns)

# COMMAND ----------
# 1) Bad append: unexpected extra column -> must be rejected
df_bad = spark.table("silver_trips_demo").limit(3).withColumn("extra", F.lit("extra_col"))
try:
    df_bad.write.format("delta").mode("append").saveAsTable("silver_trips_demo")
    print("Unexpected: the write succeeded")
except Exception as e:
    print("Blocked by schema enforcement, as designed:\n")
    print(str(e)[:600])

# COMMAND ----------
# 2) Fix A: drop the unintended column, then append
df_bad.drop("extra").write.format("delta").mode("append").saveAsTable("silver_trips_demo")
print("Rows now:", spark.table("silver_trips_demo").count())  # expect 103

# COMMAND ----------
# 3) Fix B: intentionally evolve the schema
(df_bad.write.format("delta").mode("append")
       .option("mergeSchema", "true").saveAsTable("silver_trips_demo"))
print("Columns after:", spark.table("silver_trips_demo").columns)

# COMMAND ----------
display(spark.sql("DESCRIBE HISTORY silver_trips_demo"))