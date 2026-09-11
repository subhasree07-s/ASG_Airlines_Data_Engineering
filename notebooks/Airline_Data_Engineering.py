# Databricks notebook source
display(dbutils.fs.ls("abfss://raw@asgairlines007.dfs.core.windows.net/"))

# COMMAND ----------

files = dbutils.fs.ls("abfss://raw@asgairlines007.dfs.core.windows.net/")
display(files)

# COMMAND ----------

print(files[0].path)


# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

display(dbutils.fs.ls("abfss://processed@asgairlines007.dfs.core.windows.net/"))

# COMMAND ----------

df = spark.read.option("header", "true").option("inferSchema", "true").csv(
    "abfss://processed@asgairlines007.dfs.core.windows.net/flights.csv"
)

display(df)

# COMMAND ----------

print("Rows:", df.count())
print("Columns:", len(df.columns))
print("Column names:", df.columns)

display(df.describe())

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, sum, when

print("Missing values:")
missing = df.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df.columns
])
display(missing)

print("Duplicate rows:", df.count() - df.dropDuplicates().count())

print("Flight ID examples:")
display(df.select("flight_id").distinct().limit(20))

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, when, hour, minute, concat, lit
)

# 1. Remove exact duplicate records
cleaned_df = df.dropDuplicates()

# 2. Standardize text fields
cleaned_df = (
    cleaned_df
    .withColumn("flight_id", trim(col("flight_id")))
    .withColumn("airline", trim(col("airline")))
    .withColumn("source", trim(col("source")))
    .withColumn("destination", trim(col("destination")))
)

# 3. Replace missing/blank airline with Unknown
cleaned_df = cleaned_df.withColumn(
    "airline",
    when(col("airline").isNull() | (col("airline") == ""), "Unknown")
    .otherwise(col("airline"))
)

# 4. Calculate departure and arrival in minutes
dep_minutes = hour("departure_time") * 60 + minute("departure_time")
arr_minutes = hour("arrival_time") * 60 + minute("arrival_time")

# 5. Calculate flight duration, handling overnight flights
cleaned_df = cleaned_df.withColumn(
    "duration_minutes",
    when(arr_minutes < dep_minutes,
         arr_minutes + 1440 - dep_minutes)
    .otherwise(arr_minutes - dep_minutes)
)

# 6. Create route
cleaned_df = cleaned_df.withColumn(
    "route",
    concat(col("source"), lit(" - "), col("destination"))
)

# 7. Flag zero-duration flights as anomalies
cleaned_df = cleaned_df.withColumn(
    "is_anomaly",
    when(col("duration_minutes") == 0, True).otherwise(False)
)

# 8. Validate flight ID format
cleaned_df = cleaned_df.withColumn(
    "invalid_flight_id",
    ~col("flight_id").rlike("^[A-Za-z0-9]{5}$")
)

display(cleaned_df)

# COMMAND ----------

display(
    cleaned_df.select(
        "flight_id",
        "departure_time",
        "arrival_time",
        "duration_minutes",
        "route",
        "is_anomaly"
    ).limit(20)
)

# COMMAND ----------

output_path = "abfss://processed@asgairlines007.dfs.core.windows.net/cleaned_flights"

cleaned_df.write.mode("overwrite").option("header", "true").csv(output_path)

print("Cleaned dataset saved successfully.")
print("Records:", cleaned_df.count())

# COMMAND ----------

from pyspark.sql.functions import avg, count, desc

print("=== KPI SUMMARY ===")

print("Total Flights:", cleaned_df.count())

print("Average Flight Duration (minutes):")
display(cleaned_df.select(avg("duration_minutes").alias("avg_duration_minutes")))

print("Flights by Airline:")
display(
    cleaned_df.groupBy("airline")
    .count()
    .orderBy(desc("count"))
)

print("Top Routes:")
display(
    cleaned_df.groupBy("route")
    .count()
    .orderBy(desc("count"))
)

print("Anomalies:")
display(
    cleaned_df.groupBy("is_anomaly")
    .count()
)

# COMMAND ----------

from pyspark.sql.functions import lower, trim, when, col

cleaned_df = cleaned_df.withColumn(
    "airline",
    when(
        col("airline").isNull() |
        (trim(col("airline")) == "") |
        (lower(trim(col("airline"))) == "unknown"),
        "Unknown"
    ).otherwise(trim(col("airline")))
)

# Save corrected cleaned dataset
cleaned_df.write.mode("overwrite").option("header", "true").csv(
    "abfss://processed@asgairlines007.dfs.core.windows.net/cleaned_flights"
)

print("Final cleaned records:", cleaned_df.count())

display(
    cleaned_df.groupBy("airline")
    .count()
    .orderBy(col("count").desc())
)


# COMMAND ----------

final_path = "abfss://processed@asgairlines007.dfs.core.windows.net/final_flights"

cleaned_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(final_path)

print("Final Power BI dataset created.")

# COMMAND ----------

display(dbutils.fs.ls(final_path))

# COMMAND ----------

part_file = [f.path for f in dbutils.fs.ls(final_path) if f.name.startswith("part-")][0]

dbutils.fs.cp(
    part_file,
    "abfss://processed@asgairlines007.dfs.core.windows.net/final_flights.csv"
)

print("CSV ready for Power BI")