# Databricks notebook source
spark.sql("DROP SCHEMA IF EXISTS databricks_7405612194732360.aqi_silver_layer_new CASCADE")

# COMMAND ----------

# Define your specific catalog and schema
catalog = "databricks_7405612194732360"
silver_schema = "aqi_silver_layer_new"

print(f"Cleaning up ghost data in {catalog}.{silver_schema}...")

# Drop the City tables
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{silver_schema}.city_day")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{silver_schema}.quarantine_city_day")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{silver_schema}.city_hour")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{silver_schema}.quarantine_city_hour")

# Drop the Station tables
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{silver_schema}.station_day")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{silver_schema}.quarantine_station_day")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{silver_schema}.station_hour")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{silver_schema}.quarantine_station_hour")

print("Cleanup complete! The Silver layer is now a clean slate.")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

# COMMAND ----------

from pyspark.sql.functions import col, lower, trim

catalog = "databricks_7405612194732360"
bronze_schema1 = "aqi_bronze_layer_new"
silver_schema = "aqi_silver_layer_new"

silver_stations_table = f"{catalog}.{silver_schema}.stations"

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

# Read Bronze
df_bronze_stations = spark.read.table(f"{catalog}.{bronze_schema1}.bronze_stations")

# Transform (kept as requested, but removed SCD columns)
df_transformed = df_bronze_stations.withColumn(
    "City", lower(trim(col("City")))
)

# Check if table exists
table_exists = spark.catalog.tableExists(silver_stations_table)

if not table_exists:
    # INITIAL LOAD
    df_transformed.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(silver_stations_table)

    print("Initial load complete.")

else:
    # SIMPLE OVERWRITE (no SCD)
    df_transformed.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(silver_stations_table)

    print("Table overwritten with latest data.")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_7405612194732360.aqi_bronze_layer_new.bronze_stations limit 10;

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, lower, trim, when, expr, concat_ws, row_number

catalog = "databricks_7405612194732360"
bronze_schema = "bronze_layer"
silver_schema = "aqi_silver_layer_new"



def process_silver_with_quarantine(table_name, is_station_data=False):
    print(f"\n--- Processing: {table_name} ---")

    # ====================================================================
    # STEP 1: Read from Bronze
    # ====================================================================
    df = spark.read.table(f"{catalog}.{bronze_schema}.{table_name}")
    count_initial = df.count()
    time_col = "Date" if "day" in table_name else "Datetime"

    # ====================================================================
    # STEP 2: Standardize data (timestamp, city, formats)
    # ====================================================================
    df = df.withColumn(time_col, to_timestamp(col(time_col)))
    # Ensure City is lowercase to match the new Silver Stations table
    if "City" in df.columns:
        df = df.withColumn("City", lower(trim(col("City"))))

    # ====================================================================
    # STEP 3: Type validation (before casting)
    # ====================================================================
    pollutants = ["`PM2.5`", "PM10", "NO", "NO2", "NOx", "NH3", "CO", "SO2", "O3", "Benzene", "Toluene", "Xylene", "AQI"]
    
    df = df.withColumn("flag_invalid_type", lit(False))
    
    for p in pollutants:
        clean_p = p.replace("`", "")
        if clean_p in df.columns:
            df = df.withColumn("flag_invalid_type", 
                col("flag_invalid_type") | (col(p).isNotNull() & col(p).cast("double").isNull())
            )

    # ====================================================================
    # STEP 4: Cast columns to correct types
    # ====================================================================
    for p in pollutants:
        clean_p = p.replace("`", "")
        if clean_p in df.columns:
            df = df.withColumn(clean_p, col(p).cast("double"))

    # ====================================================================
    # STEP 5: Early hard validation (null PKs, all-null pollutants, invalid timestamps)
    # ====================================================================
    if is_station_data:
        pk_null_check = col(time_col).isNull() | col("StationId").isNull()
    else:
        pk_null_check = col(time_col).isNull() | col("City").isNull()

    df = df.withColumn("flag_null_pks", pk_null_check)
    df = df.withColumn("flag_corrupt_time", col(time_col).isNull() | (col(time_col) > current_timestamp()))
    
    # Critical nulls definition (If AQI is NULL, it gets caught right here!)
    df = df.withColumn("flag_null_critical_metrics",
    col("AQI").isNull() | col("AQI_Bucket").isNull()
    )

    df = df.withColumn("flag_pm_missing",
    col("`PM2.5`").isNull() | col("PM10").isNull()
    )

    # --- OUTPUT REQUIREMENT: Calculate and print specific null counts ---
    count_null_failures = df.filter(col("flag_null_critical_metrics")).count()
    count_after_nulls = count_initial - count_null_failures

    print(f"Count before checking nulls: {count_initial}")
    print(f"Count after quarantining critical nulls: {count_after_nulls}")
    print(f"Rows flagged specifically for nulls: {count_null_failures}")

    # ====================================================================
    # STEP 6: Incremental Load via MAX Timestamp Watermark
    # Logic: Read the MAX value of the event-time column already written
    # to the Silver table. Then filter the Bronze dataframe to only rows
    # AFTER that watermark, so we process only new/updated data each run.
    # On the very first run the Silver table does not exist yet, so we
    # fall through and process the full Bronze dataset (full initial load).
    # ====================================================================
    silver_table_name_check = table_name.replace("bronze_", "")
    silver_table_path_check = f"{catalog}.{silver_schema}.{silver_table_name_check}"

    if spark.catalog.tableExists(silver_table_path_check):
        # Get the highest event-time already present in Silver
        max_ts_row = spark.read.table(silver_table_path_check).agg({time_col: "max"}).collect()[0][0]

        if max_ts_row is not None:
            print(f"Watermark detected — last processed {time_col}: {max_ts_row}")
            # Keep only Bronze rows strictly newer than the watermark
            df = df.filter(col(time_col) > max_ts_row)
            count_incremental = df.count()
            print(f"Incremental rows to process (after watermark filter): {count_incremental}")

            if count_incremental == 0:
                print("No new data beyond watermark. Skipping table.")
                return
        else:
            print("Silver table exists but is empty — processing full Bronze dataset.")
    else:
        print("Silver table does not exist yet — performing full initial load.")

    # ====================================================================
    # STEP 7: Deduplication (using latest record via ingestion timestamp)
    # ====================================================================
    if is_station_data:
        dup_cols = ["StationId", time_col]
    else:
        dup_cols = ["City", time_col]
        
    window_spec = Window.partitionBy(*dup_cols).orderBy(col("ingestion_datetime").desc())
    df = df.withColumn("row_num", row_number().over(window_spec))
    df = df.withColumn("flag_duplicate", col("row_num") > 1).drop("row_num")

    # ====================================================================
    # STEP 8: Advanced validations (range checks, logic checks, FK checks)
    # ====================================================================
    # Updated Range Check: AQI cannot be less than 0 OR greater than 500
    df = df.withColumn("flag_invalid_range", (col("`PM2.5`") < 0) | (col("PM10") < 0) | (col("AQI") < 0) | (col("AQI") > 500))

    # Updated Bucket Logic: Explicit 400 to 500 limit
    df = df.withColumn("calc_bucket",
        when(col("AQI").isNull(), lit(None))
        .when(col("AQI") <= 50, "good")
        .when(col("AQI") <= 100, "satisfactory")
        .when(col("AQI") <= 200, "moderate")
        .when(col("AQI") <= 300, "poor")
        .when(col("AQI") <= 400, "very poor")
        .when((col("AQI") > 400) & (col("AQI") <= 500), "severe")
        .otherwise(lit(None))
    )
    
    df = df.withColumn("flag_bucket_mismatch", 
        col("AQI").isNotNull() & col("AQI_Bucket").isNotNull() & (lower(trim(col("AQI_Bucket"))) != col("calc_bucket"))
    )

    stations_table = f"{catalog}.{silver_schema}.stations"

    if spark.catalog.tableExists(stations_table):

        df_stations = spark.read.table(stations_table) 

        if is_station_data:
            # =========================
            # FK CHECK: StationId
            # =========================
            df_valid = df_stations.select("StationId") \
                .withColumn("is_valid", lit(True))

            df = df.join(df_valid, "StationId", "left")

        else:
            # =========================
            # FK CHECK: City
            # =========================
            df_valid = df_stations.select("City") \
                .dropDuplicates() \
                .withColumn("is_valid", lit(True))

            df = df.join(df_valid, "City", "left")

        # Common flag
        df = df.withColumn("flag_invalid_station", col("is_valid").isNull())
        # Drop helper column immediately
        df = df.drop("is_valid")

    else:
        df = df.withColumn("flag_invalid_station", lit(False))

    # ====================================================================
    # STEP 9: Split data → Clean + Quarantine
    # ====================================================================
    df = df.withColumn(
        "quarantine_reason",
        concat_ws(" | ",
            when(col("flag_invalid_type"), "Invalid Data Type before Casting"),
            when(col("flag_null_pks"), "Missing Primary Keys"),
            when(col("flag_corrupt_time"), "Corrupt or Future Timestamp"),
            when(col("flag_null_critical_metrics"), "Missing Critical Metrics"),
            when(col("flag_invalid_range"), "Negative or Out-of-Bounds Metrics"),
            when(col("flag_bucket_mismatch"), "AQI Bucket Mismatch"),
            when(col("flag_invalid_station"), "Foreign Key Mismatch")
        )
    )

    df_quarantine = df.filter(col("quarantine_reason") != "")
    df_clean = df.filter(col("quarantine_reason") == "")

    drop_flags = [
        "flag_invalid_type", "flag_null_pks", "flag_corrupt_time", "flag_null_critical_metrics", "flag_invalid_range", "calc_bucket", "flag_bucket_mismatch", 
        "flag_invalid_station"
    ]
    df_clean = df_clean.drop(*drop_flags).drop("quarantine_reason")
    df_quarantine = df_quarantine.drop(*drop_flags)

    if is_station_data:
        df_clean = df_clean.drop("City")
    else:
        df_clean = df_clean.drop("StationId")

    # ====================================================================
    # STEP 10: flags + reason columns (Soft Warnings)
    # ====================================================================
    df_clean = df_clean.withColumn("warning_pm25_outlier", col("`PM2.5`") > 1200)
    df_clean = df_clean.withColumn("warning_pm_sensor_error", col("PM10") < col("`PM2.5`"))
    df_clean = df_clean.withColumn("warning_duplicate", col("flag_duplicate"))
    df_clean = df_clean.withColumn("warning_pm_missing", col("flag_pm_missing"))



#drop in final table
    df_clean = df_clean.drop("flag_duplicate", "flag_pm_missing")

    # ====================================================================
    # STEP 11: Write to Silver tables (incremental MERGE for clean data)
    # ====================================================================
    from delta.tables import DeltaTable

    count_clean = df_clean.count()
    count_quarantine = df_quarantine.count()

    print(f"Total clean records written to Silver: {count_clean}")
    print(f"Total bad records routed to Quarantine: {count_quarantine}")

    silver_table_name = table_name.replace("bronze_", "")
    silver_full_name = f"{catalog}.{silver_schema}.{silver_table_name}"

    # Determine merge keys based on data type
    if is_station_data:
        merge_condition = f"target.StationId = source.StationId AND target.{time_col} = source.{time_col}"
    else:
        merge_condition = f"target.City = source.City AND target.{time_col} = source.{time_col}"

    if spark.catalog.tableExists(silver_full_name):
        # INCREMENTAL LOAD: Merge new clean rows into existing Silver table
        # - Matching rows (same PK + timestamp) are updated in place
        # - New rows are inserted
        target = DeltaTable.forName(spark, silver_full_name)
        target.alias("target").merge(
            df_clean.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        print(f"Incremental MERGE complete into: {silver_full_name}")
    else:
        # INITIAL LOAD: First run — write full dataset
        df_clean.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(silver_full_name)
        print(f"Initial load complete into: {silver_full_name}")

    df_quarantine.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{silver_schema}.quarantine_{silver_table_name}")

# Run the drop command manually once before executing this to ensure a clean slate:
#spark.sql("DROP SCHEMA IF EXISTS databricks_7405612194732360.aqi_silver_layer_new CASCADE")
process_silver_with_quarantine("bronze_city_day", is_station_data=False)
process_silver_with_quarantine("bronze_city_hour", is_station_data=False)
process_silver_with_quarantine("bronze_station_day", is_station_data=True)
process_silver_with_quarantine("bronze_station_hour", is_station_data=True)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

catalog = "databricks_7405612194732360"
bronze_schema = "bronze_layer"
silver_schema = "aqi_silver_layer_new"

def verify_silver_layer(table_name):
    print(f"\n{'='*55}")
    print(f"🔍 VERIFYING: {table_name}")
    print(f"{'='*55}")

    silver_table = table_name.replace("bronze_", "")
    time_col = "Date" if "day" in table_name else "Datetime"

    # Read the three tables
    df_bronze = spark.read.table(f"{catalog}.{bronze_schema}.{table_name}")
    df_clean = spark.read.table(f"{catalog}.{silver_schema}.{silver_table}")
    df_quar = spark.read.table(f"{catalog}.{silver_schema}.quarantine_{silver_table}")

    # ---------------------------------------------------------
    # TEST 1: Data Conservation (Did we lose anything?)
    # ---------------------------------------------------------
    bronze_count = df_bronze.count()
    clean_count = df_clean.count()
    quar_count = df_quar.count()
    
    print("\n--- TEST 1: Data Conservation ---")
    print(f"Bronze Input Rows : {bronze_count}")
    print(f"Silver + Quar Rows: {clean_count + quar_count}")
    
    if bronze_count == (clean_count + quar_count):
        print("✅ PASS: 100% of data is accounted for. Nothing was lost.")
    else:
        print("❌ FAIL: Row counts do not match. Data was lost!")

    # ---------------------------------------------------------
    # TEST 2: Clean Table Purity
    # ---------------------------------------------------------
    print("\n--- TEST 2: Clean Table Purity ---")
    
    # Negative values check
    negative_metrics = df_clean.filter((col("AQI") < 0) | (col("`PM2.5`") < 0) | (col("PM10") < 0)).count()
    print(f"Rows with impossible negative metrics: {negative_metrics} " + ("✅" if negative_metrics == 0 else "❌"))

    # Future timestamp check
    future_dates = df_clean.filter(col(time_col) > current_timestamp()).count()
    print(f"Rows with future timestamps: {future_dates} " + ("✅" if future_dates == 0 else "❌"))

    # AQI upper bounds check (> 500)
    out_of_bounds_aqi = df_clean.filter(col("AQI") > 500).count()
    print(f"Rows with AQI > 500: {out_of_bounds_aqi} " + ("✅" if out_of_bounds_aqi == 0 else "❌"))

    # ---------------------------------------------------------
    # TEST 3: Quarantine Validity
    # ---------------------------------------------------------
    print("\n--- TEST 3: Quarantine Validity ---")
    
    # Quarantine validity: missing reason
    missing_reason = df_quar.filter(col("quarantine_reason").isNull() | (col("quarantine_reason") == "")).count()
    print(f"Quarantined rows missing a failure reason: {missing_reason} " + ("✅" if missing_reason == 0 else "❌"))

# Execute the verifications
verify_silver_layer("bronze_city_day")
verify_silver_layer("bronze_city_hour")
verify_silver_layer("bronze_station_day")
verify_silver_layer("bronze_station_hour")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databricks_7405612194732360.bronze_layer.bronze_city_day
# MAGIC WHERE to_date(date) >= '2020-01-01'
# MAGIC   AND to_date(date) < '2021-01-01';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT City, Datetime, AQI, quarantine_reason 
# MAGIC FROM databricks_7405612194732360.aqi_silver_layer_new.quarantine_city_hour;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT City, Datetime, AQI, `PM2.5`, warning_pm_missing, warning_duplicate
# MAGIC FROM databricks_7405612194732360.aqi_silver_layer_new.city_hour
# MAGIC WHERE warning_pm_missing = true;
