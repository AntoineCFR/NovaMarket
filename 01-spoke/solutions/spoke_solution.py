# Databricks notebook source
# MAGIC %md
# MAGIC # Spoke — one possible solution
# MAGIC
# MAGIC **Do not open this before you have finished.** It is one way through the brief,
# MAGIC not the way: the client had no opinion on the method, and neither do I.
# MAGIC
# MAGIC What is worth reading here, once you are done, is the handful of decisions the
# MAGIC brief deliberately left open — and what they cost if you decide them differently.

# COMMAND ----------

dbutils.widgets.text("catalog", "spoke", "Catalog")
dbutils.widgets.text("volume", "/Volumes/spoke/bronze/landing", "Landing volume")
CATALOG = dbutils.widgets.get("catalog")
LANDING = dbutils.widgets.get("volume")

from pyspark.sql import functions as F, Window

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for s in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — keep what arrived, exactly as it arrived
# MAGIC
# MAGIC Everything stays a string. Typing here would lose rows before anyone has looked
# MAGIC at them: a capacity that was never registered, a duration the phone got wrong.

# COMMAND ----------

stations_raw = (spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(f"{LANDING}/stations/")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path")))

stations_raw.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.bronze.stations")

rides_raw = (spark.read
    .json(f"{LANDING}/rides/")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path")))

rides_raw.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.bronze.rides")

print("bronze.stations", spark.table(f"{CATALOG}.bronze.stations").count())
print("bronze.rides   ", spark.table(f"{CATALOG}.bronze.rides").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — stations
# MAGIC
# MAGIC Three problems the brief announced, and one decision it left open.
# MAGIC
# MAGIC The duplicate station is the dangerous one. It is not a data-quality nuisance:
# MAGIC left in place, it doubles every ride joined to that station, and the totals stay
# MAGIC plausible enough that nobody notices.
# MAGIC
# MAGIC **The open decision** is what to do with decommissioned stations. They ran for
# MAGIC part of the period, so their rides are real usage — dropping them would hide
# MAGIC traffic the vans actually served. They stay.

# COMMAND ----------

w_station = Window.partitionBy("station_id").orderBy(F.col("_source_file").desc())

stations = (spark.table(f"{CATALOG}.bronze.stations")
    .withColumn("_n", F.row_number().over(w_station))
    .filter("_n = 1").drop("_n")
    .select(
        F.trim("station_id").alias("station_id"),
        # trim() alone leaves double spaces inside the name; this normalises both
        F.trim(F.regexp_replace("station_name", r"\s+", " ")).alias("station_name"),
        F.initcap(F.trim("district")).alias("district"),
        F.col("capacity").try_cast("int").alias("capacity"),
        F.to_date("commissioned_on").alias("commissioned_on"),
        F.lower(F.trim("status")).alias("status"),
    ))

stations.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.silver.stations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — rides
# MAGIC
# MAGIC The app resends a ride identically when a connection drops, so any of the copies
# MAGIC will do — but only one may survive, which is what `row_number` guarantees and
# MAGIC `rank` does not.
# MAGIC
# MAGIC The duration is recomputed from the timestamps. The brief was explicit that the
# MAGIC reported figure is not to be trusted; it is kept only to be able to report the
# MAGIC disagreement.

# COMMAND ----------

w_ride = Window.partitionBy("ride_id").orderBy(F.col("_source_file").desc())

rides = (spark.table(f"{CATALOG}.bronze.rides")
    .withColumn("_n", F.row_number().over(w_ride))
    .filter("_n = 1").drop("_n")
    .withColumn("started_at", F.try_to_timestamp("started_at"))
    .withColumn("ended_at", F.try_to_timestamp("ended_at"))
    .withColumn("measured_seconds",
                F.when(F.col("ended_at").isNotNull(),
                       F.unix_timestamp("ended_at") - F.unix_timestamp("started_at")))
    .select(
        "ride_id", "bike_id", "user_type", "started_at", "ended_at",
        "start_station_id", "end_station_id",
        F.col("duration_seconds").alias("reported_seconds"),
        "measured_seconds",
        F.col("device.platform").alias("platform"),
        F.col("device.app_version").alias("app_version"),
        "issues", "_source_file",
    ))

rides.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.silver.rides")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — the quality report
# MAGIC
# MAGIC Note what each figure counts. `duplicate_ride` counts the **extra copies**, not
# MAGIC the rides that happened to be duplicated — the client asked how many records
# MAGIC arrived more than once.
# MAGIC
# MAGIC The other three count **rides**, after deduplication. Counting them before would
# MAGIC report the same problem twice for any ride that also arrived twice.

# COMMAND ----------

r = spark.table(f"{CATALOG}.silver.rides")
known = spark.table(f"{CATALOG}.silver.stations").select("station_id")
known_ids = [row.station_id for row in known.collect()]

duplicates = (spark.table(f"{CATALOG}.bronze.rides").count()
              - spark.table(f"{CATALOG}.silver.rides").count())

report = spark.createDataFrame([
    ("duplicate_ride", duplicates),
    ("missing_end_station", r.filter(F.col("end_station_id").isNull()).count()),
    ("unknown_station", r.filter(
        ~F.col("start_station_id").isin(known_ids)
        | (F.col("end_station_id").isNotNull()
           & ~F.col("end_station_id").isin(known_ids))).count()),
    ("unreliable_duration", r.filter(
        F.col("measured_seconds").isNotNull()
        & (F.abs(F.col("measured_seconds") - F.col("reported_seconds")) > 60)).count()),
], "issue string, record_count bigint")

report.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.data_quality_report")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — daily station usage
# MAGIC
# MAGIC Two counts on two different keys: a ride is a departure at its start station on
# MAGIC its start date, and an arrival at its end station on its **end** date. A ride
# MAGIC leaving at 23:40 arrives the next morning, which is why the table spans one more
# MAGIC day than the feed does.
# MAGIC
# MAGIC Maintenance trips are excluded here, once, at the top. Excluding them later —
# MAGIC in the aggregate, or in the dashboard — is how they end up counted as demand.

# COMMAND ----------

customer = r.filter(F.col("user_type") != "staff")

departures = (customer
    .select(F.to_date("started_at").alias("ride_date"),
            F.col("start_station_id").alias("station_id"))
    .groupBy("ride_date", "station_id").agg(F.count("*").alias("rides_started")))

arrivals = (customer.filter(F.col("end_station_id").isNotNull())
    .select(F.to_date("ended_at").alias("ride_date"),
            F.col("end_station_id").alias("station_id"))
    .groupBy("ride_date", "station_id").agg(F.count("*").alias("rides_ended")))

movements = (departures.join(arrivals, ["ride_date", "station_id"], "full_outer")
    .fillna(0, ["rides_started", "rides_ended"]))

daily = (movements
    # inner join: rides at the two unregistered stations have no row to attach to,
    # and the client asked for them in the quality report instead
    .join(spark.table(f"{CATALOG}.silver.stations"), "station_id", "inner")
    .select("ride_date", "station_id", "station_name", "district",
            "rides_started", "rides_ended",
            (F.col("rides_ended") - F.col("rides_started")).alias("net_flow")))

daily.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.daily_station_usage")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — hourly demand
# MAGIC
# MAGIC Same population as the departures above: customer rides from a station the
# MAGIC reference knows. The average is taken over the rides that have a measurable
# MAGIC duration — a ride that never closed has none, and `avg` ignores absences, which
# MAGIC is the behaviour we want here rather than a silent zero.

# COMMAND ----------

hourly = (customer
    .filter(F.col("start_station_id").isin(known_ids))
    .groupBy(F.hour("started_at").cast("int").alias("hour_of_day"))
    .agg(F.count("*").cast("bigint").alias("rides_started"),
         F.round(F.avg(F.col("measured_seconds") / 60.0), 1)
          .cast("double").alias("avg_ride_minutes"))
    .orderBy("hour_of_day"))

hourly.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.hourly_demand")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the brief left to you
# MAGIC
# MAGIC | Decision | What I chose, and why |
# MAGIC |---|---|
# MAGIC | Decommissioned stations | **Kept.** They operated for part of the period; their rides are real traffic the vans served. |
# MAGIC | Rides at the two unregistered stations | **Reported, not counted.** They cannot be attributed to a station the client can send a van to. |
# MAGIC | Rides that never closed | **Counted as departures, not arrivals.** Someone took the bike out; nobody knows where it stopped. |
# MAGIC | The reported duration | **Kept alongside the measured one**, so the disagreement can be quantified rather than silently corrected. |
# MAGIC | Missing capacities | **Left null.** Inventing a capacity would put a number in a rebalancing decision that nobody measured. |
# MAGIC
# MAGIC A different set of choices can pass the checks too — the acceptance figures were
# MAGIC computed from these, and the brief states the two the client insisted on. If you
# MAGIC decided differently and wrote down why, that is the answer they asked for.
