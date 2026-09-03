# Databricks notebook source
# MAGIC %md
# MAGIC # Spoke — acceptance checks
# MAGIC
# MAGIC Run this notebook when you believe the three tables are ready.
# MAGIC
# MAGIC It checks **results only** — the contents of `spoke.gold`. It does not look at
# MAGIC your notebooks, your intermediate tables, or how you got there.
# MAGIC
# MAGIC Every expected figure comes from `generator/reference.py`, a plain-Python
# MAGIC implementation of the same rules run against the delivered files.

# COMMAND ----------

dbutils.widgets.text("catalog", "spoke", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException


class Checks:
    PASS, FAIL, WARN = " PASS ", " FAIL ", " WARN "

    def __init__(self, title):
        self.title, self.rows = title, []

    def _run(self, fn):
        try:
            return fn(), None
        except Exception as exc:                                   # noqa: BLE001
            return None, f"{type(exc).__name__}: {str(exc)[:70]}"

    def equals(self, name, fn, expected, hint=""):
        got, err = self._run(fn)
        if err:
            return self.rows.append((self.FAIL, name, err, expected, hint))
        ok = got == expected
        self.rows.append((self.PASS if ok else self.FAIL, name, got, expected,
                          "" if ok else hint))

    def close_to(self, name, fn, expected, tol, hint=""):
        got, err = self._run(fn)
        if err:
            return self.rows.append((self.FAIL, name, err, expected, hint))
        ok = got is not None and abs(float(got) - expected) <= tol
        self.rows.append((self.PASS if ok else self.FAIL, name,
                          None if got is None else round(float(got), 2),
                          f"{expected} ± {tol}", "" if ok else hint))

    def true(self, name, fn, hint=""):
        got, err = self._run(fn)
        if err:
            return self.rows.append((self.FAIL, name, err, "true", hint))
        self.rows.append((self.PASS if got else self.FAIL, name, got, "true",
                          "" if got else hint))

    def report(self):
        w = max(len(r[1]) for r in self.rows) + 2
        line = "=" * (w + 46)
        print(f"\n{line}\n  {self.title}\n{line}")
        for status, name, got, expected, hint in self.rows:
            print(f"[{status}] {name:<{w}} got={str(got):<18} expected={expected}")
            if hint:
                print(f"{'':>{w + 10}}{hint}")
        ok = sum(1 for r in self.rows if r[0] == self.PASS)
        print(line)
        print(f"  {ok} / {len(self.rows)} checks passed")
        if ok == len(self.rows):
            print("  All good. The rebalancing team can work from this.")
        print(line)


c = Checks(f"Spoke — acceptance ({CATALOG}.gold)")

# COMMAND ----------

# MAGIC %md ## The tables exist, with the agreed columns

# COMMAND ----------

CONTRACT = {
    "daily_station_usage": {
        "ride_date": "date", "station_id": "string", "station_name": "string",
        "district": "string", "rides_started": "bigint", "rides_ended": "bigint",
        "net_flow": "bigint",
    },
    "hourly_demand": {
        "hour_of_day": "int", "rides_started": "bigint", "avg_ride_minutes": "double",
    },
    "data_quality_report": {"issue": "string", "record_count": "bigint"},
}


def columns_of(table):
    return {f.name: f.dataType.simpleString()
            for f in spark.table(f"{CATALOG}.gold.{table}").schema.fields}


for table, expected in CONTRACT.items():
    c.true(f"{table} exists", lambda t=table: spark.table(f"{CATALOG}.gold.{t}") is not None,
           hint=f"expected {CATALOG}.gold.{table}")
    for col, typ in expected.items():
        c.equals(f"{table}.{col}", lambda t=table, col=col: columns_of(t).get(col), typ,
                 hint="column missing, or not the agreed type")

# COMMAND ----------

# MAGIC %md ## Data quality report — the four counts

# COMMAND ----------

EXPECTED_ISSUES = {
    "duplicate_ride": 55,
    "missing_end_station": 78,
    "unknown_station": 9,
    "unreliable_duration": 205,
}

HINTS = {
    "duplicate_ride": "the app resends a ride when a connection drops; count the extra copies",
    "missing_end_station": "the bike was never docked: no end station, no end time",
    "unknown_station": "two stations opened in June and are absent from the reference file",
    "unreliable_duration": "the reported duration disagrees with start and end by over a minute",
}


def issue_count(issue):
    row = (spark.table(f"{CATALOG}.gold.data_quality_report")
             .filter(F.col("issue") == issue).select("record_count").first())
    return row[0] if row else None


c.equals("report has four rows",
         lambda: spark.table(f"{CATALOG}.gold.data_quality_report").count(), 4,
         hint="one row per issue, no more and no fewer")

for issue, expected in EXPECTED_ISSUES.items():
    c.equals(f"{issue}", lambda i=issue: issue_count(i), expected, hint=HINTS[issue])

# COMMAND ----------

# MAGIC %md ## Daily station usage

# COMMAND ----------

usage = lambda: spark.table(f"{CATALOG}.gold.daily_station_usage")

c.equals("row count", lambda: usage().count(), 410,
         hint="one row per station per day, only for stations in the reference")
c.equals("distinct stations", lambda: usage().select("station_id").distinct().count(), 58,
         hint="two stations saw no customer ride over the period")
c.equals("distinct days", lambda: usage().select("ride_date").distinct().count(), 8,
         hint="a ride starting late on the last day ends the following morning")
c.equals("total rides_started",
         lambda: usage().agg(F.sum("rides_started")).first()[0], 4603,
         hint="maintenance trips are not customer usage")
c.equals("total rides_ended",
         lambda: usage().agg(F.sum("rides_ended")).first()[0], 4527,
         hint="rides that never closed have no end station to credit")
c.equals("net_flow is ended minus started",
         lambda: usage().filter(F.col("net_flow") != F.col("rides_ended") - F.col("rides_started")).count(), 0)
c.equals("one row per station and day",
         lambda: usage().groupBy("ride_date", "station_id").count().filter("count > 1").count(), 0,
         hint="the station reference contains one station twice — a join will fan out")

# COMMAND ----------

# MAGIC %md ## Names and districts, as the marketing team will read them

# COMMAND ----------

c.equals("station names are trimmed",
         lambda: usage().filter(F.col("station_name") != F.trim(F.col("station_name"))).count(), 0,
         hint="three names carry stray whitespace from the spreadsheet export")
c.equals("districts are consistent",
         lambda: usage().select("district").distinct().count(), 6,
         hint="the same district is spelled several ways in the reference file")
c.equals("districts are displayable",
         lambda: usage().filter(F.col("district") != F.initcap(F.col("district"))).count(), 0,
         hint="marketing publishes these names as they are")

# COMMAND ----------

# MAGIC %md ## Hourly demand

# COMMAND ----------

hourly = lambda: spark.table(f"{CATALOG}.gold.hourly_demand")

c.equals("row count", lambda: hourly().count(), 19,
         hint="only hours in which something happened")
c.equals("total matches daily usage",
         lambda: hourly().agg(F.sum("rides_started")).first()[0], 4603)
c.equals("peak hour", lambda: hourly().orderBy(F.desc("rides_started")).first()["hour_of_day"], 17)
c.equals("peak volume",
         lambda: hourly().orderBy(F.desc("rides_started")).first()["rides_started"], 688)
c.close_to("average ride at 17:00",
           lambda: hourly().filter("hour_of_day = 17").first()["avg_ride_minutes"], 37.9, 0.6,
           hint="rides that never closed have no measurable duration")
c.true("no hour outside 0-23",
       lambda: hourly().filter("hour_of_day < 0 OR hour_of_day > 23").count() == 0)

# COMMAND ----------

c.report()
