# Databricks notebook source
# MAGIC %md
# MAGIC # Grader — M2 : ingestion de la source applicative
# MAGIC
# MAGIC À exécuter après les **deux** extractions (initiale puis incrémentale).
# MAGIC Le grader est agnostique de la voie d'accès : Lakebase ou fichier, même verdict.

# COMMAND ----------

# MAGIC %run ./_grader_lib

# COMMAND ----------

dbutils.widgets.text("catalog", "novamarket", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F

g = Grader(f"M2 — source applicative ({CATALOG})")

# Clients dont la ligne a été modifiée exactement à l'horodatage du watermark.
BOUNDARY_IDS = ["C008322", "C014845", "C015028", "C021795", "C024060"]

# Bornes : 25 000 lignes initiales + 385 (extraction en `>`) à 411 (en `>=`).
CUST_MIN, CUST_MAX = 25_385, 25_411
SELL_MIN, SELL_MAX = 640, 641

CHANGE_TS = "2026-06-05 08:15:00"

EXPECTED_CUSTOMER_SCHEMA = {
    "customer_id": "string", "first_name": "string", "last_name": "string",
    "email": "string", "country": "string", "city": "string", "zip_code": "string",
    "segment": "string", "is_opt_in": "boolean", "is_deleted": "boolean",
    "created_at": "timestamp", "updated_at": "timestamp",
    "_extracted_at": "timestamp", "_ingest_batch_id": "string", "_source_system": "string",
}

EXPECTED_WM_SCHEMA = [
    ("source_name", "string"), ("watermark_column", "string"),
    ("watermark_value", "timestamp"), ("updated_at", "timestamp"),
]

# COMMAND ----------


def cust():
    return spark.table(f"{CATALOG}.bronze.app_customers_raw")


def sell():
    return spark.table(f"{CATALOG}.bronze.app_sellers_raw")


def wm():
    return spark.table(f"{CATALOG}.ops.ingest_watermarks")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Schéma

# COMMAND ----------

g.truthy("app_customers_raw : schéma conforme",
         lambda: all(dict(cust().dtypes).get(c) == t for c, t in EXPECTED_CUSTOMER_SCHEMA.items()),
         hint="15 colonnes, types imposés")
g.equals("app_customers_raw : aucun customer_id nul",
         lambda: cust().filter(F.col("customer_id").isNull()).count(), 0)
g.truthy("app_sellers_raw : colonnes techniques présentes",
         lambda: {"_extracted_at", "_ingest_batch_id", "_source_system"}.issubset(dict(sell().dtypes)))
g.equals("app_sellers_raw : plan_code typé string",
         lambda: dict(sell().dtypes).get("plan_code"), "string")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volumétrie : incrémental, ni plus ni moins

# COMMAND ----------

g.truthy("app_customers_raw : volumétrie d'un journal incrémental",
         lambda: CUST_MIN <= cust().count() <= CUST_MAX,
         hint=f"entre {CUST_MIN} et {CUST_MAX}")
g.equals("app_customers_raw : clés distinctes",
         lambda: cust().select("customer_id").distinct().count(), 25_060)
g.truthy("app_sellers_raw : volumétrie d'un journal incrémental",
         lambda: SELL_MIN <= sell().count() <= SELL_MAX,
         hint=f"entre {SELL_MIN} et {SELL_MAX}")
g.equals("app_sellers_raw : clés distinctes",
         lambda: sell().select("seller_id").distinct().count(), 600)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Le critère qui tranche
# MAGIC
# MAGIC Ces cinq clients ont été modifiés exactement à l'horodatage du watermark. Une
# MAGIC extraction en `updated_at > watermark` ne les voit jamais : la table paraît
# MAGIC correcte, et cinq déménagements ont disparu.

# COMMAND ----------

g.equals("les 5 clients de bordure ont bien déménagé à Toulouse",
         lambda: cust().filter(F.col("customer_id").isin(BOUNDARY_IDS))
                       .filter(F.col("city") == "Toulouse")
                       .select("customer_id").distinct().count(), 5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contenu du delta

# COMMAND ----------

g.equals("clients créés pendant la journée d'activité",
         lambda: cust().filter(F.col("created_at") == F.lit(CHANGE_TS).cast("timestamp"))
                       .select("customer_id").distinct().count(), 60)
g.equals("suppressions douces capturées",
         lambda: cust().filter(F.col("is_deleted")).select("customer_id").distinct().count(), 25)
g.truthy("vendeurs présents en plusieurs versions",
         lambda: 40 <= sell().groupBy("seller_id").count().filter("count > 1").count() <= 41,
         hint="40 ou 41")
g.equals("les changements de plan sont visibles dans le journal",
         lambda: sell().groupBy("seller_id")
                       .agg(F.countDistinct("plan_code").alias("n"))
                       .filter("n > 1").count(), 40)

# COMMAND ----------

# MAGIC %md
# MAGIC ## État de l'ingestion

# COMMAND ----------

g.equals("ops.ingest_watermarks : schéma exact",
         lambda: [(f.name, f.dataType.simpleString()) for f in wm().schema], EXPECTED_WM_SCHEMA)
g.equals("ops.ingest_watermarks : une seule ligne par source",
         lambda: wm().count(), wm().select("source_name").distinct().count())
g.equals("watermark des clients à jour",
         lambda: str(wm().filter("source_name = 'app_customers'")
                        .select("watermark_value").first()[0]), f"{CHANGE_TS}")
g.truthy("ops.pipeline_runs contient une entrée bronze_oltp",
         lambda: spark.table(f"{CATALOG}.ops.pipeline_runs")
                      .filter("task_name = 'bronze_oltp'").count() >= 1)

# COMMAND ----------

g.report()
