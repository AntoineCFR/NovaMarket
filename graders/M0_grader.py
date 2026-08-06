# Databricks notebook source
# MAGIC %md
# MAGIC # Grader — M0 : mise en place de la plateforme
# MAGIC
# MAGIC Exécute ce notebook après avoir terminé M0. Il ne lit que l'état d'Unity Catalog
# MAGIC et du volume : ton code ne l'intéresse pas.

# COMMAND ----------

# MAGIC %run ./_grader_lib

# COMMAND ----------

dbutils.widgets.text("catalog", "novamarket", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

import os

LANDING = f"/Volumes/{CATALOG}/landing/files"
g = Grader(f"M0 — mise en place ({CATALOG})")

# COMMAND ----------


def schemas():
    rows = spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()
    return {r[0] for r in rows}


def volumes(schema):
    rows = spark.sql(f"SHOW VOLUMES IN {CATALOG}.{schema}").collect()
    return {r[1] for r in rows}


def count_files(sub, ext):
    return len([f for f in os.listdir(f"{LANDING}/{sub}") if f.endswith(ext)])


def catalog_comment():
    # les noms de colonnes de DESCRIBE CATALOG ont varié selon les versions :
    # on cherche la ligne dont le libellé contient "comment", quel qu'il soit.
    for row in spark.sql(f"DESCRIBE CATALOG {CATALOG}").collect():
        if "comment" in str(row[0]).lower():
            return (row[1] or "").strip()
    return ""


def pipeline_runs_schema():
    return [(f.name, f.dataType.simpleString()) for f in spark.table(f"{CATALOG}.ops.pipeline_runs").schema]


EXPECTED_RUNS_SCHEMA = [
    ("run_id", "string"), ("task_name", "string"), ("source_name", "string"),
    ("started_at", "timestamp"), ("ended_at", "timestamp"), ("status", "string"),
    ("rows_written", "bigint"), ("rows_rescued", "bigint"), ("files_processed", "bigint"),
    ("notes", "string"),
]

# COMMAND ----------

g.truthy("catalog existe", lambda: spark.sql(f"SHOW CATALOGS LIKE '{CATALOG}'").count() == 1)
g.truthy("schemas landing/bronze/silver/gold/ops",
         lambda: {"landing", "bronze", "silver", "gold", "ops"}.issubset(schemas()))
g.truthy("volume landing.files", lambda: "files" in volumes("landing"))
g.truthy("volume ops.checkpoints", lambda: "checkpoints" in volumes("ops"))

g.equals("fichiers ref/*.csv", lambda: count_files("ref", ".csv"), 3)
g.equals("fichiers orders/*.csv", lambda: count_files("orders", ".csv"), 7)
g.equals("fichiers events/*.jsonl.gz", lambda: count_files("events", ".jsonl.gz"), 14)

g.truthy("commentaire sur le catalog", lambda: len(catalog_comment()) > 0)
g.equals("schéma de ops.pipeline_runs", pipeline_runs_schema, EXPECTED_RUNS_SCHEMA)

# COMMAND ----------

g.report()
