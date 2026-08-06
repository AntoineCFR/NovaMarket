# Databricks notebook source
# MAGIC %md
# MAGIC # Grader — M8 : orchestration
# MAGIC
# MAGIC À exécuter après une exécution complète du job, vague W3 ingérée.
# MAGIC Les comptages décrivent l'état **S3** (W1 + W2 + W3).

# COMMAND ----------

# MAGIC %run ./_grader_lib

# COMMAND ----------

dbutils.widgets.text("catalog", "novamarket", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F

g = Grader(f"M8 — orchestration ({CATALOG})")

JOB_RUNS_SCHEMA = [
    ("job_run_id", "string"), ("job_name", "string"), ("started_at", "timestamp"),
    ("ended_at", "timestamp"), ("n_tasks", "int"), ("status", "string"), ("notes", "string"),
]

# COMMAND ----------


def t(name):
    return spark.table(f"{CATALOG}.{name}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## La dérive de schéma a été absorbée

# COMMAND ----------

g.equals("bronze.orders_raw : lignes", lambda: t("bronze.orders_raw").count(), 289_272)
g.equals("bronze.orders_raw : fichiers sources distincts",
         lambda: t("bronze.orders_raw").select("_source_file").distinct().count(), 9)
g.truthy("les colonnes promo_code et channel existent",
         lambda: {"promo_code", "channel"}.issubset(dict(t("bronze.orders_raw").dtypes)),
         hint="ajoutées par l'évolution de schéma")
g.equals("promo_code nul sur les lignes antérieures à W3",
         lambda: t("bronze.orders_raw").filter(F.col("channel").isNull()).count(), 287_785)
g.equals("bronze.events_raw : lignes", lambda: t("bronze.events_raw").count(), 139_428)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Les couches aval ont suivi

# COMMAND ----------

g.equals("silver.order_line : lignes", lambda: t("silver.order_line").count(), 283_556)
g.equals("ops.quarantine_order_line : lignes",
         lambda: t("ops.quarantine_order_line").count(), 2_240)
g.equals("INVARIANT — silver + quarantaine",
         lambda: t("silver.order_line").count() + t("ops.quarantine_order_line").count(), 285_796)
g.equals("silver.event : lignes", lambda: t("silver.event").count(), 138_313)
g.equals("ops.quarantine_event : lignes", lambda: t("ops.quarantine_event").count(), 415)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Le job a bien orchestré
# MAGIC
# MAGIC Un identifiant d'exécution partagé par plusieurs tâches est la seule preuve, depuis
# MAGIC les données, qu'un job les a lancées ensemble.

# COMMAND ----------


def best_run():
    """Execution ayant couvert le plus de taches distinctes."""
    row = (t("ops.pipeline_runs")
           .groupBy("run_id")
           .agg(F.countDistinct("task_name").alias("n"))
           .orderBy(F.col("n").desc())
           .first())
    return row["run_id"] if row else None


def tasks_of_best_run():
    rid = best_run()
    return {r["task_name"] for r in t("ops.pipeline_runs")
            .filter(F.col("run_id") == rid).select("task_name").distinct().collect()}


def foreach_ref_iterations():
    rid = best_run()
    return (t("ops.pipeline_runs")
            .filter((F.col("run_id") == rid) & (F.col("task_name") == "bronze_ref"))
            .select("source_name").distinct().count())


g.truthy("un même run_id couvre au moins 6 tâches distinctes",
         lambda: len(tasks_of_best_run()) >= 6,
         hint=">= 6 tâches")
g.equals("le for_each a traité les 3 référentiels", foreach_ref_iterations, 3)
g.truthy("les tâches attendues sont présentes",
         lambda: {"bronze_orders", "bronze_events", "bronze_ref", "silver_order_line",
                  "silver_event", "dq_checks"}.issubset(tasks_of_best_run()),
         hint="ingestion, silver et contrôles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Le bilan d'exécution

# COMMAND ----------

g.equals("ops.job_runs : schéma exact",
         lambda: [(f.name, f.dataType.simpleString()) for f in t("ops.job_runs").schema],
         JOB_RUNS_SCHEMA)
g.truthy("ops.job_runs : au moins une exécution", lambda: t("ops.job_runs").count() >= 1)
g.equals("la dernière exécution est en succès",
         lambda: t("ops.job_runs").orderBy(F.col("started_at").desc()).first()["status"],
         "SUCCESS")
g.truthy("ops.job_runs : n_tasks cohérent",
         lambda: t("ops.job_runs").orderBy(F.col("started_at").desc()).first()["n_tasks"] >= 6)

# COMMAND ----------

g.report()
