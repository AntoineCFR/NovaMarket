# Databricks notebook source
# MAGIC %md
# MAGIC # M8 — Tâche finale : bilan de l'exécution
# MAGIC
# MAGIC Dernière tâche du job. Elle écrit ce qui s'est passé dans `ops.job_runs`.
# MAGIC
# MAGIC Une remarque de conception : cette tâche ne peut rendre compte que de ce qu'elle
# MAGIC voit, c'est-à-dire des tâches qui ont **réussi** et journalisé. Un job qui plante
# MAGIC avant elle n'écrira rien. C'est la limite de l'auto-journalisation, et c'est
# MAGIC pourquoi les notifications d'échec ne sont pas optionnelles.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

dbutils.widgets.text("catalog", "novamarket", "Catalog")
dbutils.widgets.text("run_id", "", "Run ID")
dbutils.widgets.text("job_name", "novamarket_daily", "Nom du job")

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = dbutils.widgets.get("run_id") or str(uuid.uuid4())
JOB_NAME = dbutils.widgets.get("job_name")

# COMMAND ----------

# TODO A — crée novamarket.ops.job_runs
# Colonnes : job_run_id STRING, job_name STRING, started_at TIMESTAMP,
#            ended_at TIMESTAMP, n_tasks INT, status STRING, notes STRING

spark.sql("""
    CREATE TABLE IF NOT EXISTS ...
""")

# COMMAND ----------

# TODO B — agrège ops.pipeline_runs pour ce run_id et écris le bilan.
# Pistes : nombre de tâches distinctes, première date de début, dernière date de fin,
# statut global (FAILED si au moins une tâche a échoué).


# COMMAND ----------

display(
    spark.table(f"{CATALOG}.ops.job_runs").orderBy(F.col("started_at").desc()).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contrôle : les tâches de cette exécution

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.ops.pipeline_runs")
    .filter(F.col("run_id") == RUN_ID)
    .select("task_name", "source_name", "status", "rows_written", "rows_rescued",
            "started_at", "ended_at")
    .orderBy("started_at")
)
