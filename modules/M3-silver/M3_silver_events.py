# Databricks notebook source
# MAGIC %md
# MAGIC # M3.2 — Silver : événements
# MAGIC
# MAGIC `bronze.events_raw` → `silver.event` + `silver.event_item` + `ops.quarantine_event`
# MAGIC
# MAGIC Trois difficultés spécifiques : l'aplatissement, l'explosion d'un tableau, et un
# MAGIC horodatage qui arrive sous deux formes incompatibles.

# COMMAND ----------

from pyspark.sql import functions as F, Window as W
from datetime import datetime
import uuid

CATALOG = "novamarket"
SOURCE = f"{CATALOG}.bronze.events_raw"
TARGET_EVENT = f"{CATALOG}.silver.event"
TARGET_ITEM = f"{CATALOG}.silver.event_item"
QUARANTINE = f"{CATALOG}.ops.quarantine_event"

BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now()

bronze = spark.table(SOURCE)
bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Établis l'état des lieux
# MAGIC
# MAGIC Avant de coder, réponds à trois questions sur **ta** table bronze — les réponses
# MAGIC dépendent de la façon dont ton runtime a inféré le schéma en M1 :
# MAGIC
# MAGIC 1. Quel type porte `event_ts` ? `string`, ou un type numérique ?
# MAGIC 2. Combien de lignes ont un `event_ts` nul ? Où est passée leur valeur ?
# MAGIC 3. Combien de lignes n'ont pas d'`event_id` du tout ?

# COMMAND ----------

# TODO A — état des lieux


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Quarantaine des enregistrements illisibles
# MAGIC
# MAGIC 389 lignes JSON tronquées. Elles n'ont ni `event_id` ni quoi que ce soit d'autre :
# MAGIC seule leur chaîne brute survit, dans `_rescued_data`.
# MAGIC
# MAGIC Schéma : `_rescued_data`, `_source_file`, `quarantine_reasons` (`array<string>`),
# MAGIC `quarantined_at`.

# COMMAND ----------

# TODO B — ops.quarantine_event


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Normalisation de l'horodatage
# MAGIC
# MAGIC `event_ts` arrive sous deux formes :
# MAGIC
# MAGIC - chaîne ISO : `2026-06-02T14:19:32Z`
# MAGIC - entier epoch en **millisecondes** : `1780000772000` (1 269 occurrences)
# MAGIC
# MAGIC Écris une expression qui traite les deux et ne rend jamais `null`. Si les valeurs
# MAGIC epoch sont parties dans `_rescued_data`, il faudra les y récupérer — `from_json`
# MAGIC ou `get_json_object` sont tes amis.

# COMMAND ----------

# TODO C — expression event_ts -> timestamp


def to_event_ts(df):
    ...


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Aplatissement et déduplication
# MAGIC
# MAGIC Déduplication sur `event_id`, critère de survie déterministe.
# MAGIC Schéma cible : voir le README du module.

# COMMAND ----------

# TODO D — silver.event


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Explosion des items
# MAGIC
# MAGIC L'indice de position doit être conservé : c'est ce qui distingue deux lignes
# MAGIC portant le même produit dans le même événement, et ce qui rend la table rejouable.
# MAGIC
# MAGIC `posexplode` fait ça en une fois. Attention à ne pas perdre les événements sans
# MAGIC items — ou plutôt : réfléchis à ce que doit contenir une table fille, et à quelle
# MAGIC variante d'explosion tu veux.
# MAGIC
# MAGIC `qty` et `price` sont eux aussi parfois sérialisés en chaîne.

# COMMAND ----------

# TODO E — silver.event_item


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Contrôle

# COMMAND ----------

event = spark.table(TARGET_EVENT)
item = spark.table(TARGET_ITEM)
quarantine = spark.table(QUARANTINE)

checks = [
    ("lignes silver.event", event.count(), 130_025),
    ("event_id distincts", event.select("event_id").distinct().count(), 130_025),
    ("event_ts nuls", event.filter(F.col("event_ts").isNull()).count(), 0),
    ("lignes en quarantaine", quarantine.count(), 389),
    ("lignes silver.event_item", item.count(), 26_183),
    ("événements avec items", item.select("event_id").distinct().count(), 15_755),
    ("événements identifiés", event.filter(F.col("customer_id").isNotNull()).count(), 114_396),
    ("qty nuls", item.filter(F.col("qty").isNull()).count(), 0),
    ("price nuls", item.filter(F.col("price").isNull()).count(), 0),
]

for label, got, expected in checks:
    flag = "OK " if got == expected else "KO "
    print(f"{flag} {label:28s} {got:>10,} / {expected:>10,}".replace(",", " "))

# COMMAND ----------

display(event.groupBy("event_type").count().orderBy(F.col("count").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC Le futur entonnoir de conversion (question 5 du gold) commence à se dessiner :

# COMMAND ----------

display(
    event.groupBy("utm_source")
         .agg(F.countDistinct("session_id").alias("sessions"),
              F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("achats"))
         .orderBy(F.col("sessions").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Journalisation

# COMMAND ----------


def log_run(catalog, task_name, source_name, started_at, status,
            rows_written=0, rows_rescued=0, files_processed=0, notes=None, run_id=None):
    row = [(run_id or str(uuid.uuid4()), task_name, source_name, started_at, datetime.now(),
            status, int(rows_written), int(rows_rescued), int(files_processed), notes)]
    schema = ("run_id string, task_name string, source_name string, started_at timestamp, "
              "ended_at timestamp, status string, rows_written bigint, rows_rescued bigint, "
              "files_processed bigint, notes string")
    spark.createDataFrame(row, schema).write.mode("append").saveAsTable(f"{catalog}.ops.pipeline_runs")


log_run(CATALOG, "silver_event", "bronze.events_raw", STARTED_AT, "SUCCESS",
        rows_written=event.count(), rows_rescued=quarantine.count(), run_id=BATCH_ID)
