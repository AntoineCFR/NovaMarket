# Databricks notebook source
# MAGIC %md
# MAGIC # M1.2 — Bronze : événements applicatifs
# MAGIC
# MAGIC Source : `/Volumes/novamarket/landing/files/events/*.jsonl.gz`
# MAGIC Cible : `novamarket.bronze.events_raw`
# MAGIC
# MAGIC Deux différences majeures avec les commandes : le JSON est **imbriqué** et les
# MAGIC fichiers sont **compressés**. Aucune des deux ne doit te faire écrire de code
# MAGIC supplémentaire — mais les deux changent tes options de lecture.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

CATALOG = "novamarket"
LANDING = f"/Volumes/{CATALOG}/landing/files"
SOURCE_PATH = f"{LANDING}/events"
TARGET = f"{CATALOG}.bronze.events_raw"

FLOW = "bronze_events"
CHECKPOINT = f"/Volumes/{CATALOG}/ops/checkpoints/{FLOW}/checkpoint"
SCHEMA_LOCATION = f"/Volumes/{CATALOG}/ops/checkpoints/{FLOW}/schema"

BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Inspection
# MAGIC
# MAGIC Décompresse **en mémoire, pour regarder seulement**. Ne crée pas de fichier décompressé
# MAGIC dans le volume : Auto Loader lit le gzip nativement et c'est le comportement qu'on veut valider.

# COMMAND ----------

import gzip, json, os

sample = sorted(os.listdir(SOURCE_PATH))[0]
print(f"fichier inspecté : {sample}\n")

with gzip.open(f"{SOURCE_PATH}/{sample}", "rt", encoding="utf-8") as fh:
    lines = [next(fh) for _ in range(3)]

for line in lines:
    print(json.dumps(json.loads(line), indent=2, ensure_ascii=False)[:700])
    print("-" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC **Questions d'exploration :**
# MAGIC
# MAGIC 1. Quels champs sont des objets ? Lesquels sont des tableaux ? À quelle profondeur ?
# MAGIC 2. Parcours les 5 000 premières lignes d'un fichier avec `json.loads` en attrapant
# MAGIC    les exceptions. Combien échouent ? À quoi ressemblent ces lignes ?
# MAGIC 3. Le champ `event_ts` est-il toujours du même type ?
# MAGIC 4. Le champ `items[].qty` est-il toujours du même type ?
# MAGIC
# MAGIC Les questions 2 à 4 conditionnent ce que tu vas retrouver dans `_rescued_data`.

# COMMAND ----------

# TODO A — code d'exploration des questions 2 à 4


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Le flux Auto Loader
# MAGIC
# MAGIC Contraintes du module :
# MAGIC
# MAGIC - `user`, `device`, `context` doivent rester des `STRUCT` dans la table.
# MAGIC   `items` doit rester un `ARRAY`. **Ne pas aplatir ici.**
# MAGIC - Les lignes JSON illisibles doivent atterrir dans `_rescued_data`.
# MAGIC - Un fichier `.gz` ne demande aucune option particulière — vérifie-le.
# MAGIC
# MAGIC Réfléchis à `cloudFiles.inferColumnTypes` : contrairement au CSV, ici on **veut**
# MAGIC que la structure soit inférée. Pourquoi ce choix est-il cohérent avec la règle
# MAGIC « pas de transformation en bronze » alors qu'on refusait l'inférence pour le CSV ?

# COMMAND ----------

# TODO B — readStream Auto Loader (format json)

raw = (
    spark.readStream
    .format("cloudFiles")
    # ...
    .load(SOURCE_PATH)
)

raw.printSchema()

# COMMAND ----------

# TODO C — colonnes techniques (_source_file, _source_file_modification_time,
#          _ingested_at, _ingest_batch_id)

enriched = raw

# COMMAND ----------

# TODO D — écriture vers TARGET (append, availableNow, checkpoint)

query = (
    enriched.writeStream
    # ...
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Contrôle

# COMMAND ----------

df = spark.table(TARGET)

n_rows = df.count()
n_files = df.select("_source_file").distinct().count()
n_rescued = df.filter(F.col("_rescued_data").isNotNull()).count()

print(f"lignes                     : {n_rows:>10,}".replace(",", " "))
print(f"fichiers distincts         : {n_files:>10}")
print(f"lignes avec _rescued_data  : {n_rescued:>10,}".replace(",", " "))
print()
print("types (les 4 imbriqués doivent être STRUCT / ARRAY) :")
for name, dtype in df.dtypes:
    if name in ("user", "device", "context", "items"):
        print(f"    {name:10s} {dtype[:110]}")

# COMMAND ----------

# MAGIC %md
# MAGIC Un aperçu des lignes sauvées. Note la différence de nature avec celles des commandes :
# MAGIC ici, ce n'est pas une colonne en trop, c'est l'enregistrement entier qui est illisible.

# COMMAND ----------

display(df.filter(F.col("_rescued_data").isNotNull()).select("_rescued_data", "_source_file").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Journalisation

# COMMAND ----------

def log_run(catalog, task_name, source_name, started_at, status,
            rows_written=0, rows_rescued=0, files_processed=0, notes=None, run_id=None):
    row = [(run_id or str(uuid.uuid4()), task_name, source_name, started_at, datetime.now(),
            status, int(rows_written), int(rows_rescued), int(files_processed), notes)]
    schema = ("run_id string, task_name string, source_name string, started_at timestamp, "
              "ended_at timestamp, status string, rows_written bigint, rows_rescued bigint, "
              "files_processed bigint, notes string")
    spark.createDataFrame(row, schema).write.mode("append").saveAsTable(f"{catalog}.ops.pipeline_runs")


log_run(CATALOG, FLOW, "events_jsonl", STARTED_AT, "SUCCESS",
        rows_written=n_rows, rows_rescued=n_rescued, files_processed=n_files,
        notes=f"batch {BATCH_ID}", run_id=BATCH_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Réinitialisation

# COMMAND ----------

# dbutils.fs.rm(f"/Volumes/{CATALOG}/ops/checkpoints/{FLOW}", True)
# spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
