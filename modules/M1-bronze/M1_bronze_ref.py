# Databricks notebook source
# MAGIC %md
# MAGIC # M1.3 — Bronze : référentiels
# MAGIC
# MAGIC Sources : `/Volumes/novamarket/landing/files/ref/{categories,sellers,products}.csv`
# MAGIC Cibles : `novamarket.bronze.ref_categories_raw`, `ref_sellers_raw`, `ref_products_raw`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Le point du module : Auto Loader n'est pas la réponse à tout
# MAGIC
# MAGIC Les référentiels sont livrés en **snapshot complet** : à chaque livraison, le fichier
# MAGIC contient l'intégralité du catalogue, pas les nouveautés.
# MAGIC
# MAGIC Auto Loader est un mécanisme d'**ajout incrémental** : il détecte les nouveaux fichiers
# MAGIC et ajoute leurs lignes. Appliqué à un snapshot, il empilerait 8 000 produits à chaque
# MAGIC livraison.
# MAGIC
# MAGIC **Avant de coder, réponds :**
# MAGIC
# MAGIC 1. Quel motif d'écriture correspond à une source snapshot ?
# MAGIC 2. Ces trois fichiers n'ont ni le même séparateur ni le même encodage que les commandes.
# MAGIC    Vérifie-le plutôt que de me croire.
# MAGIC 3. Quel inconvénient a le motif « snapshot + overwrite » par rapport à l'incrémental,
# MAGIC    du point de vue de l'historique ? (On y reviendra en M4.)

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

CATALOG = "novamarket"
REF_PATH = f"/Volumes/{CATALOG}/landing/files/ref"

BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now()

REFS = {
    "categories": "ref_categories_raw",
    "sellers": "ref_sellers_raw",
    "products": "ref_products_raw",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Inspection

# COMMAND ----------

with open(f"{REF_PATH}/products.csv", "rb") as fh:
    head = fh.read(400)

print(head)
print()
print(head.decode("utf-8", errors="replace"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Chargement
# MAGIC
# MAGIC Contraintes identiques aux autres tables bronze :
# MAGIC
# MAGIC - colonnes source en `STRING`
# MAGIC - `_rescued_data`, `_source_file`, `_source_file_modification_time`,
# MAGIC   `_ingested_at`, `_ingest_batch_id`
# MAGIC - **rejouable** : deux exécutions consécutives doivent laisser exactement
# MAGIC   8 000 / 600 / 39 lignes, pas le double
# MAGIC
# MAGIC Et une vigilance propre à ce notebook : **les trois fichiers ne partagent pas les
# MAGIC conventions des commandes.** Regarde-les avant de fixer tes options — un mauvais
# MAGIC séparateur donne le bon nombre de lignes et **une seule colonne**.

# COMMAND ----------

# TODO A — écris la fonction de chargement d'un référentiel.
# Elle doit être appelable trois fois de suite sans effet cumulatif.

def load_ref(name: str, target_table: str):
    """Charge un snapshot de référentiel dans bronze."""
    src = f"{REF_PATH}/{name}.csv"

    df = (
        spark.read
        # ... options de lecture (séparateur, en-tête, typage, colonne de sauvetage)
        .load(src)
    )

    # ... colonnes techniques

    # ... écriture

    return spark.table(f"{CATALOG}.bronze.{target_table}").count()


# COMMAND ----------

for name, table in REFS.items():
    n = load_ref(name, table)
    print(f"{table:24s} {n:>7,} lignes".replace(",", " "))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Contrôle d'idempotence
# MAGIC
# MAGIC Relance la cellule précédente. Les comptages ne doivent pas bouger.

# COMMAND ----------

expected = {"ref_categories_raw": 39, "ref_sellers_raw": 600, "ref_products_raw": 8000}

for table, n_expected in expected.items():
    n = spark.table(f"{CATALOG}.bronze.{table}").count()
    flag = "OK " if n == n_expected else "KO "
    print(f"{flag} {table:24s} {n:>7,} / {n_expected:,}".replace(",", " "))

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


total = sum(spark.table(f"{CATALOG}.bronze.{t}").count() for t in REFS.values())
log_run(CATALOG, "bronze_ref", "ref_csv_snapshots", STARTED_AT, "SUCCESS",
        rows_written=total, files_processed=len(REFS), notes=f"batch {BATCH_ID}", run_id=BATCH_ID)
