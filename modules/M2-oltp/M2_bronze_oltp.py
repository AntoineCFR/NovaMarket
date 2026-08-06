# Databricks notebook source
# MAGIC %md
# MAGIC # M2 — Bronze : source applicative (clients et vendeurs)
# MAGIC
# MAGIC Extraction incrémentale par watermark. Ce notebook est conçu pour être **rejoué** :
# MAGIC la première exécution charge tout, les suivantes ne prennent que le delta.
# MAGIC
# MAGIC Choisis ta voie d'accès dans le widget ci-dessous.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

dbutils.widgets.dropdown("source_mode", "file", ["file", "lakebase"], "Voie d'accès")
dbutils.widgets.dropdown("file_version", "v1", ["v1", "v2"], "Version des fichiers (voie B)")

SOURCE_MODE = dbutils.widgets.get("source_mode")
FILE_VERSION = dbutils.widgets.get("file_version")

CATALOG = "novamarket"
OLTP_PATH = f"/Volumes/{CATALOG}/landing/files/oltp"

# Voie A : nom du catalog Unity Catalog dans lequel tu as enregistré ta base Lakebase.
LAKEBASE_CATALOG = "novamarket_oltp"
LAKEBASE_SCHEMA = "public"

BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now()

print(f"voie   : {SOURCE_MODE}" + (f" ({FILE_VERSION})" if SOURCE_MODE == "file" else ""))
print(f"batch  : {BATCH_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. La table de watermarks
# MAGIC
# MAGIC C'est l'état de ton ingestion. Sans elle, « incrémental » n'a pas de sens.

# COMMAND ----------

# TODO A — crée novamarket.ops.ingest_watermarks si elle n'existe pas.
# Colonnes : source_name STRING, watermark_column STRING,
#            watermark_value TIMESTAMP, updated_at TIMESTAMP

spark.sql("""
    CREATE TABLE IF NOT EXISTS ...
""")

# COMMAND ----------

# TODO B — écris les deux fonctions de lecture/écriture du watermark.
# get_watermark doit renvoyer None quand la source n'a jamais été extraite :
# c'est ce qui distingue la charge initiale d'un delta.


def get_watermark(source_name: str):
    """Renvoie le watermark stocké pour une source, ou None."""
    ...


def set_watermark(source_name: str, column: str, value):
    """Enregistre (ou remplace) le watermark d'une source."""
    ...


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Accès à la source
# MAGIC
# MAGIC Une seule fonction, deux implémentations. Le reste du notebook ne doit pas savoir
# MAGIC d'où viennent les données — c'est ce qui te permettra de changer de voie sans
# MAGIC toucher à la logique d'ingestion.

# COMMAND ----------


def read_source(table: str):
    """Renvoie le contenu courant d'une table applicative, quelle que soit la voie."""
    if SOURCE_MODE == "lakebase":
        # Lecture fédérée via le catalog Unity Catalog enregistré sur ton instance.
        return spark.table(f"{LAKEBASE_CATALOG}.{LAKEBASE_SCHEMA}.{table}")

    suffix = "" if FILE_VERSION == "v1" else "_v2"
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(f"{OLTP_PATH}/{table}{suffix}.csv")
    )


display(read_source("app_customers").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Typage
# MAGIC
# MAGIC Rappel du schéma imposé : chaînes pour les attributs, `boolean` pour les drapeaux,
# MAGIC `timestamp` pour les dates techniques. En voie A les types sont déjà bons ; en voie B
# MAGIC tout arrive en chaîne. Ta fonction doit produire le même schéma dans les deux cas.

# COMMAND ----------

# TODO C — normalise le typage des deux tables.


def typed_customers(df):
    ...


def typed_sellers(df):
    ...


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. L'extraction incrémentale
# MAGIC
# MAGIC Le cœur du module. Trois cas à couvrir :
# MAGIC
# MAGIC - pas de watermark → tout charger ;
# MAGIC - watermark présent → ne prendre que ce qui a changé ;
# MAGIC - rien n'a changé → ne rien écrire, et ne pas faire reculer le watermark.
# MAGIC
# MAGIC Et la question qui décide de tout : `>` ou `>=` ?

# COMMAND ----------

# TODO D — implémente l'extraction.


def ingest(source_name: str, table: str, key: str, target: str, typer):
    """Extrait le delta d'une table applicative et l'ajoute au journal bronze.

    Renvoie (nb_lignes_extraites, nouveau_watermark).
    """
    watermark = get_watermark(source_name)
    df = typer(read_source(table))

    # ... filtrage sur updated_at selon le watermark

    # ... colonnes techniques : _extracted_at, _ingest_batch_id, _source_system

    # ... écriture en append

    # ... avancée du watermark, APRÈS une écriture réussie

    return 0, None


# COMMAND ----------

n_customers, wm_customers = ingest(
    "app_customers", "app_customers", "customer_id",
    f"{CATALOG}.bronze.app_customers_raw", typed_customers,
)
n_sellers, wm_sellers = ingest(
    "app_sellers", "app_sellers", "seller_id",
    f"{CATALOG}.bronze.app_sellers_raw", typed_sellers,
)

print(f"clients  : {n_customers:>6} ligne(s) extraite(s), watermark -> {wm_customers}")
print(f"vendeurs : {n_sellers:>6} ligne(s) extraite(s), watermark -> {wm_sellers}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Contrôle
# MAGIC
# MAGIC Après la **première** exécution : 25 000 et 600.
# MAGIC Après la **seconde** (avec les changements appliqués) : le journal a grossi du delta,
# MAGIC pas doublé.

# COMMAND ----------

for name in ["app_customers_raw", "app_sellers_raw"]:
    df = spark.table(f"{CATALOG}.bronze.{name}")
    key = "customer_id" if "customers" in name else "seller_id"
    print(f"{name:22s} {df.count():>7} ligne(s)  "
          f"{df.select(key).distinct().count():>7} clé(s) distincte(s)")

display(spark.table(f"{CATALOG}.ops.ingest_watermarks"))

# COMMAND ----------

# MAGIC %md
# MAGIC Les entités vues en plusieurs versions — la matière première du SCD2 de M4 :

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.bronze.app_sellers_raw")
    .groupBy("seller_id")
    .agg(F.count("*").alias("versions"),
         F.collect_list("plan_code").alias("plans"),
         F.collect_list("updated_at").alias("dates"))
    .filter("versions > 1")
    .orderBy("seller_id")
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


log_run(CATALOG, "bronze_oltp", f"app_customers+app_sellers ({SOURCE_MODE})", STARTED_AT,
        "SUCCESS", rows_written=n_customers + n_sellers, run_id=BATCH_ID,
        notes=f"watermarks: clients={wm_customers}, vendeurs={wm_sellers}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Tes réponses
# MAGIC
# MAGIC **1. Combien de lignes en `>` ? En `>=` ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **2. Coût de chaque erreur ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **3. Ta cible est un journal `append`, qui ne déduplique rien. En quoi cela rend-il
# MAGIC les deux erreurs de la question 2 asymétriques, et laquelle acceptes-tu de commettre ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **4. Quel cas un watermark sur `updated_at` ne détecte-t-il pas ?**
# MAGIC
# MAGIC > …

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Réinitialisation

# COMMAND ----------

# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.bronze.app_customers_raw")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.bronze.app_sellers_raw")
# spark.sql(f"DELETE FROM {CATALOG}.ops.ingest_watermarks WHERE source_name LIKE 'app_%'")
