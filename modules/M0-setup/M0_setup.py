# Databricks notebook source
# MAGIC %md
# MAGIC # M0 — Mise en place de la plateforme NovaMarket
# MAGIC
# MAGIC Ce notebook construit l'arborescence Unity Catalog du projet.
# MAGIC Exécute les cellules dans l'ordre. Deux d'entre elles contiennent des `TODO`.
# MAGIC
# MAGIC Rappel : sur Free Edition, le compute est **serverless**. Clique sur *Connect*
# MAGIC en haut à droite et choisis *Serverless* si ce n'est pas déjà fait.

# COMMAND ----------

CATALOG = "novamarket"
VOLUME_LANDING = f"/Volumes/{CATALOG}/landing/files"
VOLUME_CHECKPOINTS = f"/Volumes/{CATALOG}/ops/checkpoints"

print(f"catalog            : {CATALOG}")
print(f"volume landing     : {VOLUME_LANDING}")
print(f"volume checkpoints : {VOLUME_CHECKPOINTS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Catalog et schemas
# MAGIC
# MAGIC Si `CREATE CATALOG` échoue (droits insuffisants sur ton workspace), utilise le
# MAGIC catalog `workspace` et préfixe les schemas par `nm_`. Pense alors à adapter la
# MAGIC constante `CATALOG` ci-dessus **et** le widget des graders.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

# TODO 1 — documente le catalog.
# Unity Catalog accepte un commentaire sur un catalog. C'est la première brique de
# la gouvernance, et le grader le vérifie.
# Indice : COMMENT ON CATALOG ... IS '...'
spark.sql(f"...")

# COMMAND ----------

for schema in ["landing", "bronze", "silver", "gold", "ops"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"schema {CATALOG}.{schema} ok")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Volumes
# MAGIC
# MAGIC Un volume managé est notre substitut au bucket S3 : même stockage objet, même
# MAGIC sémantique de fichiers, gouverné par Unity Catalog.

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.files "
          f"COMMENT 'Zone d atterrissage des fichiers sources NovaMarket'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ops.checkpoints "
          f"COMMENT 'Checkpoints et schemas Auto Loader'")

# COMMAND ----------

import os

for sub in ["orders", "events", "ref"]:
    os.makedirs(f"{VOLUME_LANDING}/{sub}", exist_ok=True)

print(sorted(os.listdir(VOLUME_LANDING)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Journal d'exécution
# MAGIC
# MAGIC Toutes les tâches d'ingestion et de transformation du parcours écriront une ligne
# MAGIC ici. C'est ce qui rendra le pipeline observable en M6, et c'est ce qu'un job de
# MAGIC production fait toujours.
# MAGIC
# MAGIC Le schéma est imposé (voir `docs/03-conventions.md`) : les graders s'appuient dessus.

# COMMAND ----------

# TODO 2 — crée la table novamarket.ops.pipeline_runs.
# Colonnes, dans cet ordre exact :
#   run_id          STRING
#   task_name       STRING
#   source_name     STRING
#   started_at      TIMESTAMP
#   ended_at        TIMESTAMP
#   status          STRING
#   rows_written    BIGINT
#   rows_rescued    BIGINT
#   files_processed BIGINT
#   notes           STRING
# Ajoute un COMMENT sur la table.

spark.sql("""
    CREATE TABLE IF NOT EXISTS ...
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fonction utilitaire de journalisation
# MAGIC
# MAGIC Fournie. Tu la réutiliseras dans tous les notebooks d'ingestion.
# MAGIC Copie-la telle quelle dans tes notebooks, ou range-la dans un fichier
# MAGIC `utils.py` à côté et importe-la — au choix.

# COMMAND ----------

from datetime import datetime
import uuid


def log_run(catalog, task_name, source_name, started_at, status,
            rows_written=0, rows_rescued=0, files_processed=0, notes=None, run_id=None):
    """Insere une ligne dans ops.pipeline_runs."""
    row = [(
        run_id or str(uuid.uuid4()),
        task_name,
        source_name,
        started_at,
        datetime.now(),
        status,
        int(rows_written),
        int(rows_rescued),
        int(files_processed),
        notes,
    )]
    schema = ("run_id string, task_name string, source_name string, started_at timestamp, "
              "ended_at timestamp, status string, rows_written bigint, rows_rescued bigint, "
              "files_processed bigint, notes string")
    spark.createDataFrame(row, schema).write.mode("append").saveAsTable(f"{catalog}.ops.pipeline_runs")


# petit test d'écriture
log_run(CATALOG, "M0_setup", "n/a", datetime.now(), "SUCCESS", notes="mise en place initiale")
display(spark.table(f"{CATALOG}.ops.pipeline_runs"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Vérification du téléversement
# MAGIC
# MAGIC À exécuter **après** avoir téléversé les vagues W0 et W1 (voir le README du module).

# COMMAND ----------

import os

expected = {"ref": (3, ".csv"), "orders": (7, ".csv"), "events": (14, ".jsonl.gz")}

for sub, (n_expected, ext) in expected.items():
    path = f"{VOLUME_LANDING}/{sub}"
    files = sorted(f for f in os.listdir(path) if f.endswith(ext))
    flag = "OK " if len(files) == n_expected else "KO "
    print(f"{flag} {sub:8s} {len(files):3d} / {n_expected} fichier(s) {ext}")
    for f in files:
        size = os.path.getsize(f"{path}/{f}") / 1024
        print(f"        {f:36s} {size:9.1f} Ko")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Et maintenant ?
# MAGIC
# MAGIC Lance `graders/M0_grader.py`. S'il passe au vert, enchaîne sur
# MAGIC `modules/M1-bronze/README.md`.
