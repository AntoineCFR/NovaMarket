# Databricks notebook source
# MAGIC %md
# MAGIC # Corrigé — M0

# COMMAND ----------

CATALOG = "novamarket"

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO 1 — commentaire sur le catalog
# MAGIC
# MAGIC `COMMENT ON` s'applique aux catalogs, schemas, tables, colonnes et volumes. C'est le
# MAGIC point d'entrée le moins cher de la gouvernance, et le premier que tout le monde saute.

# COMMAND ----------

spark.sql(f"""
    COMMENT ON CATALOG {CATALOG} IS
    'NovaMarket - plateforme data de la marketplace. Couches landing / bronze / silver / gold / ops.'
""")

display(spark.sql(f"DESCRIBE CATALOG {CATALOG}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO 2 — table ops.pipeline_runs
# MAGIC
# MAGIC Deux points d'attention :
# MAGIC
# MAGIC - **L'ordre et les types des colonnes comptent.** Le grader compare le schéma exact,
# MAGIC   noms *et* types. C'est volontaire : un contrôle de schéma qui ne vérifie que les
# MAGIC   noms laisse passer les régressions les plus coûteuses.
# MAGIC - `BIGINT` et non `INT` pour les compteurs. Un compteur de lignes qui déborde à
# MAGIC   2 milliards, ça se voit toujours au pire moment.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.ops.pipeline_runs (
        run_id          STRING    COMMENT 'Identifiant unique de l execution',
        task_name       STRING    COMMENT 'Nom du flux, ex. bronze_orders',
        source_name     STRING    COMMENT 'Source logique traitee',
        started_at      TIMESTAMP COMMENT 'Debut de l execution',
        ended_at        TIMESTAMP COMMENT 'Fin de l execution',
        status          STRING    COMMENT 'SUCCESS ou FAILED',
        rows_written    BIGINT    COMMENT 'Lignes presentes en cible apres execution',
        rows_rescued    BIGINT    COMMENT 'Lignes portant un _rescued_data non nul',
        files_processed BIGINT    COMMENT 'Fichiers sources distincts vus en cible',
        notes           STRING    COMMENT 'Commentaire libre'
    )
    COMMENT 'Journal d execution des taches du pipeline NovaMarket'
""")

display(spark.sql(f"DESCRIBE TABLE {CATALOG}.ops.pipeline_runs"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Note sur le téléversement
# MAGIC
# MAGIC Si la CLI refuse `dbfs:/Volumes/...`, vérifie sa version :
# MAGIC
# MAGIC ```bash
# MAGIC databricks version
# MAGIC ```
# MAGIC
# MAGIC Les versions 0.2xx et supérieures gèrent les volumes UC. Sur une v0.1x (l'ancienne
# MAGIC CLI Python), seul l'ancien DBFS est adressable : il faut passer par l'interface
# MAGIC graphique, ou mettre la CLI à jour.
