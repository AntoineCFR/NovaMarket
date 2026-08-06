# Databricks notebook source
# MAGIC %md
# MAGIC # M7 — Source du pipeline déclaratif
# MAGIC
# MAGIC ⚠️ **Ce fichier ne s'exécute pas dans un notebook.** Il est le *source* d'un pipeline
# MAGIC Lakeflow. Rien ne se passera si tu cliques sur « Run all ».
# MAGIC
# MAGIC ## Création du pipeline
# MAGIC
# MAGIC *Jobs & Pipelines → Create → ETL pipeline*, puis :
# MAGIC
# MAGIC | Paramètre | Valeur |
# MAGIC |---|---|
# MAGIC | Source code | ce fichier |
# MAGIC | Catalog cible | `novamarket` |
# MAGIC | Schema cible | `ldp` |
# MAGIC | Mode | Triggered (surtout pas Continuous — le quota Free Edition n'y survivrait pas) |
# MAGIC
# MAGIC Rappel : **un seul pipeline actif** en Free Edition. Supprime les autres d'abord.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F, Window as W

# En Free Edition comme ailleurs, le pipeline lit le volume directement : le chemin est
# la seule configuration dont il a besoin.
SOURCE_PATH = "/Volumes/novamarket/landing/files/orders"

VALID_STATUSES = ["DELIVERED", "SHIPPED", "PENDING", "CANCELLED", "RETURNED"]
NON_REVENUE_STATUSES = ["CANCELLED", "RETURNED"]
TS_FORMAT = "yyyy-MM-dd HH:mm:ss"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fonctions de nettoyage
# MAGIC
# MAGIC Les mêmes qu'en M3. Le fait de devoir les recopier ici est en soi une réponse
# MAGIC partielle à la question 2 du README.

# COMMAND ----------


def clean_decimal(col):
    stripped = F.regexp_replace(col, r"[^0-9,.\-]", "")
    normalized = F.regexp_replace(stripped, ",", ".")
    return F.when(normalized == "", None).otherwise(normalized.cast("decimal(10,2)"))


def clean_int(col):
    return F.regexp_replace(col, r"[^0-9\-]", "").cast("int")


def clean_status(col):
    return F.upper(F.trim(col))


def parse_ts(col):
    return F.to_timestamp(col, TS_FORMAT)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Table de streaming — ingestion
# MAGIC
# MAGIC Aucun `checkpointLocation`, aucun `trigger`, aucun `awaitTermination`. Le pipeline
# MAGIC s'en charge.

# COMMAND ----------

# TODO A — la table de streaming orders_bronze
# Décorateur : @dlt.table(...) sur une fonction qui renvoie un DataFrame de streaming.
# Mêmes options Auto Loader qu'en M1, moins le checkpoint.


@dlt.table(
    name="orders_bronze",
    comment="Lignes de commande brutes. Colonnes source en STRING, fidelite a la source.",
)
def orders_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        # ... à compléter
        .load(SOURCE_PATH)
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Vue matérialisée silver, avec attentes
# MAGIC
# MAGIC Les attentes s'appliquent au DataFrame **renvoyé** par la fonction. Déduplique
# MAGIC avant de renvoyer, sinon elles s'évaluent sur 287 785 lignes et les compteurs
# MAGIC deviennent faux.
# MAGIC
# MAGIC `dlt.read("orders_bronze")` lit la table en batch — c'est ce qui rend la vue
# MAGIC matérialisée possible sur une source de streaming.

# COMMAND ----------

# TODO B — order_line_silver
# Décorateurs à empiler : @dlt.table puis les quatre @dlt.expect_or_drop(nom, condition).
# Noms imposés : valid_timestamp, valid_quantity, valid_price, known_status.


@dlt.table(name="order_line_silver", comment="...")
# @dlt.expect_or_drop("valid_timestamp", "...")
# ...
def order_line_silver():
    ...


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. La quarantaine
# MAGIC
# MAGIC `expect_or_drop` compte les lignes écartées. Il ne les garde pas.
# MAGIC
# MAGIC Deux approches possibles, et l'une est nettement moins bonne que l'autre :
# MAGIC
# MAGIC - réécrire les quatre conditions en négatif dans un second dataset — ça marche, et
# MAGIC   ça garantit qu'un jour les deux versions divergeront ;
# MAGIC - factoriser le calcul des motifs dans un dataset intermédiaire, puis en dériver
# MAGIC   deux vues : celle qui passe et celle qui ne passe pas.
# MAGIC
# MAGIC La seconde demande un dataset de plus. Elle est préférable, et c'est exactement le
# MAGIC raisonnement de M3 avec `quarantine_reasons`.

# COMMAND ----------

# TODO C — order_line_quarantine (2 229 lignes, avec les motifs)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. L'agrégat
# MAGIC
# MAGIC 42 lignes, 234 272 lignes de CA agrégées, 24 049 792,86 € de CA net.

# COMMAND ----------

# TODO D — revenue_by_month_country
