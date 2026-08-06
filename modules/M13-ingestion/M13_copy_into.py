# Databricks notebook source
# MAGIC %md
# MAGIC # M13 — `COPY INTO` contre Auto Loader
# MAGIC
# MAGIC Mêmes fichiers, mêmes options, deux mécanismes d'état. À la fin, le même nombre de
# MAGIC lignes — et deux raisons différentes pour lesquelles la seconde exécution n'ajoute
# MAGIC rien.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

CATALOG = "novamarket"
SOURCE_PATH = f"/Volumes/{CATALOG}/landing/files/orders"
TARGET = f"{CATALOG}.bronze.orders_copyinto"

print(f"lignes dans bronze.orders_raw : "
      f"{spark.table(f'{CATALOG}.bronze.orders_raw').count():,}".replace(",", " "))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Le journal de comparaison

# COMMAND ----------

# TODO A — crée ops.ingestion_comparison (schéma dans le README) et la fonction
# qui y consigne une exécution.


def record(method, target_table, run_number, rows_after, rows_added, notes):
    ...


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. La table cible
# MAGIC
# MAGIC `COPY INTO` a besoin d'une table qui existe. On peut la créer **sans colonnes** et
# MAGIC laisser la première exécution les découvrir — à condition d'activer la fusion de
# MAGIC schéma.

# COMMAND ----------

# TODO B — crée la table cible, vide, sans déclarer de colonnes.


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `COPY INTO`
# MAGIC
# MAGIC Les options de format sont **les mêmes** qu'en M1 : c'est le même fichier, il n'a
# MAGIC pas changé d'encodage ni de séparateur. Deux blocs à distinguer :
# MAGIC
# MAGIC - `FORMAT_OPTIONS` — comment lire le fichier
# MAGIC - `COPY_OPTIONS` — comment se comporter vis-à-vis de la cible
# MAGIC
# MAGIC La fusion de schéma est dans le second : c'est une décision sur la cible, pas sur
# MAGIC la lecture.

# COMMAND ----------

# TODO C — écris et exécute le COPY INTO.
#
# spark.sql(f"""
#     COPY INTO {TARGET}
#     FROM '{SOURCE_PATH}'
#     FILEFORMAT = CSV
#     FORMAT_OPTIONS (...)
#     COPY_OPTIONS (...)
# """)


# COMMAND ----------

# TODO D — consigne l'exécution n°1.


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. La deuxième exécution
# MAGIC
# MAGIC Relance **exactement la même commande**, sans rien ajouter dans le volume.
# MAGIC
# MAGIC Regarde ce que renvoie la commande elle-même : `COPY INTO` retourne le nombre de
# MAGIC fichiers et de lignes traités. C'est une information que le `writeStream` d'Auto
# MAGIC Loader ne te donne pas aussi directement.

# COMMAND ----------

# TODO E — relance, puis consigne l'exécution n°2.


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. La même chose côté Auto Loader
# MAGIC
# MAGIC Relance le notebook de M1 sans rien téléverser, et consigne les deux exécutions
# MAGIC dans le même journal. Tu compares alors deux mécanismes sur la même échelle.

# COMMAND ----------

# TODO F — consigne les deux exécutions d'Auto Loader.


# COMMAND ----------

display(spark.table(f"{CATALOG}.ops.ingestion_comparison")
        .orderBy("method", "run_number"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Le contrôle qui compte

# COMMAND ----------

a = spark.table(f"{CATALOG}.bronze.orders_raw").count()
b = spark.table(TARGET).count()
print(f"Auto Loader : {a:,}".replace(",", " "))
print(f"COPY INTO   : {b:,}".replace(",", " "))
print("identiques" if a == b else f"ECART DE {abs(a - b)} LIGNES")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Le tableau à remplir
# MAGIC
# MAGIC Après avoir manipulé, pas avant.
# MAGIC
# MAGIC | | `COPY INTO` | Auto Loader |
# MAGIC |---|---|---|
# MAGIC | Comment il sait ce qu'il a déjà lu | … | … |
# MAGIC | Où vit cet état | … | … |
# MAGIC | Que se passe-t-il si on le perd | … | … |
# MAGIC | Passage à l'échelle sur des millions de fichiers | … | … |
# MAGIC | Évolution de schéma | … | … |
# MAGIC | Peut-il tourner en streaming | … | … |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Tes réponses
# MAGIC
# MAGIC **1. Quand choisir `COPY INTO` alors qu'Auto Loader est disponible ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **2. Checkpoint supprimé d'un côté, historique supprimé de l'autre : que se passe-t-il ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **3. Un million de fichiers : laquelle se dégrade en premier ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **4. Dépôts irréguliers : quel déclencheur ? Défends le choix inverse.**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **5. JDBC scripté ou connecteur managé : sur quels critères ?**
# MAGIC
# MAGIC > …
