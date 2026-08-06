# Databricks notebook source
# MAGIC %md
# MAGIC # M12 — Performance, monitoring et optimisation
# MAGIC
# MAGIC Règle du module : **aucune affirmation de performance sans mesure.**

# COMMAND ----------

from pyspark.sql import functions as F, Window as W
from datetime import datetime
import time

CATALOG = "novamarket"
HOT_SELLER = "S0001"
AMPLIFICATION = 400

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. L'outil de mesure
# MAGIC
# MAGIC Spark est paresseux : chronométrer la construction d'un DataFrame mesure la
# MAGIC compilation du plan, pas l'exécution. Il faut forcer le calcul.
# MAGIC
# MAGIC Et se méfier du cache : la deuxième variante bénéficie de ce que la première a
# MAGIC chargé. Un `spark.catalog.clearCache()` entre deux mesures est le minimum.

# COMMAND ----------

# TODO A — écris la fonction de mesure.
# Elle doit : vider le cache, chronométrer une action qui force réellement le calcul,
# et renvoyer (durée_ms, nombre_de_lignes).


def measure(df):
    """Renvoie (duration_ms, rows_out)."""
    ...


# TODO B — crée ops.perf_measurements (schéma dans le README) et la fonction qui
# y consigne une mesure.


def record(scenario, variant, config, duration_ms, rows_out):
    ...


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fabriquer un déséquilibre
# MAGIC
# MAGIC Les données du projet sont trop bien réparties pour montrer quoi que ce soit.
# MAGIC Amplifie un seul vendeur jusqu'à ce qu'il pèse ~40 % des lignes.

# COMMAND ----------

# TODO C — construis ops.skew_demo à partir de silver.order_line.
# Piste : explode(sequence(1, N)) sur le sous-ensemble d'un vendeur, puis union.


# COMMAND ----------

display(
    spark.table(f"{CATALOG}.ops.skew_demo")
    .groupBy("seller_id").count()
    .withColumn("part", F.round(F.col("count") / F.sum("count").over(W.partitionBy()), 4))
    .orderBy(F.col("count").desc()).limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Voir le skew
# MAGIC
# MAGIC Lance la jointure, puis **ouvre le Spark UI** depuis la cellule (lien sous le
# MAGIC résultat, ou onglet *Query profile*).
# MAGIC
# MAGIC Cherche le stage le plus long, puis son résumé de tâches. Compare la médiane et le
# MAGIC maximum de *shuffle read*. Regarde les colonnes de *spill*.

# COMMAND ----------

# TODO D — la jointure de démonstration, sur seller_id, avec une agrégation
# qui force un shuffle.


# COMMAND ----------

# MAGIC %md
# MAGIC ### Ce que tu as vu
# MAGIC
# MAGIC | Métrique | Valeur relevée |
# MAGIC |---|---|
# MAGIC | Durée du stage le plus long | … |
# MAGIC | Shuffle read médian | … |
# MAGIC | Shuffle read maximum | … |
# MAGIC | Rapport max / médiane | … |
# MAGIC | Spill mémoire / disque | … |
# MAGIC
# MAGIC **L'écart vient-il du skew ou de la taille absolue ?**
# MAGIC
# MAGIC > …

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Les paramètres de tuning
# MAGIC
# MAGIC Trois variantes de la même jointure. Note bien celles que le compute **refuse** ou
# MAGIC **ignore** : c'est une information d'examen sur les limites du serverless.

# COMMAND ----------

for param in ["spark.sql.shuffle.partitions",
              "spark.sql.autoBroadcastJoinThreshold",
              "spark.default.parallelism"]:
    try:
        print(f"{param:45s} = {spark.conf.get(param)}")
    except Exception as exc:
        print(f"{param:45s} -> non lisible ({type(exc).__name__})")

# COMMAND ----------

# TODO E — mesure les trois variantes et consigne-les :
#   1. defaut
#   2. autoBroadcastJoinThreshold = -1     (broadcast interdit)
#   3. autoBroadcastJoinThreshold = 100MB  (broadcast encouragé)
#
# Vérifie que rows_out est identique partout : sinon tu ne compares pas la même chose.


# COMMAND ----------

display(spark.table(f"{CATALOG}.ops.perf_measurements").orderBy("scenario", "variant"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Liquid clustering
# MAGIC
# MAGIC Deux copies identiques, une seule groupée. Deux tables plutôt qu'une mesure
# MAGIC avant/après : sinon le cache du premier passage fausse le second.

# COMMAND ----------

# TODO F — construis ops.perf_baseline (sans regroupement) et ops.perf_clustered
# (CLUSTER BY order_date, seller_id puis OPTIMIZE), toutes deux depuis
# gold.fact_order_line.


# COMMAND ----------

for table in ["perf_baseline", "perf_clustered"]:
    d = spark.sql(f"DESCRIBE DETAIL {CATALOG}.ops.{table}").first()
    print(f"{table:16s} {d['numFiles']:>4} fichier(s)  "
          f"regroupement : {d['clusteringColumns']}")

# COMMAND ----------

# TODO G — mesure la même requête filtrée sur les deux tables.
# Un filtre sur order_date ET seller_id : c'est là que l'élagage de fichiers joue.
# Regarde le nombre de fichiers réellement lus dans le query profile, pas seulement
# la durée.


# COMMAND ----------

# MAGIC %md
# MAGIC ### Predictive optimization

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {CATALOG}.gold.fact_order_line"))
display(spark.sql(f"SHOW TBLPROPERTIES {CATALOG}.gold.fact_order_line"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Tendances de jobs
# MAGIC
# MAGIC Même motif que la détection de dérive de M9, appliqué au pipeline au lieu des
# MAGIC données. La matière est là depuis M0 : `ops.pipeline_runs`.

# COMMAND ----------

# TODO H — construis ops.job_perf_trend :
# par task_name, la durée de la dernière exécution, la moyenne des précédentes,
# et l'écart relatif.


# COMMAND ----------

display(spark.table(f"{CATALOG}.ops.job_perf_trend").orderBy(F.col("deviation").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Tes réponses
# MAGIC
# MAGIC **1. L'AQE a-t-elle corrigé le skew toute seule ? Comment l'as-tu vérifié ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **2. Effet de `autoBroadcastJoinThreshold = -1` ? Quand le broadcast devient-il mauvais ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **3. Quels paramètres le serverless a-t-il refusés ? Qu'est-ce que ça dit du modèle ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **4. Gain du liquid clustering ? Si faible, inutile ou jeu de test trop petit ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **5. Plus de compute / meilleur plan / moins de données : classe les trois.**
# MAGIC
# MAGIC > …
