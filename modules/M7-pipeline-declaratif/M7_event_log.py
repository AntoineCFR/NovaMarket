# Databricks notebook source
# MAGIC %md
# MAGIC # M7 — Exploitation du journal d'événements
# MAGIC
# MAGIC À exécuter **après** une mise à jour réussie du pipeline.
# MAGIC
# MAGIC Le pipeline publie tout ce qu'il fait sans que tu aies rien instrumenté. Compare
# MAGIC avec le travail de M6 : c'est le principal bénéfice du déclaratif, et il est
# MAGIC rarement celui qu'on met en avant.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

CATALOG = "novamarket"
LDP_SCHEMA = f"{CATALOG}.ldp"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Contrôle du résultat

# COMMAND ----------

for table, expected in [("orders_bronze", 287_785), ("order_line_silver", 282_104),
                        ("order_line_quarantine", 2_229), ("revenue_by_month_country", 42)]:
    n = spark.table(f"{LDP_SCHEMA}.{table}").count()
    flag = "OK " if n == expected else "KO "
    print(f"{flag} {table:26s} {n:>8,} / {expected:>8,}".replace(",", " "))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Accès au journal
# MAGIC
# MAGIC La fonction `event_log()` prend une table **publiée par le pipeline** en argument
# MAGIC et renvoie tous les événements de ce pipeline.
# MAGIC
# MAGIC Si elle n'est pas disponible sur ton workspace : dans les paramètres du pipeline,
# MAGIC section *Advanced*, publie le journal vers une table Unity Catalog, puis lis-la
# MAGIC directement.

# COMMAND ----------

events = spark.sql(f"SELECT * FROM event_log(TABLE({LDP_SCHEMA}.order_line_silver))")
events.createOrReplaceTempView("ldp_events")

display(
    spark.sql("""
        SELECT event_type, count(*) AS n, max(timestamp) AS dernier
        FROM ldp_events GROUP BY event_type ORDER BY n DESC
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Les métriques d'attentes
# MAGIC
# MAGIC Elles sont dans `details`, en JSON, sous
# MAGIC `flow_progress.data_quality.expectations` — un **tableau** d'objets.
# MAGIC
# MAGIC Regarde d'abord la structure brute avant d'écrire l'extraction.

# COMMAND ----------

display(
    spark.sql("""
        SELECT timestamp, origin.flow_name, details
        FROM ldp_events
        WHERE event_type = 'flow_progress'
          AND details:flow_progress.data_quality.expectations IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT 3
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `ops.ldp_expectations`
# MAGIC
# MAGIC Schéma imposé : `expectation_name`, `dataset` (`string`), `passed_records`,
# MAGIC `failed_records` (`bigint`), `update_id` (`string`), `extracted_at` (`timestamp`).
# MAGIC
# MAGIC Ne garde que la **dernière** mise à jour : le journal contient tout l'historique,
# MAGIC et sommer plusieurs exécutions donnerait des compteurs multipliés.

# COMMAND ----------

# TODO A — extraction vers ops.ldp_expectations
# Pistes : from_json ou la syntaxe `:` sur une colonne JSON, puis explode du tableau.


# COMMAND ----------

display(spark.table(f"{CATALOG}.ops.ldp_expectations").orderBy("expectation_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC Attendu :
# MAGIC
# MAGIC | Attente | En échec |
# MAGIC |---|---|
# MAGIC | `valid_timestamp` | 1 422 |
# MAGIC | `valid_quantity` | 813 |
# MAGIC | `valid_price` | 0 |
# MAGIC | `known_status` | 0 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Le test qui compte : deux implémentations, un seul résultat
# MAGIC
# MAGIC `silver.order_line` (notebooks de M3) et `ldp.order_line_silver` (ce pipeline)
# MAGIC décrivent la même donnée avec les mêmes règles. Ils doivent contenir exactement
# MAGIC les mêmes clés.

# COMMAND ----------

a = spark.table(f"{CATALOG}.silver.order_line").select("order_line_id")
b = spark.table(f"{LDP_SCHEMA}.order_line_silver").select("order_line_id")

print(f"dans M3 mais pas dans le pipeline : {a.exceptAll(b).count()}")
print(f"dans le pipeline mais pas dans M3 : {b.exceptAll(a).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Tes réponses
# MAGIC
# MAGIC **1. Qu'as-tu gagné ? (liste ce que tu n'as pas eu à écrire)**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **2. Qu'as-tu perdu ? (un contrôle de M3 ou M6 non exprimable ici)**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **3. Quelle règle mériterait un `expect_or_fail` ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **4. Pourquoi le silver est-il recalculé intégralement ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **5. Pipeline déclaratif ou notebooks, sur quelle partie du flux ?**
# MAGIC
# MAGIC > …
