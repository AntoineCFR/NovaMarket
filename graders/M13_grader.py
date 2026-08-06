# Databricks notebook source
# MAGIC %md
# MAGIC # Grader — M13 : méthodes d'ingestion
# MAGIC
# MAGIC Critères relatifs : le grader compare les deux tables **entre elles**, jamais à une
# MAGIC constante. Valable quelle que soit la vague ingérée.

# COMMAND ----------

# MAGIC %run ./_grader_lib

# COMMAND ----------

dbutils.widgets.text("catalog", "novamarket", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F

g = Grader(f"M13 — méthodes d'ingestion ({CATALOG})")

COMPARISON_SCHEMA = [
    ("method", "string"), ("target_table", "string"), ("run_number", "int"),
    ("rows_after", "bigint"), ("rows_added", "bigint"), ("notes", "string"),
    ("measured_at", "timestamp"),
]

SOURCE_COLS = ["order_id", "order_line_id", "order_ts", "customer_id", "seller_id",
               "product_id", "quantity", "unit_price", "order_status"]

# COMMAND ----------


def copyinto():
    return spark.table(f"{CATALOG}.bronze.orders_copyinto")


def autoloader():
    return spark.table(f"{CATALOG}.bronze.orders_raw")


def comparison():
    return spark.table(f"{CATALOG}.ops.ingestion_comparison")


def second_run_added(method):
    row = (comparison()
           .filter((F.col("method") == method) & (F.col("run_number") == 2))
           .orderBy(F.col("measured_at").desc()).limit(1).collect())
    return int(row[0]["rows_added"]) if row else None


def final_rows(method):
    row = (comparison().filter(F.col("method") == method)
           .orderBy(F.col("run_number").desc(), F.col("measured_at").desc())
           .limit(1).collect())
    return int(row[0]["rows_after"]) if row else None


# COMMAND ----------

# MAGIC %md
# MAGIC ## La table chargée par `COPY INTO`

# COMMAND ----------

g.truthy("bronze.orders_copyinto existe", lambda: copyinto() is not None)
g.truthy("elle contient exactement autant de lignes que la table Auto Loader",
         lambda: copyinto().count() == autoloader().count(),
         hint="mêmes fichiers, même résultat")
g.truthy("elle porte une colonne de sauvetage",
         lambda: any(c.startswith("_rescued") for c in copyinto().columns))
g.truthy("les colonnes source y sont en STRING",
         lambda: all(dict(copyinto().dtypes).get(c) == "string" for c in SOURCE_COLS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Le journal de comparaison

# COMMAND ----------

g.equals("ops.ingestion_comparison : schéma exact",
         lambda: [(f.name, f.dataType.simpleString()) for f in comparison().schema],
         COMPARISON_SCHEMA)
g.truthy("les deux méthodes sont documentées",
         lambda: {"COPY_INTO", "AUTO_LOADER"}.issubset(
             {r["method"] for r in comparison().select("method").distinct().collect()}))
g.truthy("chaque méthode a deux exécutions enregistrées",
         lambda: comparison().groupBy("method")
                  .agg(F.countDistinct("run_number").alias("n"))
                  .filter("n < 2").count() == 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Les deux critères qui comptent
# MAGIC
# MAGIC L'idempotence des deux méthodes, et le fait qu'elles convergent sur le même
# MAGIC résultat malgré des mécanismes d'état complètement différents.

# COMMAND ----------

g.equals("COPY INTO : la deuxième exécution n'ajoute rien",
         lambda: second_run_added("COPY_INTO"), 0)
g.equals("Auto Loader : la deuxième exécution n'ajoute rien",
         lambda: second_run_added("AUTO_LOADER"), 0)
g.truthy("les deux méthodes convergent sur le même nombre de lignes",
         lambda: final_rows("COPY_INTO") == final_rows("AUTO_LOADER"),
         hint="deux états différents, un même résultat")

# COMMAND ----------

g.report()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vérification manuelle
# MAGIC
# MAGIC Le grader ne peut pas lire ton tableau des six lignes ni tes réponses. Ils font
# MAGIC pourtant l'essentiel du module : l'objectif d'examen porte sur l'**arbitrage**
# MAGIC entre méthodes, pas sur la capacité à écrire un `COPY INTO`.
# MAGIC
# MAGIC Relis-les avec `solutions/M13/` à côté avant de passer à la suite.
