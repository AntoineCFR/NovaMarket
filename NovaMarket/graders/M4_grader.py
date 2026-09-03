# Databricks notebook source
# MAGIC %md
# MAGIC # Grader — M4 : historisation SCD2
# MAGIC
# MAGIC À exécuter après la **troisième** extraction et l'application du `MERGE`.
# MAGIC Valeurs de référence produites par `generator/reference_gold.py`.

# COMMAND ----------

# MAGIC %run ./_grader_lib

# COMMAND ----------

dbutils.widgets.text("catalog", "novamarket", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F, Window as W

g = Grader(f"M4 — historisation SCD2 ({CATALOG})")

SELLER_SCHEMA = [
    ("seller_id", "string"), ("seller_name", "string"), ("seller_country", "string"),
    ("seller_city", "string"), ("main_top_category", "string"), ("plan_code", "string"),
    ("is_active", "boolean"), ("onboarded_at", "date"),
    ("valid_from", "timestamp"), ("valid_to", "timestamp"), ("is_current", "boolean"),
    ("_scd_hash", "string"), ("_processed_at", "timestamp"),
]

CUSTOMER_SCHEMA = [
    ("customer_id", "string"), ("first_name", "string"), ("last_name", "string"),
    ("email", "string"), ("country", "string"), ("city", "string"), ("zip_code", "string"),
    ("segment", "string"), ("is_opt_in", "boolean"), ("is_deleted", "boolean"),
    ("created_at", "timestamp"),
    ("valid_from", "timestamp"), ("valid_to", "timestamp"), ("is_current", "boolean"),
    ("_scd_hash", "string"), ("_processed_at", "timestamp"),
]

# COMMAND ----------


def scd(name):
    return spark.table(f"{CATALOG}.silver.{name}")


# --- contrôles d'intégrité temporelle, indépendants du jeu de données ---------


def n_keys_without_exactly_one_current(name, key):
    return (scd(name)
            .groupBy(key)
            .agg(F.sum(F.col("is_current").cast("int")).alias("n"))
            .filter("n <> 1").count())


def n_current_with_valid_to(name):
    return scd(name).filter("is_current AND valid_to IS NOT NULL").count()


def n_closed_with_bad_interval(name):
    return scd(name).filter("valid_to IS NOT NULL AND valid_to <= valid_from").count()


def n_chain_breaks(name, key):
    w = W.partitionBy(key).orderBy("valid_from")
    return (scd(name)
            .withColumn("_next_from", F.lead("valid_from").over(w))
            .filter(F.col("_next_from").isNotNull())
            .filter(F.col("valid_to") != F.col("_next_from"))
            .count())


def n_consecutive_same_hash(name, key):
    w = W.partitionBy(key).orderBy("valid_from")
    return (scd(name)
            .withColumn("_prev", F.lag("_scd_hash").over(w))
            .filter(F.col("_prev") == F.col("_scd_hash"))
            .count())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Vendeurs

# COMMAND ----------

g.equals("seller_scd2 : schéma exact",
         lambda: [(f.name, f.dataType.simpleString()) for f in scd("seller_scd2").schema],
         SELLER_SCHEMA)
g.equals("seller_scd2 : lignes", lambda: scd("seller_scd2").count(), 665)
g.equals("seller_scd2 : lignes courantes", lambda: scd("seller_scd2").filter("is_current").count(), 600)
g.equals("seller_scd2 : clés distinctes",
         lambda: scd("seller_scd2").select("seller_id").distinct().count(), 600)
g.equals("vendeurs ayant changé de plan",
         lambda: scd("seller_scd2").groupBy("seller_id").count().filter("count = 2").count(), 65)
g.equals("aucun vendeur en 3 versions ou plus",
         lambda: scd("seller_scd2").groupBy("seller_id").count().filter("count > 2").count(), 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clients

# COMMAND ----------

g.equals("customer_scd2 : schéma exact",
         lambda: [(f.name, f.dataType.simpleString()) for f in scd("customer_scd2").schema],
         CUSTOMER_SCHEMA)
g.equals("customer_scd2 : lignes", lambda: scd("customer_scd2").count(), 25_570)
g.equals("customer_scd2 : lignes courantes",
         lambda: scd("customer_scd2").filter("is_current").count(), 25_080)
g.equals("clients en 1 version",
         lambda: scd("customer_scd2").groupBy("customer_id").count().filter("count = 1").count(), 24_593)
g.equals("clients en 2 versions",
         lambda: scd("customer_scd2").groupBy("customer_id").count().filter("count = 2").count(), 484)
g.equals("clients en 3 versions",
         lambda: scd("customer_scd2").groupBy("customer_id").count().filter("count = 3").count(), 3)
g.equals("clients courants marqués supprimés",
         lambda: scd("customer_scd2").filter("is_current AND is_deleted").count(), 35)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intégrité temporelle
# MAGIC
# MAGIC Ces contrôles ne dépendent d'aucun comptage : ils resteront valables sur d'autres
# MAGIC données. Ce sont ceux qu'on met en production.

# COMMAND ----------

for table, key in [("seller_scd2", "seller_id"), ("customer_scd2", "customer_id")]:
    g.equals(f"{table} : une seule version courante par clé",
             lambda t=table, k=key: n_keys_without_exactly_one_current(t, k), 0)
    g.equals(f"{table} : aucune version courante avec valid_to",
             lambda t=table: n_current_with_valid_to(t), 0)
    g.equals(f"{table} : aucun intervalle vide ou inversé",
             lambda t=table: n_closed_with_bad_interval(t), 0)
    g.equals(f"{table} : chaînage sans rupture",
             lambda t=table, k=key: n_chain_breaks(t, k), 0)
    g.equals(f"{table} : aucune empreinte consécutive identique",
             lambda t=table, k=key: n_consecutive_same_hash(t, k), 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change Data Feed

# COMMAND ----------


def cdf_enabled():
    props = spark.sql(f"SHOW TBLPROPERTIES {CATALOG}.silver.seller_scd2").collect()
    return any(r[0] == "delta.enableChangeDataFeed" and str(r[1]).lower() == "true" for r in props)


g.truthy("CDF activé sur silver.seller_scd2", cdf_enabled)
g.truthy("ops.scd2_change_log alimentée",
         lambda: spark.table(f"{CATALOG}.ops.scd2_change_log").count() >= 1)
g.truthy("ops.scd2_change_log : colonnes du CDF conservées",
         lambda: {"_change_type", "_commit_version", "_commit_timestamp"}
                 .issubset(dict(spark.table(f"{CATALOG}.ops.scd2_change_log").dtypes)))

# COMMAND ----------

g.report()
