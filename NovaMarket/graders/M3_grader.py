# Databricks notebook source
# MAGIC %md
# MAGIC # Grader — M3 : couche silver
# MAGIC
# MAGIC Les valeurs attendues proviennent de `generator/reference_stats.py`, une
# MAGIC implémentation des mêmes règles en Python pur, indépendante de Spark.

# COMMAND ----------

# MAGIC %run ./_grader_lib

# COMMAND ----------

dbutils.widgets.text("catalog", "novamarket", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F
from decimal import Decimal

g = Grader(f"M3 — couche silver ({CATALOG})")

ORDER_LINE_SCHEMA = [
    ("order_line_id", "string"), ("order_id", "string"), ("order_ts", "timestamp"),
    ("order_date", "date"), ("customer_id", "string"), ("seller_id", "string"),
    ("product_id", "string"), ("quantity", "int"),
    ("unit_price", "decimal(10,2)"), ("discount_amount", "decimal(10,2)"),
    ("gross_amount", "decimal(12,2)"), ("net_amount", "decimal(12,2)"),
    ("currency", "string"), ("shipping_country", "string"), ("payment_method", "string"),
    ("order_status", "string"), ("is_revenue", "boolean"),
    ("is_orphan_customer", "boolean"), ("is_orphan_product", "boolean"),
    ("shipping_address", "string"), ("_source_file", "string"),
    ("_silver_processed_at", "timestamp"),
]

EVENT_TYPES = {
    "add_to_cart": 9_470, "checkout_start": 4_412, "page_view": 47_775,
    "product_view": 52_318, "purchase": 1_873, "search": 14_177,
}

# COMMAND ----------


def sil(name):
    return spark.table(f"{CATALOG}.silver.{name}")


def ops(name):
    return spark.table(f"{CATALOG}.ops.{name}")


def reasons_count(motif):
    return (ops("quarantine_order_line")
            .select(F.explode("quarantine_reasons").alias("m"))
            .filter(F.col("m") == motif).count())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Commandes — structure

# COMMAND ----------

g.equals("silver.order_line : schéma exact",
         lambda: [(f.name, f.dataType.simpleString()) for f in sil("order_line").schema],
         ORDER_LINE_SCHEMA)
g.truthy("quarantine_order_line : conserve la donnée brute",
         lambda: {"order_line_id", "order_ts", "quantity", "unit_price", "_rescued_data",
                  "_source_file", "quarantine_reasons", "quarantined_at"}
                 .issubset(dict(ops("quarantine_order_line").dtypes)),
         hint="colonnes source + rescue + motifs")
g.equals("quarantine_reasons typé array<string>",
         lambda: dict(ops("quarantine_order_line").dtypes).get("quarantine_reasons"), "array<string>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Commandes — volumétrie et invariant

# COMMAND ----------

g.equals("silver.order_line : lignes", lambda: sil("order_line").count(), 282_104)
g.equals("silver.order_line : order_line_id unique",
         lambda: sil("order_line").select("order_line_id").distinct().count(), 282_104)
g.equals("quarantine_order_line : lignes", lambda: ops("quarantine_order_line").count(), 2_229)
g.equals("INVARIANT — rien ne disparaît sans laisser d'adresse",
         lambda: sil("order_line").count() + ops("quarantine_order_line").count(), 284_333)
g.equals("commandes distinctes", lambda: sil("order_line").select("order_id").distinct().count(), 136_824)
# Toute adresse encore amputee est une adresse que la reparation de M1 n'a pas recollee.
# Si ce controle echoue en meme temps que le compte de lignes, la cause est presque
# toujours la meme : jointure sur `orders_address_repair` sans `distinct` prealable.
# Une adresse complete porte soit une virgule (saine : "<rue>, <cp> <ville>"), soit un
# point-virgule (reparee : "<rue>; Batiment X; <cp> <ville>"). Une adresse tronquee n'a
# ni l'un ni l'autre — c'est le seul test qui distingue les trois cas.
g.equals("silver.order_line : plus aucune adresse tronquée (réparation de M1 recollée)",
         lambda: sil("order_line").filter(
             ~(F.col("shipping_address").contains(",")
               | F.col("shipping_address").contains(";"))).count(), 0,
         hint="coalesce(shipping_address_full, shipping_address)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Commandes — motifs de quarantaine

# COMMAND ----------

g.equals("motif INVALID_TIMESTAMP", lambda: reasons_count("INVALID_TIMESTAMP"), 1_422)
g.equals("motif INVALID_QUANTITY", lambda: reasons_count("INVALID_QUANTITY"), 813)
g.equals("motif INVALID_PRICE (le nettoyage doit tout récupérer)",
         lambda: reasons_count("INVALID_PRICE"), 0)
g.equals("motif UNKNOWN_STATUS (la casse n'est pas une erreur)",
         lambda: reasons_count("UNKNOWN_STATUS"), 0)
g.equals("lignes cumulant deux motifs",
         lambda: ops("quarantine_order_line").filter(F.size("quarantine_reasons") > 1).count(), 6)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Commandes — exactitude des montants
# MAGIC
# MAGIC C'est ici qu'un nettoyage approximatif se voit : un cast naïf laisserait 1 102
# MAGIC prix à `null` et amputerait le chiffre d'affaires.

# COMMAND ----------

g.equals("aucun unit_price nul",
         lambda: sil("order_line").filter(F.col("unit_price").isNull()).count(), 0)
g.equals("aucun order_ts nul",
         lambda: sil("order_line").filter(F.col("order_ts").isNull()).count(), 0)
g.equals("somme de net_amount",
         lambda: str(sil("order_line").agg(F.sum("net_amount")).first()[0]), "28983772.55")
g.equals("somme de net_amount sur les lignes de CA",
         lambda: str(sil("order_line").filter("is_revenue").agg(F.sum("net_amount")).first()[0]),
         "24049792.86")
g.equals("lignes de chiffre d'affaires", lambda: sil("order_line").filter("is_revenue").count(), 234_272)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Commandes — intégrité référentielle signalée, pas masquée

# COMMAND ----------

g.equals("lignes à client orphelin", lambda: sil("order_line").filter("is_orphan_customer").count(), 1_721)
g.equals("lignes à produit orphelin", lambda: sil("order_line").filter("is_orphan_product").count(), 545)
g.equals("première date de commande",
         lambda: str(sil("order_line").agg(F.min("order_date")).first()[0]), "2025-12-01")
g.equals("dernière date de commande",
         lambda: str(sil("order_line").agg(F.max("order_date")).first()[0]), "2026-06-02")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Événements

# COMMAND ----------

g.equals("silver.event : lignes", lambda: sil("event").count(), 130_025)
g.equals("silver.event : event_id unique",
         lambda: sil("event").select("event_id").distinct().count(), 130_025)
g.equals("silver.event : aucun event_ts nul",
         lambda: sil("event").filter(F.col("event_ts").isNull()).count(), 0)
g.equals("silver.event : event_ts typé timestamp",
         lambda: dict(sil("event").dtypes).get("event_ts"), "timestamp")
g.equals("quarantine_event : lignes", lambda: ops("quarantine_event").count(), 389)
g.equals("événements identifiés",
         lambda: sil("event").filter(F.col("customer_id").isNotNull()).count(), 114_396)

# COMMAND ----------

g.equals("silver.event_item : lignes", lambda: sil("event_item").count(), 26_183)
g.equals("silver.event_item : événements concernés",
         lambda: sil("event_item").select("event_id").distinct().count(), 15_755)
g.equals("silver.event_item : aucun qty nul",
         lambda: sil("event_item").filter(F.col("qty").isNull()).count(), 0)
g.equals("silver.event_item : aucun price nul",
         lambda: sil("event_item").filter(F.col("price").isNull()).count(), 0)
g.equals("silver.event_item : price typé decimal(10,2)",
         lambda: dict(sil("event_item").dtypes).get("price"), "decimal(10,2)")
g.truthy("silver.event_item : indice de position conservé",
         lambda: "item_index" in dict(sil("event_item").dtypes))

# COMMAND ----------


def type_distribution():
    rows = sil("event").groupBy("event_type").count().collect()
    return {r["event_type"]: r["count"] for r in rows}


g.equals("répartition par event_type", type_distribution, EVENT_TYPES)

# COMMAND ----------

g.report()
