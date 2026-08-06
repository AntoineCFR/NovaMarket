# Databricks notebook source
# MAGIC %md
# MAGIC # Corrigé — M7 : source du pipeline déclaratif

# COMMAND ----------

import dlt
from pyspark.sql import functions as F, Window as W

SOURCE_PATH = "/Volumes/novamarket/landing/files/orders"

VALID_STATUSES = ["DELIVERED", "SHIPPED", "PENDING", "CANCELLED", "RETURNED"]
NON_REVENUE_STATUSES = ["CANCELLED", "RETURNED"]
TS_FORMAT = "yyyy-MM-dd HH:mm:ss"

SOURCE_COLS = ["order_id", "order_line_id", "order_ts", "customer_id", "seller_id",
               "product_id", "quantity", "unit_price", "discount_amount", "currency",
               "shipping_country", "payment_method", "order_status", "shipping_address"]

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
# MAGIC ## TODO A — table de streaming
# MAGIC
# MAGIC Options identiques à M1, moins `cloudFiles.schemaLocation` et
# MAGIC `checkpointLocation` : le pipeline les gère.

# COMMAND ----------


@dlt.table(
    name="orders_bronze",
    comment="Lignes de commande brutes. Colonnes source en STRING, fidelite a la source.",
    table_properties={"quality": "bronze"},
)
def orders_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "windows-1252")
        .option("rescuedDataColumn", "_rescued_data")
        .load(SOURCE_PATH)
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataset intermédiaire : les motifs, calculés une seule fois
# MAGIC
# MAGIC C'est la clé de la question 3 du README. En factorisant déduplication et validation
# MAGIC dans un dataset intermédiaire, `order_line_silver` et `order_line_quarantine` en
# MAGIC dérivent tous les deux — et il devient **impossible** qu'ils divergent.
# MAGIC
# MAGIC Écrire les conditions deux fois, une fois en positif et une fois en négatif,
# MAGIC fonctionnerait aujourd'hui et serait faux dans six mois.
# MAGIC
# MAGIC `temporary=True` : le dataset existe dans le graphe du pipeline mais n'est pas
# MAGIC publié dans Unity Catalog. C'est de la plomberie, pas un livrable.

# COMMAND ----------


@dlt.table(name="order_line_validated", temporary=True)
def order_line_validated():
    w = W.partitionBy("order_line_id").orderBy(F.col("_source_file").asc())
    return (
        dlt.read("orders_bronze")
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1).drop("_rn")
        .withColumn("quarantine_reasons", F.array_compact(F.array(
            F.when(parse_ts("order_ts").isNull(), F.lit("INVALID_TIMESTAMP")),
            F.when((clean_int("quantity").isNull()) | (clean_int("quantity") <= 0),
                   F.lit("INVALID_QUANTITY")),
            F.when((clean_decimal("unit_price").isNull()) | (clean_decimal("unit_price") <= 0),
                   F.lit("INVALID_PRICE")),
            F.when(~clean_status("order_status").isin(VALID_STATUSES), F.lit("UNKNOWN_STATUS")),
        )))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO B — silver, avec attentes
# MAGIC
# MAGIC Les attentes portent sur le DataFrame renvoyé, donc **après** la déduplication
# MAGIC héritée de `order_line_validated`. C'est ce qui garantit 1 422 et 813, et non les
# MAGIC chiffres gonflés qu'on obtiendrait sur les 287 785 lignes brutes.
# MAGIC
# MAGIC Les conditions portent sur les colonnes **typées de sortie** : une attente ne peut
# MAGIC référencer que des colonnes présentes dans le DataFrame renvoyé.
# MAGIC
# MAGIC Note honnête : les prédicats sont donc exprimés deux fois — une fois comme attentes
# MAGIC ici, une fois comme `quarantine_reasons` dans le dataset intermédiaire. Le
# MAGIC **nettoyage** reste factorisé dans les fonctions `clean_*`, ce qui limite le risque,
# MAGIC mais la duplication des règles est réelle. C'est une partie de la réponse à la
# MAGIC question 2 du README : le modèle déclaratif impose ici une redite que le code
# MAGIC impératif de M3 n'avait pas.

# COMMAND ----------


@dlt.table(
    name="order_line_silver",
    comment="Lignes de commande typees et validees. Le CA ne se somme que sur is_revenue.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_timestamp", "order_ts IS NOT NULL")
@dlt.expect_or_drop("valid_quantity", "quantity IS NOT NULL AND quantity > 0")
@dlt.expect_or_drop("valid_price", "unit_price IS NOT NULL AND unit_price > 0")
@dlt.expect_or_drop(
    "known_status",
    "order_status IN ('DELIVERED', 'SHIPPED', 'PENDING', 'CANCELLED', 'RETURNED')")
def order_line_silver():
    return (
        dlt.read("order_line_validated")
        .withColumn("_ts", parse_ts("order_ts"))
        .withColumn("_qty", clean_int("quantity"))
        .withColumn("_unit", clean_decimal("unit_price"))
        .withColumn("_disc", F.coalesce(clean_decimal("discount_amount"),
                                        F.lit(0).cast("decimal(10,2)")))
        .withColumn("_gross", (F.col("_qty") * F.col("_unit")).cast("decimal(12,2)"))
        .select(
            "order_line_id", "order_id",
            F.col("_ts").alias("order_ts"),
            F.col("_ts").cast("date").alias("order_date"),
            "customer_id", "seller_id", "product_id",
            F.col("_qty").alias("quantity"),
            F.col("_unit").alias("unit_price"),
            F.col("_disc").alias("discount_amount"),
            F.col("_gross").alias("gross_amount"),
            (F.col("_gross") - F.col("_disc")).cast("decimal(12,2)").alias("net_amount"),
            "currency", "shipping_country", "payment_method",
            clean_status("order_status").alias("order_status"),
            (~clean_status("order_status").isin(NON_REVENUE_STATUSES)).alias("is_revenue"),
            "shipping_address", "_source_file",
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO C — la quarantaine
# MAGIC
# MAGIC Le miroir exact, dérivé du même dataset intermédiaire.

# COMMAND ----------


@dlt.table(
    name="order_line_quarantine",
    comment="Lignes ecartees par les attentes, avec leur donnee brute et leurs motifs. "
            "Existe parce que expect_or_drop compte les rejets mais ne les conserve pas.",
    table_properties={"quality": "ops"},
)
def order_line_quarantine():
    return (
        dlt.read("order_line_validated")
        .filter(F.size("quarantine_reasons") > 0)
        .select(*SOURCE_COLS, "_rescued_data", "_source_file",
                "quarantine_reasons",
                F.current_timestamp().alias("quarantined_at"))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO D — l'agrégat

# COMMAND ----------


@dlt.table(
    name="revenue_by_month_country",
    comment="CA net par mois et pays de livraison, sur les lignes de chiffre d affaires.",
    table_properties={"quality": "gold"},
)
def revenue_by_month_country():
    return (
        dlt.read("order_line_silver")
        .filter("is_revenue")
        .groupBy(F.date_format("order_date", "yyyy-MM").alias("year_month"),
                 F.col("shipping_country"))
        .agg(F.sum("net_amount").cast("decimal(18,2)").alias("net_amount"),
             F.count("*").alias("n_lines"),
             F.countDistinct("order_id").alias("n_orders"))
    )
