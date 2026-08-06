# Databricks notebook source
# MAGIC %md
# MAGIC # Grader — M7 : pipeline déclaratif Lakeflow
# MAGIC
# MAGIC À exécuter après une mise à jour réussie du pipeline **et** le notebook
# MAGIC d'exploitation du journal d'événements.

# COMMAND ----------

# MAGIC %run ./_grader_lib

# COMMAND ----------

dbutils.widgets.text("catalog", "novamarket", "Catalog")
dbutils.widgets.text("ldp_schema", "ldp", "Schema du pipeline")
CATALOG = dbutils.widgets.get("catalog")
LDP = f"{CATALOG}.{dbutils.widgets.get('ldp_schema')}"

from pyspark.sql import functions as F

g = Grader(f"M7 — pipeline déclaratif ({LDP})")

SOURCE_COLS = ["order_id", "order_line_id", "order_ts", "customer_id", "seller_id",
               "product_id", "quantity", "unit_price", "discount_amount", "currency",
               "shipping_country", "payment_method", "order_status", "shipping_address"]

EXPECTED_EXPECTATIONS = {
    "valid_timestamp": 1_422,
    "valid_quantity": 813,
    "valid_price": 0,
    "known_status": 0,
}

# COMMAND ----------


def ldp(name):
    return spark.table(f"{LDP}.{name}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Les jeux de données du pipeline

# COMMAND ----------

g.equals("orders_bronze : lignes", lambda: ldp("orders_bronze").count(), 287_785)
g.truthy("orders_bronze : colonnes source en STRING",
         lambda: all(dict(ldp("orders_bronze").dtypes).get(c) == "string" for c in SOURCE_COLS))
g.truthy("orders_bronze : colonne de sauvetage présente",
         lambda: "_rescued_data" in dict(ldp("orders_bronze").dtypes))

g.equals("order_line_silver : lignes", lambda: ldp("order_line_silver").count(), 282_104)
g.equals("order_line_silver : order_line_id unique",
         lambda: ldp("order_line_silver").select("order_line_id").distinct().count(), 282_104)
g.equals("order_line_silver : aucun order_ts nul",
         lambda: ldp("order_line_silver").filter(F.col("order_ts").isNull()).count(), 0)

g.equals("order_line_quarantine : lignes", lambda: ldp("order_line_quarantine").count(), 2_229)
g.equals("INVARIANT — silver + quarantaine",
         lambda: ldp("order_line_silver").count() + ldp("order_line_quarantine").count(), 284_333)

# COMMAND ----------

# MAGIC %md
# MAGIC ## L'agrégat

# COMMAND ----------

g.equals("revenue_by_month_country : lignes", lambda: ldp("revenue_by_month_country").count(), 42)
g.equals("revenue_by_month_country : lignes de CA agrégées",
         lambda: int(spark.sql(f"SELECT sum(n_lines) FROM {LDP}.revenue_by_month_country")
                          .first()[0]), 234_272)
g.equals("revenue_by_month_country : CA net total",
         lambda: str(spark.sql(f"SELECT round(sum(net_amount), 2) "
                               f"FROM {LDP}.revenue_by_month_country").first()[0]),
         "24049792.86")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Les attentes, vues depuis le journal d'événements

# COMMAND ----------


def expectations_map():
    df = spark.table(f"{CATALOG}.ops.ldp_expectations")
    latest = df.agg(F.max("extracted_at")).first()[0]
    rows = df.filter(F.col("extracted_at") == F.lit(latest)) \
             .select("expectation_name", "failed_records").collect()
    return {r["expectation_name"]: int(r["failed_records"]) for r in rows}


g.equals("les 4 attentes avec le bon nombre d'échecs", expectations_map, EXPECTED_EXPECTATIONS)
g.truthy("ops.ldp_expectations : passed_records renseigné",
         lambda: spark.table(f"{CATALOG}.ops.ldp_expectations")
                      .filter(F.col("passed_records").isNull()).count() == 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Le test du module
# MAGIC
# MAGIC Deux implémentations indépendantes, la même donnée, les mêmes règles. Elles doivent
# MAGIC produire exactement le même ensemble de clés.

# COMMAND ----------


def divergence():
    a = spark.table(f"{CATALOG}.silver.order_line").select("order_line_id")
    b = ldp("order_line_silver").select("order_line_id")
    return a.exceptAll(b).count() + b.exceptAll(a).count()


g.equals("M3 et le pipeline convergent sur les mêmes lignes", divergence, 0)

# COMMAND ----------

g.report()
