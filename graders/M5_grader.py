# Databricks notebook source
# MAGIC %md
# MAGIC # Grader — M5 : couche gold
# MAGIC
# MAGIC Valeurs de référence produites par `generator/reference_gold.py`.

# COMMAND ----------

# MAGIC %run ./_grader_lib

# COMMAND ----------

dbutils.widgets.text("catalog", "novamarket", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

g = Grader(f"M5 — couche gold ({CATALOG})")

FACT_SCHEMA = [
    ("order_line_id", "string"), ("order_id", "string"), ("order_date", "date"),
    ("order_ts", "timestamp"), ("customer_id", "string"), ("seller_sk", "string"),
    ("seller_id", "string"), ("product_id", "string"), ("quantity", "int"),
    ("unit_price", "decimal(10,2)"), ("discount_amount", "decimal(10,2)"),
    ("gross_amount", "decimal(12,2)"), ("net_amount", "decimal(12,2)"),
    ("commission_rate", "decimal(5,3)"), ("commission_amount", "decimal(12,2)"),
    ("order_status", "string"), ("payment_method", "string"), ("shipping_country", "string"),
    ("is_revenue", "boolean"), ("is_orphan_customer", "boolean"),
    ("is_orphan_product", "boolean"), ("_processed_at", "timestamp"),
]

COMMENTED_COLUMNS = ["seller_sk", "commission_rate", "commission_amount",
                     "is_revenue", "net_amount"]

# COMMAND ----------


def gold(name):
    return spark.table(f"{CATALOG}.gold.{name}")


def scalar(sql):
    return spark.sql(sql).first()[0]


def money(sql):
    return str(scalar(sql))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensions

# COMMAND ----------

g.equals("dim_date : lignes", lambda: gold("dim_date").count(), 212)
g.equals("dim_date : clés uniques",
         lambda: gold("dim_date").select("date_key").distinct().count(), 212)
g.equals("dim_customer : lignes", lambda: gold("dim_customer").count(), 25_080)
g.equals("dim_customer : customer_id unique",
         lambda: gold("dim_customer").select("customer_id").distinct().count(), 25_080)
g.equals("dim_seller : lignes (toutes les versions)", lambda: gold("dim_seller").count(), 665)
g.equals("dim_seller : seller_sk unique",
         lambda: gold("dim_seller").select("seller_sk").distinct().count(), 665)
g.equals("dim_seller : vendeurs distincts",
         lambda: gold("dim_seller").select("seller_id").distinct().count(), 600)
g.equals("dim_product : lignes", lambda: gold("dim_product").count(), 8_000)
g.equals("ref_commission_plan : lignes", lambda: gold("ref_commission_plan").count(), 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fait

# COMMAND ----------

g.equals("fact_order_line : schéma exact",
         lambda: [(f.name, f.dataType.simpleString()) for f in gold("fact_order_line").schema],
         FACT_SCHEMA)
g.equals("fact_order_line : lignes (aucune perte en jointure)",
         lambda: gold("fact_order_line").count(), 282_104)
g.equals("fact_order_line : order_line_id unique",
         lambda: gold("fact_order_line").select("order_line_id").distinct().count(), 282_104)
g.equals("aucun seller_sk nul",
         lambda: gold("fact_order_line").filter(F.col("seller_sk").isNull()).count(), 0)
g.equals("intégrité référentielle fait -> dim_seller",
         lambda: gold("fact_order_line").join(
             gold("dim_seller").select("seller_sk"), on="seller_sk", how="left_anti").count(), 0)
g.equals("aucun commission_amount nul",
         lambda: gold("fact_order_line").filter(F.col("commission_amount").isNull()).count(), 0)
g.equals("commission nulle hors chiffre d'affaires",
         lambda: gold("fact_order_line")
                 .filter("NOT is_revenue AND commission_amount <> 0").count(), 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Le critère central
# MAGIC
# MAGIC La commission historisée et la commission calculée avec le plan courant doivent
# MAGIC différer de 45 765,97 €. Si elles sont égales, la clé de version ne résout pas la
# MAGIC bonne ligne de `dim_seller`.

# COMMAND ----------

g.equals("commission historisée",
         lambda: money(f"SELECT round(sum(commission_amount), 2) "
                       f"FROM {CATALOG}.gold.fact_order_line"),
         "3164343.53")

g.equals("commission avec le plan courant (contrôle négatif)",
         lambda: money(f"""
             SELECT round(sum(f.net_amount * c.commission_rate), 2)
             FROM {CATALOG}.gold.fact_order_line f
             JOIN {CATALOG}.gold.dim_seller d ON f.seller_id = d.seller_id AND d.is_current
             JOIN {CATALOG}.gold.ref_commission_plan c ON d.plan_code = c.plan_code
             WHERE f.is_revenue
         """),
         "3118577.56")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agrégats

# COMMAND ----------

g.equals("agg_revenue_monthly : lignes", lambda: gold("agg_revenue_monthly").count(), 13_333)
g.equals("agg_revenue_monthly : somme du CA net",
         lambda: money(f"SELECT round(sum(net_amount), 2) FROM {CATALOG}.gold.agg_revenue_monthly"),
         "24049792.86")
g.equals("agg_revenue_monthly : somme des commissions",
         lambda: money(f"SELECT round(sum(commission_amount), 2) "
                       f"FROM {CATALOG}.gold.agg_revenue_monthly"),
         "3164343.53")
g.equals("décembre 2025 : CA net",
         lambda: money(f"SELECT round(sum(net_amount), 2) FROM {CATALOG}.gold.agg_revenue_monthly "
                       f"WHERE year_month = '2025-12'"), "4840958.20")
g.equals("décembre 2025 : lignes",
         lambda: scalar(f"SELECT sum(n_lines) FROM {CATALOG}.gold.agg_revenue_monthly "
                        f"WHERE year_month = '2025-12'"), 47_515)
g.equals("juin 2026 : CA net",
         lambda: money(f"SELECT round(sum(net_amount), 2) FROM {CATALOG}.gold.agg_revenue_monthly "
                       f"WHERE year_month = '2026-06'"), "260610.22")
g.equals("juin 2026 : lignes",
         lambda: scalar(f"SELECT sum(n_lines) FROM {CATALOG}.gold.agg_revenue_monthly "
                        f"WHERE year_month = '2026-06'"), 2_529)

# COMMAND ----------

g.equals("agg_funnel_source : lignes", lambda: gold("agg_funnel_source").count(), 7)
g.equals("agg_funnel_source : sessions au total",
         lambda: scalar(f"SELECT sum(sessions) FROM {CATALOG}.gold.agg_funnel_source"), 31_867)
g.equals("agg_funnel_source : sessions avec achat",
         lambda: scalar(f"SELECT sum(sessions_purchase) FROM {CATALOG}.gold.agg_funnel_source"), 1_873)
g.truthy("agg_funnel_source : entonnoir décroissant",
         lambda: gold("agg_funnel_source").filter(
             "sessions < sessions_product_view OR sessions_product_view < sessions_add_to_cart "
             "OR sessions_add_to_cart < sessions_checkout_start "
             "OR sessions_checkout_start < sessions_purchase").count() == 0,
         hint="chaque étape <= la précédente")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vues

# COMMAND ----------

g.equals("v_top_products_90d : lignes", lambda: gold("v_top_products_90d").count(), 20)
g.equals("v_top_products_90d : produit en tête",
         lambda: gold("v_top_products_90d").orderBy(F.col("net_amount").desc())
                     .first()["product_id"], "P007960")
g.equals("v_top_products_90d : CA du produit en tête",
         lambda: str(gold("v_top_products_90d").orderBy(F.col("net_amount").desc())
                         .first()["net_amount"]), "9571.30")

for view in ["v_seller_quality_monthly", "v_basket_by_segment", "v_customer_cohort"]:
    g.truthy(f"{view} existe et renvoie des lignes",
             lambda v=view: gold(v).limit(1).count() == 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documentation

# COMMAND ----------


def table_comment():
    for row in spark.sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.gold.fact_order_line").collect():
        if str(row[0]).strip().lower() == "comment":
            return (row[1] or "").strip()
    return ""


def documented_columns():
    rows = spark.sql(f"DESCRIBE TABLE {CATALOG}.gold.fact_order_line").collect()
    return {r[0]: (r[2] or "").strip() for r in rows if r[0] in COMMENTED_COLUMNS}


g.truthy("fact_order_line : commentaire de table", lambda: len(table_comment()) > 0)
g.equals("fact_order_line : colonnes clés documentées",
         lambda: sorted(c for c, comment in documented_columns().items() if comment),
         sorted(COMMENTED_COLUMNS))

# COMMAND ----------

g.report()
