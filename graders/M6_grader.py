# Databricks notebook source
# MAGIC %md
# MAGIC # Grader — M6 : qualité, métadonnées et observabilité

# COMMAND ----------

# MAGIC %run ./_grader_lib

# COMMAND ----------

dbutils.widgets.text("catalog", "novamarket", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F

g = Grader(f"M6 — qualité et métadonnées ({CATALOG})")

DQ_SCHEMA = [
    ("run_id", "string"), ("measured_at", "timestamp"), ("layer", "string"),
    ("table_name", "string"), ("check_name", "string"), ("metric_value", "double"),
    ("threshold", "double"), ("comparison", "string"), ("status", "string"),
]

EXPECTED_CHECKS = {
    ("bronze.orders_raw", "row_count"): 287_785.0,
    # Pas "rescued_rows" : sur les commandes, `_rescued_data` est vide (cf. M1 etape 5).
    # Les 1 087 lignes abimees ne se reperent qu'a leur `shipping_address` tronquee.
    ("bronze.orders_raw", "truncated_rows"): 1_087.0,
    ("bronze.events_raw", "row_count"): 131_068.0,
    ("bronze.events_raw", "malformed_rows"): 389.0,
    ("bronze.ref_products_raw", "row_count"): 8_000.0,
    ("bronze.app_customers_raw", "row_count"): 25_971.0,
    ("silver.order_line", "row_count"): 282_104.0,
    ("silver.order_line", "duplicate_keys"): 0.0,
    ("silver.order_line", "null_unit_price"): 0.0,
    ("silver.order_line", "orphan_customer_rows"): 1_721.0,
    ("silver.order_line", "orphan_product_rows"): 545.0,
    ("ops.quarantine_order_line", "row_count"): 2_229.0,
    ("silver.seller_scd2", "multiple_current_versions"): 0.0,
    ("silver.seller_scd2", "chain_breaks"): 0.0,
    ("gold.fact_order_line", "row_count"): 282_104.0,
    ("gold.fact_order_line", "orphan_seller_sk"): 0.0,
}

INVARIANTS = [k for k, v in EXPECTED_CHECKS.items() if v == 0.0]

EXPECTED_VIOLATIONS = {
    "ORDER_LINE_ID_UNIQUE": 3_452,
    "ORDER_TS_PARSABLE": 1_422,
    "QUANTITY_POSITIVE": 813,
    "UNIT_PRICE_NUMERIC": 1_102,
    "CURRENCY_ALWAYS_EUR": 0,
    "EVENT_TS_ISO8601": 1_267,
}

PII_COLUMNS = {"first_name", "last_name", "email", "zip_code"}

# COMMAND ----------


def dq():
    return spark.table(f"{CATALOG}.ops.dq_metrics")


def latest_run():
    ts = dq().agg(F.max("measured_at")).first()[0]
    return dq().filter(F.col("measured_at") == F.lit(ts))


def checks_map():
    rows = latest_run().select("table_name", "check_name", "metric_value").collect()
    return {(r["table_name"], r["check_name"]): float(r["metric_value"]) for r in rows}


def status_map():
    rows = latest_run().select("table_name", "check_name", "status").collect()
    return {(r["table_name"], r["check_name"]): r["status"] for r in rows}


def inconsistent_statuses():
    """Nombre de lignes dont le statut ne decoule pas de la comparaison declaree."""
    expected = (
        F.when(
            ((F.col("comparison") == "<=") & (F.col("metric_value") <= F.col("threshold")))
            | ((F.col("comparison") == ">=") & (F.col("metric_value") >= F.col("threshold")))
            | ((F.col("comparison") == "==") & (F.col("metric_value") == F.col("threshold"))),
            F.lit("PASS"))
        .otherwise(F.lit("NOT_PASS"))
    )
    return (latest_run()
            .withColumn("_expected", expected)
            .filter(((F.col("_expected") == "PASS") & (F.col("status") != "PASS"))
                    | ((F.col("_expected") == "NOT_PASS") & (F.col("status") == "PASS")))
            .count())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Contrôles de qualité

# COMMAND ----------

g.equals("ops.dq_metrics : schéma exact",
         lambda: [(f.name, f.dataType.simpleString()) for f in dq().schema], DQ_SCHEMA)
g.equals("16 contrôles sur la dernière exécution", lambda: latest_run().count(), 16)
g.equals("les 16 mesures attendues", checks_map, EXPECTED_CHECKS)
g.equals("statuts cohérents avec seuil et comparaison", inconsistent_statuses, 0)
g.equals("les 6 invariants sont en PASS",
         lambda: sum(1 for k in INVARIANTS if status_map().get(k) == "PASS"), len(INVARIANTS))
g.truthy("valeurs de statut valides",
         lambda: set(r["status"] for r in latest_run().select("status").collect())
                 <= {"PASS", "WARN", "FAIL"},
         hint="PASS / WARN / FAIL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Écarts au contrat d'interface

# COMMAND ----------


def violations_map():
    rows = spark.table(f"{CATALOG}.ops.contract_violations") \
                .select("rule_code", "violation_rows").collect()
    return {r["rule_code"]: int(r["violation_rows"]) for r in rows}


def rate_mismatches():
    return (spark.table(f"{CATALOG}.ops.contract_violations")
            .filter(F.col("scope_rows") > 0)
            .filter(F.abs(F.col("violation_rate")
                          - F.col("violation_rows") / F.col("scope_rows")) > 1e-6)
            .count())


g.equals("les 6 règles avec le bon nombre de violations", violations_map, EXPECTED_VIOLATIONS)
g.truthy("CURRENCY_ALWAYS_EUR est présente et à zéro",
         lambda: violations_map().get("CURRENCY_ALWAYS_EUR") == 0,
         hint="un contrôle qui passe est une information")
g.equals("violation_rate cohérent avec violation_rows / scope_rows", rate_mismatches, 0)
g.truthy("scope_rows renseigné partout",
         lambda: spark.table(f"{CATALOG}.ops.contract_violations")
                      .filter(F.col("scope_rows").isNull() | (F.col("scope_rows") == 0)).count() == 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bilan des lignes sauvées

# COMMAND ----------


def rescued_map():
    rows = spark.table(f"{CATALOG}.ops.dq_rescued_summary") \
                .select("table_name", "rescue_reason", "n_rows").collect()
    return {(r["table_name"], r["rescue_reason"]): int(r["n_rows"]) for r in rows}


g.equals("orders_raw / EXTRA_COLUMNS",
         lambda: rescued_map().get(("bronze.orders_raw", "EXTRA_COLUMNS")), 1_087)
g.equals("events_raw / MALFORMED_JSON",
         lambda: rescued_map().get(("bronze.events_raw", "MALFORMED_JSON")), 389)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Métadonnées

# COMMAND ----------


def undocumented(schema):
    """Tables managees sans commentaire. Les vues sont recommandees mais non exigees."""
    rows = spark.sql(f"""
        SELECT table_name FROM {CATALOG}.information_schema.tables
        WHERE table_schema = '{schema}'
          AND table_type IN ('MANAGED', 'EXTERNAL')
          AND (comment IS NULL OR trim(comment) = '')
        ORDER BY table_name
    """).collect()
    return [r["table_name"] for r in rows]


def pii_tagged_columns():
    rows = spark.sql(f"""
        SELECT column_name FROM {CATALOG}.information_schema.column_tags
        WHERE lower(tag_name) = 'pii'
    """).collect()
    return len(rows)


g.equals("aucune table silver sans commentaire", lambda: undocumented("silver"), [])
g.equals("aucune table gold sans commentaire", lambda: undocumented("gold"), [])
g.soft("colonnes étiquetées pii", lambda: pii_tagged_columns() >= 8, hint=">= 8")

# COMMAND ----------

g.report()
