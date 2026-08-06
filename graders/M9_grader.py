# Databricks notebook source
# MAGIC %md
# MAGIC # Grader — M9 : capstone
# MAGIC
# MAGIC À exécuter **après réparation complète**. État attendu : S4 (W1 → W4), référentiel
# MAGIC restauré, règle de vraisemblance d'échelle active.

# COMMAND ----------

# MAGIC %run ./_grader_lib

# COMMAND ----------

dbutils.widgets.text("catalog", "novamarket", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F

g = Grader(f"M9 — capstone ({CATALOG})")

INCIDENT_SCHEMA = {
    "incident_id": "string", "detected_at": "timestamp", "detected_by": "string",
    "severity": "string", "title": "string", "symptom": "string", "root_cause": "string",
    "affected_rows": "bigint", "impact_amount": "decimal(18,2)", "containment": "string",
    "remediation": "string", "prevention": "string", "status": "string",
}

# COMMAND ----------


def t(name):
    return spark.table(f"{CATALOG}.{name}")


def reason(motif):
    return (t("ops.quarantine_order_line")
            .select(F.explode("quarantine_reasons").alias("m"))
            .filter(F.col("m") == motif).count())


# COMMAND ----------

# MAGIC %md
# MAGIC ## L'ingestion est complète

# COMMAND ----------

g.equals("bronze.orders_raw : lignes", lambda: t("bronze.orders_raw").count(), 290_711)
g.equals("bronze.orders_raw : fichiers sources distincts",
         lambda: t("bronze.orders_raw").select("_source_file").distinct().count(), 10)
g.equals("bronze.ref_products_raw : référentiel restauré",
         lambda: t("bronze.ref_products_raw").count(), 8_000)
g.equals("dernière date de commande",
         lambda: str(t("silver.order_line").agg(F.max("order_date")).first()[0]), "2026-06-04")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Les lignes fautives sont écartées, pas supprimées

# COMMAND ----------

g.equals("silver.order_line : lignes", lambda: t("silver.order_line").count(), 284_909)
g.equals("ops.quarantine_order_line : lignes", lambda: t("ops.quarantine_order_line").count(), 2_305)
g.equals("INVARIANT — silver + quarantaine",
         lambda: t("silver.order_line").count() + t("ops.quarantine_order_line").count(), 287_214)
g.equals("motif SUSPECTED_UNIT_SCALE", lambda: reason("SUSPECTED_UNIT_SCALE"), 55)
g.equals("motif INVALID_TIMESTAMP", lambda: reason("INVALID_TIMESTAMP"), 1_435)
g.equals("motif INVALID_QUANTITY", lambda: reason("INVALID_QUANTITY"), 821)
g.equals("motif INVALID_PRICE", lambda: reason("INVALID_PRICE"), 1)
g.equals("motif UNKNOWN_STATUS", lambda: reason("UNKNOWN_STATUS"), 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Le chiffre d'affaires est redevenu juste

# COMMAND ----------

g.equals("CA net sur les lignes de chiffre d'affaires",
         lambda: str(t("silver.order_line").filter("is_revenue")
                      .agg(F.sum("net_amount")).first()[0]), "24295870.03")
g.equals("lignes à produit orphelin",
         lambda: t("silver.order_line").filter("is_orphan_product").count(), 548)
g.equals("gold.fact_order_line : lignes", lambda: t("gold.fact_order_line").count(), 284_909)
g.equals("aucune ligne de fait au montant aberrant",
         lambda: t("gold.fact_order_line").filter("net_amount > 100000").count(), 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## L'incident est documenté

# COMMAND ----------

g.truthy("ops.incident_log : schéma conforme",
         lambda: all(dict(t("ops.incident_log").dtypes).get(c) == ty
                     for c, ty in INCIDENT_SCHEMA.items()),
         hint="13 colonnes, types imposés")
g.truthy("au moins 2 incidents distincts",
         lambda: t("ops.incident_log").select("incident_id").distinct().count() >= 2)
g.truthy("un incident chiffré à 439 591,92 €",
         lambda: t("ops.incident_log")
                  .filter(F.col("impact_amount") == F.lit("439591.92").cast("decimal(18,2)"))
                  .count() >= 1,
         hint="impact_amount = 439591.92")
g.equals("aucun incident laissé ouvert",
         lambda: t("ops.incident_log").filter("status = 'OPEN'").count(), 0)
g.truthy("chaque incident porte une cause racine et une prévention",
         lambda: t("ops.incident_log")
                  .filter((F.trim(F.coalesce("root_cause", F.lit(""))) == "")
                          | (F.trim(F.coalesce("prevention", F.lit(""))) == "")).count() == 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Les contrôles manquants ont été ajoutés

# COMMAND ----------


def check_names():
    ts = t("ops.dq_metrics").agg(F.max("measured_at")).first()[0]
    return {r["check_name"] for r in t("ops.dq_metrics")
            .filter(F.col("measured_at") == F.lit(ts)).select("check_name").distinct().collect()}


g.truthy("contrôle daily_revenue_anomaly présent",
         lambda: "daily_revenue_anomaly" in check_names())
g.truthy("contrôle reference_volume_drop présent",
         lambda: "reference_volume_drop" in check_names())
g.truthy("les contrôles historiques sont toujours là",
         lambda: {"row_count", "duplicate_keys", "orphan_product_rows"}.issubset(check_names()),
         hint="ne pas casser le moteur de M6")

# COMMAND ----------

g.report()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fin du parcours
# MAGIC
# MAGIC Si ce grader passe au vert, tu as construit — et réparé — une plateforme de données
# MAGIC complète : quatre sources hétérogènes, quatre couches, historisation, qualité
# MAGIC instrumentée, orchestration, et un incident de production diagnostiqué à partir
# MAGIC d'une phrase vague dans un message.
# MAGIC
# MAGIC Relis tes réponses de M1 à M9 d'affilée. C'est le vrai livrable.
