# Databricks notebook source
# MAGIC %md
# MAGIC # Grader — M12 : performance et monitoring
# MAGIC
# MAGIC Aucun seuil de durée en dur : une mesure dépend du compute, et un grader qui
# MAGIC exigerait « moins de 3 secondes » serait faux la semaine prochaine. Les critères
# MAGIC portent sur la **cohérence** des mesures, pas sur leur valeur.

# COMMAND ----------

# MAGIC %run ./_grader_lib

# COMMAND ----------

dbutils.widgets.text("catalog", "novamarket", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F, Window as W

g = Grader(f"M12 — performance et monitoring ({CATALOG})")

PERF_SCHEMA = [
    ("scenario", "string"), ("variant", "string"), ("config", "string"),
    ("duration_ms", "bigint"), ("rows_out", "bigint"), ("measured_at", "timestamp"),
]

# COMMAND ----------


def t(name):
    return spark.table(f"{CATALOG}.ops.{name}")


def detail(table):
    return spark.sql(f"DESCRIBE DETAIL {CATALOG}.ops.{table}").first()


def top_key_share():
    df = t("skew_demo").groupBy("seller_id").count()
    total = df.agg(F.sum("count")).first()[0]
    top = df.agg(F.max("count")).first()[0]
    return top / total if total else 0.0


# COMMAND ----------

# MAGIC %md
# MAGIC ## Le déséquilibre est réel

# COMMAND ----------

g.truthy("ops.skew_demo existe", lambda: t("skew_demo").count() > 0)
g.truthy("une clé concentre plus de 30 % des lignes",
         lambda: top_key_share() > 0.30,
         hint="> 0,30")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Les mesures sont exploitables

# COMMAND ----------

g.equals("ops.perf_measurements : schéma exact",
         lambda: [(f.name, f.dataType.simpleString()) for f in t("perf_measurements").schema],
         PERF_SCHEMA)
g.truthy("au moins 2 scénarios distincts",
         lambda: t("perf_measurements").select("scenario").distinct().count() >= 2)
g.truthy("un scénario compte au moins 3 variantes",
         lambda: t("perf_measurements").groupBy("scenario")
                  .agg(F.countDistinct("variant").alias("n"))
                  .filter("n >= 3").count() >= 1,
         hint="les trois réglages de jointure")
g.equals("toutes les durées sont strictement positives",
         lambda: t("perf_measurements").filter("duration_ms <= 0").count(), 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Le critère qui sépare une mesure d'une impression
# MAGIC
# MAGIC Deux variantes d'un même scénario qui ne renvoient pas le même nombre de lignes ne
# MAGIC mesurent pas la même chose. La comparaison de leurs durées ne veut alors rien dire.

# COMMAND ----------


def scenarios_with_inconsistent_rows():
    return (t("perf_measurements")
            .groupBy("scenario")
            .agg(F.countDistinct("rows_out").alias("n"))
            .filter("n > 1").count())


g.equals("au sein d'un scénario, toutes les variantes renvoient le même rows_out",
         scenarios_with_inconsistent_rows, 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Le regroupement liquide

# COMMAND ----------

g.truthy("ops.perf_clustered déclare des colonnes de regroupement",
         lambda: bool(detail("perf_clustered")["clusteringColumns"]),
         hint="CLUSTER BY")
g.truthy("ops.perf_baseline n'en déclare pas",
         lambda: not detail("perf_baseline")["clusteringColumns"],
         hint="le témoin doit rester un témoin")
g.truthy("les deux tables contiennent le même nombre de lignes",
         lambda: t("perf_baseline").count() == t("perf_clustered").count(),
         hint="sinon la comparaison est biaisée")
g.truthy("le scénario de regroupement est mesuré sur les deux variantes",
         lambda: t("perf_measurements")
                  .filter(F.lower("scenario").contains("cluster"))
                  .select("variant").distinct().count() >= 2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Les tendances de jobs

# COMMAND ----------

g.truthy("ops.job_perf_trend existe et couvre plusieurs tâches",
         lambda: t("job_perf_trend").select("task_name").distinct().count() >= 3)
g.truthy("ops.job_perf_trend porte une durée et un écart à la moyenne",
         lambda: {"last_duration_ms", "avg_duration_ms", "deviation"}
                 .issubset(dict(t("job_perf_trend").dtypes)))
g.equals("aucune durée négative dans les tendances",
         lambda: t("job_perf_trend").filter("last_duration_ms < 0").count(), 0)

# COMMAND ----------

g.report()
