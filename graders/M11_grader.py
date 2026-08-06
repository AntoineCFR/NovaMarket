# Databricks notebook source
# MAGIC %md
# MAGIC # Grader — M11 : CI/CD
# MAGIC
# MAGIC À exécuter après avoir déployé **et lancé** le job de fumée sur les deux targets.
# MAGIC Critères comportementaux : le grader lit les traces de déploiement dans les deux
# MAGIC catalogs. Aucun comptage de données.

# COMMAND ----------

# MAGIC %run ./_grader_lib

# COMMAND ----------

dbutils.widgets.text("catalog_prod", "novamarket", "Catalog de production")
dbutils.widgets.text("catalog_dev", "novamarket_dev", "Catalog de développement")

PROD = dbutils.widgets.get("catalog_prod")
DEV = dbutils.widgets.get("catalog_dev")

from pyspark.sql import functions as F

g = Grader(f"M11 — CI/CD ({DEV} → {PROD})")

LOG_SCHEMA = [
    ("deployed_at", "timestamp"), ("bundle_name", "string"), ("bundle_target", "string"),
    ("job_name", "string"), ("catalog_used", "string"), ("git_branch", "string"),
]

# COMMAND ----------


def log(catalog):
    return spark.table(f"{catalog}.ops.deployment_log")


def latest(catalog, target):
    rows = (log(catalog)
            .filter(F.col("bundle_target") == target)
            .orderBy(F.col("deployed_at").desc())
            .limit(1).collect())
    return rows[0].asDict() if rows else None


# COMMAND ----------

# MAGIC %md
# MAGIC ## Les deux environnements existent

# COMMAND ----------

g.truthy("le catalog de développement existe",
         lambda: spark.sql(f"SHOW CATALOGS LIKE '{DEV}'").count() == 1)
g.equals("ops.deployment_log (prod) : schéma exact",
         lambda: [(f.name, f.dataType.simpleString()) for f in log(PROD).schema], LOG_SCHEMA)
g.equals("ops.deployment_log (dev) : schéma exact",
         lambda: [(f.name, f.dataType.simpleString()) for f in log(DEV).schema], LOG_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## La promotion a réellement eu lieu

# COMMAND ----------

g.truthy("une exécution enregistrée sur le target dev",
         lambda: latest(DEV, "dev") is not None)
g.truthy("une exécution enregistrée sur le target prod",
         lambda: latest(PROD, "prod") is not None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Les deux critères qui comptent
# MAGIC
# MAGIC Ils prouvent que **le même code** a produit deux déploiements différents — c'est
# MAGIC toute la définition de la promotion d'environnement.

# COMMAND ----------

g.truthy("la surcharge de variable a fonctionné : deux catalogs différents",
         lambda: latest(DEV, "dev")["catalog_used"] != latest(PROD, "prod")["catalog_used"],
         hint="catalog_used doit différer")
g.truthy("les modes de déploiement ont fonctionné : deux noms de job différents",
         lambda: latest(DEV, "dev")["job_name"] != latest(PROD, "prod")["job_name"],
         hint="le mode development préfixe les noms")
g.truthy("le nom du job de dev porte la marque du mode development",
         lambda: "dev" in latest(DEV, "dev")["job_name"].lower())
g.truthy("le bundle porte le même nom sur les deux targets",
         lambda: latest(DEV, "dev")["bundle_name"] == latest(PROD, "prod")["bundle_name"],
         hint="un seul bundle, deux cibles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Git et périmètre du bundle

# COMMAND ----------

g.soft("la branche Git est renseignée",
       lambda: (latest(PROD, "prod")["git_branch"] or "unknown") != "unknown",
       hint="exécution depuis un Git Folder")

# COMMAND ----------

# MAGIC %md
# MAGIC Le bundle doit déclarer autre chose que le job de fumée : le job quotidien de M8 et
# MAGIC le pipeline de M7. On le vérifie côté workspace, via l'API Jobs.

# COMMAND ----------


def workspace_job_names():
    from databricks.sdk import WorkspaceClient
    return {j.settings.name for j in WorkspaceClient().jobs.list()}


def workspace_pipeline_names():
    from databricks.sdk import WorkspaceClient
    return {p.name for p in WorkspaceClient().pipelines.list_pipelines()}


g.soft("le job quotidien de M8 est déployé par le bundle",
       lambda: any("novamarket_daily" in n for n in workspace_job_names()))
g.soft("le pipeline de M7 est déployé par le bundle",
       lambda: any("novamarket" in (n or "") for n in workspace_pipeline_names()))

# COMMAND ----------

g.report()
