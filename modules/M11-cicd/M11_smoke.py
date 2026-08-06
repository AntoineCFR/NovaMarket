# Databricks notebook source
# MAGIC %md
# MAGIC # M11 — Tâche de fumée
# MAGIC
# MAGIC Une seule tâche, qui tourne dans **n'importe quel** environnement, y compris un
# MAGIC catalog vide. Son unique travail : laisser une trace vérifiable de ce qui a été
# MAGIC déployé, où, et sous quel nom.
# MAGIC
# MAGIC C'est ce qui transforme « j'ai déployé » d'une affirmation en un fait contrôlable.

# COMMAND ----------

from datetime import datetime

# Ces paramètres sont alimentés par le bundle via des références dynamiques.
# En exécution manuelle, les valeurs par défaut prennent le relais.
dbutils.widgets.text("catalog", "novamarket", "Catalog")
dbutils.widgets.text("bundle_name", "local", "Nom du bundle")
dbutils.widgets.text("bundle_target", "manual", "Target du bundle")
dbutils.widgets.text("job_name", "manual", "Nom du job")

CATALOG = dbutils.widgets.get("catalog")
BUNDLE_NAME = dbutils.widgets.get("bundle_name")
BUNDLE_TARGET = dbutils.widgets.get("bundle_target")
JOB_NAME = dbutils.widgets.get("job_name")

print(f"catalog : {CATALOG}")
print(f"bundle  : {BUNDLE_NAME} / {BUNDLE_TARGET}")
print(f"job     : {JOB_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## La branche Git
# MAGIC
# MAGIC Quand le notebook tourne depuis un Git Folder, le contexte d'exécution connaît la
# MAGIC branche. Hors Git Folder, il n'y a rien à trouver — et c'est une information en soi.

# COMMAND ----------

# TODO A — récupère le nom de la branche Git courante.
# Piste : dbutils.notebook.entry_point.getDbutils().notebook().getContext()
# expose des attributs de contexte. Enveloppe l'accès dans un try/except : hors
# Git Folder, l'information n'existe pas, et le notebook ne doit pas planter pour ça.

GIT_BRANCH = "unknown"

print(f"branche : {GIT_BRANCH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## La trace de déploiement
# MAGIC
# MAGIC Le schéma est imposé — le grader s'appuie dessus.

# COMMAND ----------

# TODO B — crée le schema ops s'il n'existe pas, puis la table ops.deployment_log
#   deployed_at TIMESTAMP, bundle_name STRING, bundle_target STRING,
#   job_name STRING, catalog_used STRING, git_branch STRING


# COMMAND ----------

# TODO C — insère une ligne décrivant cette exécution.
# Attention : `catalog_used` doit contenir le catalog RÉELLEMENT utilisé, celui
# que la variable de target a fourni. C'est ce qui prouve que la surcharge a marché.


# COMMAND ----------

from pyspark.sql import functions as F

display(
    spark.table(f"{CATALOG}.ops.deployment_log")
    .orderBy(F.col("deployed_at").desc())
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contrôle de fumée
# MAGIC
# MAGIC Une tâche de fumée doit échouer bruyamment si l'environnement n'est pas utilisable.
# MAGIC Un job vert dans un environnement cassé est pire qu'un job rouge.

# COMMAND ----------

assert spark.sql(f"SELECT current_catalog()") is not None
assert spark.table(f"{CATALOG}.ops.deployment_log").count() >= 1, \
    "la trace de deploiement n'a pas ete ecrite"

print("environnement operationnel")
