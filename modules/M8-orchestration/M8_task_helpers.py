# Databricks notebook source
# MAGIC %md
# MAGIC # M8 — Adaptations à apporter aux notebooks existants
# MAGIC
# MAGIC Ce notebook ne produit rien seul. Il rassemble les quatre modifications à
# MAGIC répercuter dans les notebooks de M1 à M6 pour qu'ils deviennent des tâches de job.
# MAGIC
# MAGIC Le principe général : un notebook doit fonctionner **à l'identique** en exécution
# MAGIC manuelle et en tâche de job. Si tu dois modifier du code pour passer de l'un à
# MAGIC l'autre, tu testes autre chose que ce que tu déploies.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Le `run_id` vient du job quand il existe
# MAGIC
# MAGIC En exécution manuelle, il n'y a pas de job : on retombe sur un identifiant local.
# MAGIC En tâche de job, le paramètre `{{job.run_id}}` est résolu par Databricks.
# MAGIC
# MAGIC C'est ce qui permet de retrouver toutes les tâches d'une exécution dans
# MAGIC `ops.pipeline_runs` — sans ça, un job de nuit qui plante est indéboguable.

# COMMAND ----------

import uuid

dbutils.widgets.text("catalog", "novamarket", "Catalog")
dbutils.widgets.text("run_id", "", "Run ID (fourni par le job)")

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = dbutils.widgets.get("run_id") or str(uuid.uuid4())

print(f"catalog : {CATALOG}")
print(f"run_id  : {RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `M1_bronze_ref.py` devient paramétrable
# MAGIC
# MAGIC Aujourd'hui il boucle sur les trois référentiels. Pour la tâche `for_each`, il doit
# MAGIC n'en traiter qu'un, choisi par un widget.
# MAGIC
# MAGIC Chaque itération écrit sa propre ligne dans `ops.pipeline_runs`, avec `source_name`
# MAGIC égal au nom du référentiel : c'est ce qui rend le `for_each` observable.

# COMMAND ----------

# TODO A — adapter M1_bronze_ref.py
#
# dbutils.widgets.text("ref_name", "products", "Referentiel a charger")
# REF_NAME = dbutils.widgets.get("ref_name")
# TARGETS = {"categories": "ref_categories_raw", ...}
#
# puis un seul appel à load_ref(REF_NAME, TARGETS[REF_NAME])
# et un log_run(..., task_name="bronze_ref", source_name=REF_NAME, run_id=RUN_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `dq_checks` publie une valeur de tâche
# MAGIC
# MAGIC À ajouter en fin de `M6_qualite.py`. La valeur de tâche est le seul moyen propre de
# MAGIC faire circuler un résultat entre deux tâches : ni fichier temporaire, ni table de
# MAGIC passage, ni variable globale.
# MAGIC
# MAGIC Le point qui compte : ce n'est pas « aucun contrôle n'a échoué », c'est « aucun
# MAGIC contrôle **bloquant** n'a échoué ». Tu as tranché ça en M6 (question 3).

# COMMAND ----------

# TODO B — publier dq_status
#
# n_blocking_failures = (
#     spark.table(f"{CATALOG}.ops.dq_metrics")
#          .filter((F.col("run_id") == RUN_ID) & (F.col("status") == "FAIL"))
#          .count()
# )
# dbutils.jobs.taskValues.set(key="dq_status", value=... )
# dbutils.jobs.taskValues.set(key="n_failures", value=int(n_blocking_failures))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. La tâche conditionnelle
# MAGIC
# MAGIC Dans l'interface : *Add task → If/else condition*.
# MAGIC
# MAGIC | Champ | Valeur |
# MAGIC |---|---|
# MAGIC | Operand gauche | `{{tasks.dq_checks.values.dq_status}}` |
# MAGIC | Opérateur | `==` |
# MAGIC | Operand droit | `PASS` |
# MAGIC
# MAGIC Les dépendances des tâches suivantes se règlent sur la branche `true` ou `false`.
# MAGIC
# MAGIC Attention : une tâche dont la condition n'est pas remplie est marquée **skipped**,
# MAGIC pas *failed*. Un job dont toutes les tâches critiques ont été sautées se termine en
# MAGIC succès. Réfléchis à ce que tu veux vraiment sur la branche `false` — c'est le sujet
# MAGIC de la question 3 du README.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Vérification avant de lancer le job
# MAGIC
# MAGIC Exécute cette cellule après avoir adapté tes notebooks : elle montre si le `run_id`
# MAGIC circule correctement entre les tâches.

# COMMAND ----------

from pyspark.sql import functions as F

display(
    spark.table(f"{CATALOG}.ops.pipeline_runs")
    .groupBy("run_id")
    .agg(F.countDistinct("task_name").alias("taches_distinctes"),
         F.collect_set("task_name").alias("taches"),
         F.min("started_at").alias("debut"),
         F.max("ended_at").alias("fin"))
    .orderBy(F.col("debut").desc())
    .limit(5)
)
