# Databricks notebook source
# MAGIC %md
# MAGIC # M6 — Qualité, métadonnées et observabilité
# MAGIC
# MAGIC Aucune donnée métier nouvelle. On produit ce qui permet de faire confiance à
# MAGIC celles qui existent.

# COMMAND ----------

from pyspark.sql import functions as F, Window as W
from datetime import datetime
import uuid

CATALOG = "novamarket"
RUN_ID = str(uuid.uuid4())
STARTED_AT = datetime.now()

print(f"run_id : {RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Le moteur de contrôles
# MAGIC
# MAGIC Seize contrôles écrits à la main, c'est seize occasions de se tromper. Décris-les,
# MAGIC puis exécute-les en boucle.
# MAGIC
# MAGIC Un descripteur porte : la couche, la table, le nom du contrôle, la requête qui
# MAGIC produit **un seul nombre**, le seuil et la comparaison.

# COMMAND ----------

# TODO A — la structure de description des contrôles.
# Suggestion : une liste de tuples ou de dicts. Les requêtes renvoient un scalaire.

CHECKS = [
    # (layer, table_name, check_name, sql, threshold, comparison)
    ("bronze", "orders_raw", "row_count",
     f"SELECT count(*) FROM {CATALOG}.bronze.orders_raw", ..., ...),
    # ... les 15 autres
]

# COMMAND ----------

# TODO B — le moteur : exécute chaque contrôle, calcule le statut, écrit dans ops.dq_metrics.
# `status` se DÉDUIT de metric_value, threshold et comparison. Ne l'écris jamais à la main.


def evaluate(metric_value, threshold, comparison):
    ...


def run_checks(checks, run_id):
    ...


# COMMAND ----------

# TODO C — crée ops.dq_metrics (schéma imposé, voir README) puis lance les contrôles.


# COMMAND ----------

display(
    spark.table(f"{CATALOG}.ops.dq_metrics")
         .filter(F.col("run_id") == RUN_ID)
         .orderBy("layer", "table_name", "check_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Justifie tes seuils
# MAGIC
# MAGIC Les six invariants ont un seuil évident : 0. Les dix autres non.
# MAGIC
# MAGIC Écris ici, en une ligne chacun, pourquoi tu as retenu les seuils que tu as retenus.
# MAGIC Un seuil qu'on ne sait pas justifier est un seuil qu'on désactivera à la première
# MAGIC alerte de nuit.
# MAGIC
# MAGIC > …

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Les écarts au contrat d'interface
# MAGIC
# MAGIC Ce que la source promet dans `docs/02-sources-et-modele.md`, confronté à ce qu'elle
# MAGIC livre. Six règles, portées explicites.
# MAGIC
# MAGIC Attention à la portée : une règle de valeur se mesure sur les lignes **dédupliquées**
# MAGIC (284 333 commandes, 130 025 événements), la règle d'unicité sur le brut (287 785).
# MAGIC C'est pour ça que `scope_rows` est une colonne et pas une constante.

# COMMAND ----------

# TODO D — ops.contract_violations


# COMMAND ----------

display(spark.table(f"{CATALOG}.ops.contract_violations").orderBy("rule_code"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bilan des lignes sauvées
# MAGIC
# MAGIC Deux causes bien distinctes, qu'il ne faut pas mélanger dans un compteur unique :
# MAGIC une colonne en trop n'a pas la même gravité qu'un enregistrement illisible.

# COMMAND ----------

# TODO E — ops.dq_rescued_summary


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Documentation
# MAGIC
# MAGIC Commence par mesurer ta dette : combien de tables n'ont aucun commentaire ?

# COMMAND ----------


def undocumented_tables(schema):
    """`information_schema` évite une boucle de DESCRIBE et expose déjà le commentaire."""
    rows = spark.sql(f"""
        SELECT table_name FROM {CATALOG}.information_schema.tables
        WHERE table_schema = '{schema}'
          AND table_type IN ('MANAGED', 'EXTERNAL')
          AND (comment IS NULL OR trim(comment) = '')
        ORDER BY table_name
    """).collect()
    return [r["table_name"] for r in rows]


for schema in ["silver", "gold"]:
    missing = undocumented_tables(schema)
    print(f"{schema:8s} {len(missing)} table(s) sans commentaire : {missing}")

# COMMAND ----------

# TODO F — documente toutes les tables silver et gold.
# Un commentaire utile dit ce que la table N'EST PAS et quels pièges elle contient.


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Étiquetage des données personnelles

# COMMAND ----------

# TODO G — étiquette pii = true sur first_name, last_name, email, zip_code
#          dans silver.customer_scd2 et gold.dim_customer
# Syntaxe : ALTER TABLE ... ALTER COLUMN ... SET TAGS ('pii' = 'true')


# COMMAND ----------

display(spark.sql(f"""
    SELECT schema_name, table_name, column_name, tag_name, tag_value
    FROM {CATALOG}.information_schema.column_tags
    ORDER BY schema_name, table_name, column_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Lineage
# MAGIC
# MAGIC Non évalué. Ouvre `gold.fact_order_line` dans Catalog Explorer, onglet **Lineage**,
# MAGIC et remonte la chaîne jusqu'aux fichiers du volume.
# MAGIC
# MAGIC Puis pose-toi la question du README : ce graphe dit-il d'où vient `commission_rate` ?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Vue de service pour le tableau de bord
# MAGIC
# MAGIC De quoi construire un dashboard AI/BI en trois clics. Optionnel mais recommandé :
# MAGIC c'est le livrable que verront les gens qui ne liront jamais ton code.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.ops.v_dq_latest AS
    WITH latest AS (
        SELECT max(measured_at) AS ts FROM {CATALOG}.ops.dq_metrics
    )
    SELECT m.layer, m.table_name, m.check_name, m.metric_value,
           m.threshold, m.comparison, m.status, m.measured_at
    FROM {CATALOG}.ops.dq_metrics m, latest l
    WHERE m.measured_at = l.ts
""")

display(spark.table(f"{CATALOG}.ops.v_dq_latest").groupBy("status").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Journalisation

# COMMAND ----------


def log_run(catalog, task_name, source_name, started_at, status,
            rows_written=0, rows_rescued=0, files_processed=0, notes=None, run_id=None):
    row = [(run_id or str(uuid.uuid4()), task_name, source_name, started_at, datetime.now(),
            status, int(rows_written), int(rows_rescued), int(files_processed), notes)]
    schema = ("run_id string, task_name string, source_name string, started_at timestamp, "
              "ended_at timestamp, status string, rows_written bigint, rows_rescued bigint, "
              "files_processed bigint, notes string")
    spark.createDataFrame(row, schema).write.mode("append").saveAsTable(f"{catalog}.ops.pipeline_runs")


n_fail = spark.table(f"{CATALOG}.ops.dq_metrics").filter(
    (F.col("run_id") == RUN_ID) & (F.col("status") == "FAIL")).count()

log_run(CATALOG, "dq_checks", "novamarket.*", STARTED_AT,
        "FAILED" if n_fail else "SUCCESS", run_id=RUN_ID,
        notes=f"{n_fail} contrôle(s) en échec")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Tes réponses
# MAGIC
# MAGIC **1. Comment détecter une dérive que ce modèle ne voit pas ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **2. Avant l'écriture, après, ou les deux ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **3. Un `FAIL` doit-il arrêter le pipeline ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **4. Qu'envoies-tu à l'équipe source, et sous quelle forme ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **5. À quoi sert concrètement l'étiquette `pii` ?**
# MAGIC
# MAGIC > …
