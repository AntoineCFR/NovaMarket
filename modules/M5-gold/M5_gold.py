# Databricks notebook source
# MAGIC %md
# MAGIC # M5 — Couche gold : modèle en étoile
# MAGIC
# MAGIC `silver.*` → `gold.dim_*`, `gold.fact_order_line`, `gold.agg_*`, `gold.v_*`
# MAGIC
# MAGIC Objectif : un analyste répond aux six questions métier sans toi, et sans pouvoir
# MAGIC se tromper sur la commission.

# COMMAND ----------

from pyspark.sql import functions as F, Window as W
from datetime import datetime
import uuid

CATALOG = "novamarket"
BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now()

CALENDAR_START, CALENDAR_END = "2025-12-01", "2026-06-30"
LOOKBACK_DAYS = 90

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `dim_date`
# MAGIC
# MAGIC 212 jours. `sequence` + `explode` évite la boucle Python et reste lisible.

# COMMAND ----------

# TODO A — dim_date


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `dim_customer` — SCD1
# MAGIC
# MAGIC État courant uniquement. Question à te poser avant d'écrire le filtre : que fait-on
# MAGIC des 35 clients marqués supprimés ? Les exclure de la dimension casserait les faits
# MAGIC qui les référencent.

# COMMAND ----------

# TODO B — dim_customer


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `dim_seller` — SCD2 avec clé de version
# MAGIC
# MAGIC Toutes les versions, plus `seller_sk` en première colonne :
# MAGIC
# MAGIC ```
# MAGIC seller_sk = concat(seller_id, '#', date_format(valid_from, 'yyyyMMddHHmmss'))
# MAGIC ```
# MAGIC
# MAGIC Déterministe et dérivée du contenu : elle survit à une reconstruction de la
# MAGIC dimension, contrairement à un identifiant auto-incrémenté.

# COMMAND ----------

# TODO C — dim_seller


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `dim_product` et `ref_commission_plan`

# COMMAND ----------

# TODO D — dim_product (jointure produits × catégories) et ref_commission_plan


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `fact_order_line`
# MAGIC
# MAGIC Le cœur du module. Deux points de vigilance :
# MAGIC
# MAGIC - **La jointure temporelle.** Convention `[valid_from, valid_to)`, et surtout
# MAGIC   n'oublie pas la branche `valid_to IS NULL` : sans elle, tu perds toutes les lignes
# MAGIC   rattachées à une version courante — c'est-à-dire presque toutes.
# MAGIC - **`commission_amount` vaut `0.00` hors CA**, jamais `null`. Une commande annulée
# MAGIC   ne génère pas de commission ; ce n'est pas une commission inconnue.
# MAGIC
# MAGIC Vérifie le nombre de lignes après la jointure **avant** d'écrire la table. Un fait
# MAGIC qui perd des lignes dans une jointure de dimension est le bug le plus fréquent et
# MAGIC le plus silencieux de la modélisation dimensionnelle.

# COMMAND ----------

# TODO E — fact_order_line

fact = None

print(f"silver.order_line : {spark.table(f'{CATALOG}.silver.order_line').count():>7}")
# print(f"fact_order_line   : {fact.count():>7}   (les deux doivent être égaux : 282 104)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Le test qui compte
# MAGIC
# MAGIC Attendu : **3 164 343,53** et **3 118 577,56**. Si les deux colonnes sont égales,
# MAGIC ta clé de version ne résout pas la bonne ligne de `dim_seller`.

# COMMAND ----------

spark.sql(f"""
    SELECT
        round(sum(f.commission_amount), 2)              AS commission_historisee,
        round(sum(f.net_amount * c.commission_rate), 2) AS commission_plan_courant
    FROM {CATALOG}.gold.fact_order_line f
    JOIN {CATALOG}.gold.dim_seller d
        ON f.seller_id = d.seller_id AND d.is_current
    JOIN {CATALOG}.gold.ref_commission_plan c
        ON d.plan_code = c.plan_code
    WHERE f.is_revenue
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `agg_revenue_monthly` — question 1
# MAGIC
# MAGIC Lignes de CA uniquement. Produits orphelins regroupés sous `'UNKNOWN'`.
# MAGIC Attendu : **13 333** lignes.

# COMMAND ----------

# TODO F — agg_revenue_monthly


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. `agg_funnel_source` — question 5
# MAGIC
# MAGIC Une session compte dans une étape si elle contient **au moins un** événement de ce
# MAGIC type. Attendu : 7 lignes, 31 867 sessions au total, dont 1 873 avec achat.

# COMMAND ----------

# TODO G — agg_funnel_source


# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. `v_top_products_90d` — question 6
# MAGIC
# MAGIC Fenêtre : strictement après `max(order_date) - 90 jours`, soit après le 2026-03-04.
# MAGIC Calcule la borne, ne la code pas en dur — la vue doit rester juste quand de
# MAGIC nouvelles données arrivent.

# COMMAND ----------

# TODO H — v_top_products_90d


# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Les trois vues des questions 2, 3 et 4
# MAGIC
# MAGIC | Vue | Question |
# MAGIC |---|---|
# MAGIC | `v_seller_quality_monthly` | Taux d'annulation et de retour par vendeur et par mois |
# MAGIC | `v_basket_by_segment` | Panier moyen et nombre de commandes par segment et par pays |
# MAGIC | `v_customer_cohort` | Rétention par mois de première commande |
# MAGIC
# MAGIC Attention au grain sur le panier moyen : « moyenne des `net_amount` » et « moyenne
# MAGIC par commande » ne sont pas la même chose, et l'écart est d'un facteur 2 environ.

# COMMAND ----------

# TODO I — les trois vues


# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Documentation
# MAGIC
# MAGIC Une table gold non documentée n'est pas business-ready. Le commentaire de
# MAGIC `seller_sk` est le plus important du projet : c'est lui qui évite l'erreur à
# MAGIC 45 765,97 €.

# COMMAND ----------

# TODO J — commentaires Unity Catalog sur fact_order_line et ses colonnes clés
# Rappel : COMMENT ON TABLE ... IS '...'  /  ALTER TABLE ... ALTER COLUMN ... COMMENT '...'


# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.gold.fact_order_line"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Journalisation

# COMMAND ----------


def log_run(catalog, task_name, source_name, started_at, status,
            rows_written=0, rows_rescued=0, files_processed=0, notes=None, run_id=None):
    row = [(run_id or str(uuid.uuid4()), task_name, source_name, started_at, datetime.now(),
            status, int(rows_written), int(rows_rescued), int(files_processed), notes)]
    schema = ("run_id string, task_name string, source_name string, started_at timestamp, "
              "ended_at timestamp, status string, rows_written bigint, rows_rescued bigint, "
              "files_processed bigint, notes string")
    spark.createDataFrame(row, schema).write.mode("append").saveAsTable(f"{catalog}.ops.pipeline_runs")


log_run(CATALOG, "gold_star_schema", "silver.*", STARTED_AT, "SUCCESS",
        rows_written=spark.table(f"{CATALOG}.gold.fact_order_line").count(), run_id=BATCH_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Tes réponses
# MAGIC
# MAGIC **1. Pourquoi `dim_customer` en SCD1 et `dim_seller` en SCD2 ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **2. Comment gérer proprement les 1 721 lignes à client orphelin ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **3. `agg_revenue_monthly` vaut-il son coût de maintenance ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **4. Que se passe-t-il le jour où NovaMarket change ses taux de commission ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **5. Vue ou table : sur quel critère as-tu tranché ?**
# MAGIC
# MAGIC > …
