# Databricks notebook source
# MAGIC %md
# MAGIC # M4 — Historisation SCD2
# MAGIC
# MAGIC `bronze.app_*_raw` (journal) → `silver.*_scd2`
# MAGIC
# MAGIC Le notebook couvre les trois temps du module : reconstruction complète,
# MAGIC application incrémentale par `MERGE`, puis vérification croisée.

# COMMAND ----------

from pyspark.sql import functions as F, Window as W
from datetime import datetime
import uuid

CATALOG = "novamarket"
BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now()

CUSTOMER_TRACKED = ["first_name", "last_name", "email", "country", "city", "zip_code",
                    "segment", "is_opt_in", "is_deleted"]
SELLER_TRACKED = ["seller_name", "seller_country", "seller_city", "main_top_category",
                  "plan_code", "is_active"]

CUSTOMER_CARRIED = ["created_at"]
SELLER_CARRIED = ["onboarded_at"]

# COMMAND ----------

for name in ["app_customers_raw", "app_sellers_raw"]:
    df = spark.table(f"{CATALOG}.bronze.{name}")
    print(f"{name:20s} {df.count():>7} ligne(s) de journal")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. L'empreinte des attributs suivis
# MAGIC
# MAGIC Comparer neuf colonnes deux à deux est illisible, et faux dès qu'une valeur est
# MAGIC nulle (`null != null` en SQL). Une empreinte règle les deux problèmes — à condition
# MAGIC de neutraliser les `null` **avant** de hacher, sinon deux versions différentes
# MAGIC peuvent produire la même chaîne concaténée.

# COMMAND ----------

# TODO A — fonction d'empreinte
# Piste : sha2 sur une concaténation. Réfléchis au séparateur : que se passe-t-il si
# une valeur contient le caractère que tu as choisi ?


def scd_hash(cols):
    """Empreinte stable des attributs suivis."""
    ...


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Reconstruction complète
# MAGIC
# MAGIC Trois opérations enchaînées :
# MAGIC
# MAGIC 1. trier les versions d'une clé — `updated_at` **puis** `_extracted_at` ;
# MAGIC 2. fusionner deux versions consécutives de même empreinte ;
# MAGIC 3. calculer `valid_to` depuis la version suivante.
# MAGIC
# MAGIC Les étapes 2 et 3 se font toutes les deux avec une fonction de fenêtrage. `lag`
# MAGIC pour comparer à la précédente, `lead` pour aller chercher la suivante.

# COMMAND ----------

# TODO B — reconstruction complète


def rebuild_scd2(journal, key, tracked, carried):
    """Construit la table SCD2 complète depuis le journal bronze."""
    ...


# COMMAND ----------

seller_scd2 = rebuild_scd2(
    spark.table(f"{CATALOG}.bronze.app_sellers_raw"), "seller_id", SELLER_TRACKED, SELLER_CARRIED)
customer_scd2 = rebuild_scd2(
    spark.table(f"{CATALOG}.bronze.app_customers_raw"), "customer_id", CUSTOMER_TRACKED, CUSTOMER_CARRIED)

(seller_scd2.write.mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(f"{CATALOG}.silver.seller_scd2"))
(customer_scd2.write.mode("overwrite").option("overwriteSchema", "true")
              .saveAsTable(f"{CATALOG}.silver.customer_scd2"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contrôle après les deux premières extractions
# MAGIC
# MAGIC Attendu : 640 / 600 pour les vendeurs, 25 390 / 25 060 pour les clients.
# MAGIC Si tu vois 641 et 25 411, tes versions identiques ne sont pas fusionnées.

# COMMAND ----------


def report(table, key):
    df = spark.table(f"{CATALOG}.silver.{table}")
    print(f"{table:18s} {df.count():>7} ligne(s)  "
          f"{df.filter('is_current').count():>7} courante(s)  "
          f"{df.select(key).distinct().count():>7} clé(s)")


report("seller_scd2", "seller_id")
report("customer_scd2", "customer_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Contrôles d'intégrité temporelle
# MAGIC
# MAGIC Ces contrôles ne dépendent d'aucun chiffre du jeu de données. Écris-les une fois,
# MAGIC réutilise-les partout.

# COMMAND ----------

# TODO C — écris les cinq contrôles :
#   1. exactement une version courante par clé
#   2. aucune version courante avec valid_to renseigné
#   3. aucune version fermée avec valid_to <= valid_from
#   4. chaînage : valid_to d'une version = valid_from de la suivante
#   5. aucune empreinte identique sur deux versions consécutives


def integrity_checks(table, key):
    """Renvoie un dict {nom_du_controle: nombre_d_anomalies}."""
    ...


for table, key in [("seller_scd2", "seller_id"), ("customer_scd2", "customer_id")]:
    print(table)
    for name, n in integrity_checks(table, key).items():
        print(f"    {'OK ' if n == 0 else 'KO '} {name:44s} {n}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Nouvelle journée d'activité
# MAGIC
# MAGIC **Arrête-toi ici.** Va appliquer la vague D2 (voir l'étape 2 du README), relance
# MAGIC le notebook de M2 pour la troisième extraction, puis reviens.
# MAGIC
# MAGIC Le journal doit être passé à 25 971 lignes clients et 706 lignes vendeurs.

# COMMAND ----------

for name in ["app_customers_raw", "app_sellers_raw"]:
    print(f"{name:20s} {spark.table(f'{CATALOG}.bronze.{name}').count():>7}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Application du delta par `MERGE`
# MAGIC
# MAGIC Sans reconstruire. Le motif en deux temps :
# MAGIC
# MAGIC - on identifie les clés dont l'empreinte courante diffère de la nouvelle ;
# MAGIC - pour chacune, il faut **fermer** l'ancienne ligne *et* **insérer** la nouvelle.
# MAGIC
# MAGIC Un seul `MERGE` ne sait pas faire les deux sur la même correspondance. L'astuce
# MAGIC classique : construire une source où chaque changement apparaît **deux fois** — une
# MAGIC fois avec la clé de correspondance renseignée (qui déclenchera le `UPDATE`), une
# MAGIC fois avec cette clé à `null` (qui ne correspondra à rien et déclenchera l'`INSERT`).

# COMMAND ----------

# TODO D — construis le delta : les nouvelles versions absentes du SCD2 courant


def compute_delta(journal, scd2_table, key, tracked):
    """Nouvelles versions à appliquer : celles postérieures à la version courante
    et dont l'empreinte diffère."""
    ...


# COMMAND ----------

# TODO E — le MERGE SCD2 en deux temps


def merge_scd2(delta, scd2_table, key, tracked, carried):
    ...


# COMMAND ----------

merge_scd2(compute_delta(spark.table(f"{CATALOG}.bronze.app_sellers_raw"),
                         f"{CATALOG}.silver.seller_scd2", "seller_id", SELLER_TRACKED),
           f"{CATALOG}.silver.seller_scd2", "seller_id", SELLER_TRACKED, SELLER_CARRIED)

merge_scd2(compute_delta(spark.table(f"{CATALOG}.bronze.app_customers_raw"),
                         f"{CATALOG}.silver.customer_scd2", "customer_id", CUSTOMER_TRACKED),
           f"{CATALOG}.silver.customer_scd2", "customer_id", CUSTOMER_TRACKED, CUSTOMER_CARRIED)

report("seller_scd2", "seller_id")
report("customer_scd2", "customer_id")

# COMMAND ----------

# MAGIC %md
# MAGIC Attendu : 665 / 600 et 25 570 / 25 080.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Vérification croisée
# MAGIC
# MAGIC Le test qui prouve qu'un pipeline incrémental est correct : reconstruire tout, et
# MAGIC comparer. `exceptAll` dans les deux sens, sur les colonnes métier — pas sur
# MAGIC `_processed_at`, qui diffère forcément.

# COMMAND ----------

# TODO F — comparaison MERGE vs reconstruction complète


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Change Data Feed
# MAGIC
# MAGIC Le CDF enregistre, version après version, ce que chaque écriture a réellement fait.
# MAGIC Sur un `MERGE` SCD2, il montre les fermetures (`update_preimage` /
# MAGIC `update_postimage`) et les insertions (`insert`).
# MAGIC
# MAGIC Attention : le CDF n'enregistre que ce qui se passe **après** son activation.

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {CATALOG}.silver.seller_scd2
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

display(spark.sql(f"DESCRIBE HISTORY {CATALOG}.silver.seller_scd2").limit(10))

# COMMAND ----------

# TODO G — alimente ops.scd2_change_log depuis table_changes()
# Signature : table_changes('table', version_de_depart)
# Colonnes minimales attendues : seller_id, _change_type, _commit_version, _commit_timestamp


# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Le point de tout ça : la commission historisée
# MAGIC
# MAGIC Aperçu de ce que M5 va exploiter. Les deux chiffres ci-dessous doivent différer.

# COMMAND ----------

rates = spark.createDataFrame(
    [("BASIC", 0.150), ("PLUS", 0.115), ("PREMIUM", 0.085)],
    "plan_code string, commission_rate double")
rates.createOrReplaceTempView("plan_rates")

spark.sql(f"""
    SELECT
        round(sum(o.net_amount * r_hist.commission_rate), 2) AS commission_historisee,
        round(sum(o.net_amount * r_cur.commission_rate), 2)  AS commission_plan_courant
    FROM {CATALOG}.silver.order_line o
    JOIN {CATALOG}.silver.seller_scd2 s_hist
        ON  o.seller_id = s_hist.seller_id
        AND o.order_ts >= s_hist.valid_from
        AND (s_hist.valid_to IS NULL OR o.order_ts < s_hist.valid_to)
    JOIN {CATALOG}.silver.seller_scd2 s_cur
        ON  o.seller_id = s_cur.seller_id AND s_cur.is_current
    JOIN plan_rates r_hist ON s_hist.plan_code = r_hist.plan_code
    JOIN plan_rates r_cur  ON s_cur.plan_code  = r_cur.plan_code
    WHERE o.is_revenue
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Journalisation

# COMMAND ----------


def log_run(catalog, task_name, source_name, started_at, status,
            rows_written=0, rows_rescued=0, files_processed=0, notes=None, run_id=None):
    row = [(run_id or str(uuid.uuid4()), task_name, source_name, started_at, datetime.now(),
            status, int(rows_written), int(rows_rescued), int(files_processed), notes)]
    schema = ("run_id string, task_name string, source_name string, started_at timestamp, "
              "ended_at timestamp, status string, rows_written bigint, rows_rescued bigint, "
              "files_processed bigint, notes string")
    spark.createDataFrame(row, schema).write.mode("append").saveAsTable(f"{catalog}.ops.pipeline_runs")


log_run(CATALOG, "silver_scd2", "app_customers+app_sellers", STARTED_AT, "SUCCESS",
        rows_written=spark.table(f"{CATALOG}.silver.customer_scd2").count()
                     + spark.table(f"{CATALOG}.silver.seller_scd2").count(),
        run_id=BATCH_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Tes réponses
# MAGIC
# MAGIC **1. `valid_to` à `null` ou à `9999-12-31` ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **2. D'où viennent les 401 et 41 versions fusionnées ? Pouvait-on les éviter ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **3. Les trois clients à trois versions : leur histoire.**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **4. Ton `MERGE` est-il rejouable ? Démontre-le.**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **5. Que devient un vendeur supprimé de la source ?**
# MAGIC
# MAGIC > …
