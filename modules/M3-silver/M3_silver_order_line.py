# Databricks notebook source
# MAGIC %md
# MAGIC # M3.1 — Silver : lignes de commande
# MAGIC
# MAGIC `bronze.orders_raw` → `silver.order_line` + `ops.quarantine_order_line`
# MAGIC
# MAGIC **Invariant** : `count(silver) + count(quarantaine) = 284 333`

# COMMAND ----------

from pyspark.sql import functions as F, Window as W
from datetime import datetime
import uuid

CATALOG = "novamarket"
SOURCE = f"{CATALOG}.bronze.orders_raw"
TARGET = f"{CATALOG}.silver.order_line"
QUARANTINE = f"{CATALOG}.ops.quarantine_order_line"

VALID_STATUSES = ["DELIVERED", "SHIPPED", "PENDING", "CANCELLED", "RETURNED"]
NON_REVENUE_STATUSES = ["CANCELLED", "RETURNED"]
TS_FORMAT = "yyyy-MM-dd HH:mm:ss"

BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now()

bronze = spark.table(SOURCE)
print(f"bronze : {bronze.count():,} lignes".replace(",", " "))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Avant de coder : regarde le bruit
# MAGIC
# MAGIC Les valeurs de `unit_price` qui ne sont pas de simples nombres. Affiche-les avec
# MAGIC leurs **codes de caractères**, pas seulement leur rendu à l'écran : une partie du
# MAGIC bruit est invisible.

# COMMAND ----------

# TODO A — inventorie les formes prises par unit_price.
# Piste : filtre les valeurs qui ne matchent pas ^[0-9]+,[0-9]{2}$, puis regarde
# ce que renvoie ascii() ou hex() sur le premier caractère.


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Déduplication
# MAGIC
# MAGIC Sur `order_line_id`, sur toute la table. Une seule ligne par clé.
# MAGIC
# MAGIC Choisis un critère de survie **déterministe** et justifie-le : deux exécutions du
# MAGIC notebook doivent produire exactement la même table. `dropDuplicates` sur la seule
# MAGIC clé ne t'offre aucune garantie sur la ligne conservée.

# COMMAND ----------

# TODO B — déduplication

deduped = bronze

print(f"après déduplication : {deduped.count():,} lignes (attendu 284 333)".replace(",", " "))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Nettoyage
# MAGIC
# MAGIC Écris des expressions réutilisables : les mêmes règles serviront pour `unit_price`
# MAGIC et `discount_amount`, et tu y reviendras en M9.

# COMMAND ----------

# TODO C — expressions de nettoyage


def clean_decimal(col):
    """Chaîne polluée -> decimal(10,2), ou null si inexploitable."""
    ...


def clean_status(col):
    """Normalisation du statut."""
    ...


def parse_ts(col):
    """Horodatage au format contractuel strict, ou null."""
    ...


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validation
# MAGIC
# MAGIC Construis une colonne `quarantine_reasons` de type `array<string>`, en ne gardant
# MAGIC que les motifs réellement déclenchés. Une ligne dont le tableau est vide passe en
# MAGIC silver.
# MAGIC
# MAGIC Pense à `array_compact` ou `filter` pour éliminer les `null` du tableau.

# COMMAND ----------

# TODO D — colonne quarantine_reasons

validated = deduped

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quarantaine
# MAGIC
# MAGIC Colonnes source telles quelles + `_rescued_data` + `_source_file`
# MAGIC + `quarantine_reasons` + `quarantined_at`.

# COMMAND ----------

# TODO E — écriture de la quarantaine (overwrite : elle est recalculée à chaque passage)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver
# MAGIC
# MAGIC Typage, montants calculés, drapeaux d'orphelinat. Les référentiels dont tu as
# MAGIC besoin viennent de M1 et M2.

# COMMAND ----------

known_customers = spark.table(f"{CATALOG}.bronze.app_customers_raw").select("customer_id").distinct()
known_products = spark.table(f"{CATALOG}.bronze.ref_products_raw").select("product_id").distinct()

print(f"clients connus : {known_customers.count():,}".replace(",", " "))
print(f"produits connus : {known_products.count():,}".replace(",", " "))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Les adresses réparées en M1
# MAGIC
# MAGIC Silver est la couche où l'on remet en place les 1 087 adresses que le lecteur CSV
# MAGIC avait tronquées : `coalesce(shipping_address_full, shipping_address)`.
# MAGIC
# MAGIC Regarde d'abord ce que contient la table de réparation. Le compte de lignes et le
# MAGIC compte de clés distinctes ne sont pas les mêmes — comprends pourquoi **avant** de
# MAGIC joindre.

# COMMAND ----------

address_repair = spark.table(f"{CATALOG}.bronze.orders_address_repair")

print("lignes           :", address_repair.count())
print("clés distinctes  :", address_repair.select("order_line_id").distinct().count())

# COMMAND ----------

# TODO F — construction de silver.order_line
# Deux pièges de jointure dans cette seule cellule :
#   - le type de jointure pour détecter les orphelins : une jointure interne écarterait
#     justement les lignes que tu veux signaler ;
#   - la table de réparation, dont les clés ne sont PAS uniques. Une jointure gauche
#     garantit *au moins* une ligne par ligne de gauche, jamais exactement une.


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Contrôle

# COMMAND ----------

silver = spark.table(TARGET)
quarantine = spark.table(QUARANTINE)

n_silver, n_quarantine = silver.count(), quarantine.count()

checks = [
    ("lignes silver", n_silver, 282_104),
    ("lignes quarantaine", n_quarantine, 2_229),
    ("invariant silver + quarantaine", n_silver + n_quarantine, 284_333),
    ("order_line_id distincts", silver.select("order_line_id").distinct().count(), 282_104),
    ("commandes distinctes", silver.select("order_id").distinct().count(), 136_824),
    ("lignes de CA", silver.filter("is_revenue").count(), 234_272),
    ("clients orphelins", silver.filter("is_orphan_customer").count(), 1_721),
    ("produits orphelins", silver.filter("is_orphan_product").count(), 545),
]

for label, got, expected in checks:
    flag = "OK " if got == expected else "KO "
    print(f"{flag} {label:34s} {got:>10,} / {expected:>10,}".replace(",", " "))

# COMMAND ----------

display(
    quarantine.select(F.explode("quarantine_reasons").alias("motif"))
              .groupBy("motif").count().orderBy("motif")
)

# COMMAND ----------

display(
    silver.agg(
        F.sum("net_amount").alias("ca_total"),
        F.sum(F.when(F.col("is_revenue"), F.col("net_amount"))).alias("ca_net"),
        F.min("order_date").alias("premiere_commande"),
        F.max("order_date").alias("derniere_commande"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Journalisation

# COMMAND ----------


def log_run(catalog, task_name, source_name, started_at, status,
            rows_written=0, rows_rescued=0, files_processed=0, notes=None, run_id=None):
    row = [(run_id or str(uuid.uuid4()), task_name, source_name, started_at, datetime.now(),
            status, int(rows_written), int(rows_rescued), int(files_processed), notes)]
    schema = ("run_id string, task_name string, source_name string, started_at timestamp, "
              "ended_at timestamp, status string, rows_written bigint, rows_rescued bigint, "
              "files_processed bigint, notes string")
    spark.createDataFrame(row, schema).write.mode("append").saveAsTable(f"{catalog}.ops.pipeline_runs")


log_run(CATALOG, "silver_order_line", "bronze.orders_raw", STARTED_AT, "SUCCESS",
        rows_written=n_silver, rows_rescued=n_quarantine, run_id=BATCH_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Tes réponses
# MAGIC
# MAGIC **1. Les six lignes à double motif : qui sont-elles, et qu'est-ce qui a pu produire ça ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **2. Fallait-il implémenter `INVALID_PRICE` et `UNKNOWN_STATUS` puisqu'ils ne se déclenchent jamais ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **3. Quel processus de reprise pour les 2 229 lignes en quarantaine ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **4. À partir de quel seuil l'orphelinat devrait-il faire échouer le pipeline ?**
# MAGIC
# MAGIC > …
