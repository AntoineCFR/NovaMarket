# Databricks notebook source
# MAGIC %md
# MAGIC # M1.1 — Bronze : commandes
# MAGIC
# MAGIC Source : `/Volumes/novamarket/landing/files/orders/*.csv`
# MAGIC Cible : `novamarket.bronze.orders_raw`
# MAGIC
# MAGIC **Règle du module** : rien ne se perd. Toutes les colonnes source restent en `STRING`.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

CATALOG = "novamarket"
LANDING = f"/Volumes/{CATALOG}/landing/files"
SOURCE_PATH = f"{LANDING}/orders"
TARGET = f"{CATALOG}.bronze.orders_raw"

FLOW = "bronze_orders"
CHECKPOINT = f"/Volumes/{CATALOG}/ops/checkpoints/{FLOW}/checkpoint"
SCHEMA_LOCATION = f"/Volumes/{CATALOG}/ops/checkpoints/{FLOW}/schema"

# Second flux, pour la passe de réparation de la section 8. Un flux, un checkpoint :
# les mélanger rendrait les deux inrejouables indépendamment.
# Separateur qui n'apparait dans aucun fichier : toute la ligne tient en une colonne.
RAW_SEP = "\u0001"

FLOW_REPAIR = "bronze_orders_repair"
REPAIR_TARGET = f"{CATALOG}.bronze.orders_address_repair"
CHECKPOINT_REPAIR = f"/Volumes/{CATALOG}/ops/checkpoints/{FLOW_REPAIR}/checkpoint"
SCHEMA_LOCATION_REPAIR = f"/Volumes/{CATALOG}/ops/checkpoints/{FLOW_REPAIR}/schema"

BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now()

print(f"source     : {SOURCE_PATH}")
print(f"cible      : {TARGET}")
print(f"batch_id   : {BATCH_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Regarde le fichier avant de coder
# MAGIC
# MAGIC Aucune option de lecture ne s'invente. Elle se déduit du fichier.

# COMMAND ----------

import os

sample = sorted(os.listdir(SOURCE_PATH))[0]
print(f"fichier inspecté : {sample}\n")

with open(f"{SOURCE_PATH}/{sample}", "rb") as fh:
    head = fh.read(900)

print("--- octets bruts -----------------------------------------------------")
print(head[:260])
print("\n--- décodé en UTF-8 --------------------------------------------------")
print(head.decode("utf-8", errors="replace")[:260])
print("\n--- décodé en windows-1252 -------------------------------------------")
print(head.decode("cp1252", errors="replace")[:260])

# COMMAND ----------

# MAGIC %md
# MAGIC **Questions d'exploration** (réponds pour toi-même avant de continuer) :
# MAGIC
# MAGIC 1. Quel séparateur ? Quelle fin de ligne ? Quel encodage ?
# MAGIC 2. Comment sont écrits les nombres décimaux ?
# MAGIC 3. Compte le nombre de séparateurs par ligne sur les 2 000 premières lignes.
# MAGIC    Toutes les lignes en ont-elles le même nombre ? Si non, à quoi ressemblent
# MAGIC    les lignes déviantes ?
# MAGIC
# MAGIC Écris le code de la question 3 dans la cellule ci-dessous — c'est ce qui va
# MAGIC expliquer le contenu de `_rescued_data` plus loin.

# COMMAND ----------

# TODO A — distribution du nombre de séparateurs par ligne


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Le flux Auto Loader
# MAGIC
# MAGIC Options à déterminer : format, emplacement du schéma, typage des colonnes,
# MAGIC colonne de sauvetage, et les options CSV qui découlent de ton exploration.
# MAGIC
# MAGIC Deux pièges :
# MAGIC - `cloudFiles.inferColumnTypes` : par défaut, Auto Loader infère. Ici on veut
# MAGIC   **tout en `STRING`**. Quelle valeur donner ?
# MAGIC - `rescuedDataColumn` : c'est une option **du lecteur**, pas d'Auto Loader.
# MAGIC   Elle n'a pas de préfixe `cloudFiles.`.

# COMMAND ----------

# TODO B — construis le readStream Auto Loader

raw = (
    spark.readStream
    .format("cloudFiles")
    # ... options cloudFiles
    # ... options CSV (séparateur, en-tête, encodage)
    # ... colonne de sauvetage
    .load(SOURCE_PATH)
)

raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Les colonnes de métadonnées
# MAGIC
# MAGIC Spark expose une colonne cachée `_metadata` sur toute source fichier. Elle contient
# MAGIC notamment `file_name`, `file_path`, `file_size` et `file_modification_time`.
# MAGIC
# MAGIC C'est ce qui permet, six mois plus tard, de répondre à « d'où sort cette ligne ? ».

# COMMAND ----------

# TODO C — ajoute les 4 colonnes techniques attendues :
#   _source_file                     <- _metadata.file_name
#   _source_file_modification_time   <- _metadata.file_modification_time
#   _ingested_at                     <- horodatage d'écriture
#   _ingest_batch_id                 <- BATCH_ID

enriched = raw

enriched.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Écriture
# MAGIC
# MAGIC Sur serverless, le seul déclencheur disponible est `availableNow`. Il traite tout
# MAGIC ce qui est arrivé depuis le dernier checkpoint, puis s'arrête.
# MAGIC
# MAGIC `awaitTermination()` bloque jusqu'à la fin du micro-batch : sans lui, la cellule
# MAGIC suivante compterait les lignes d'avant.

# COMMAND ----------

# TODO D — écris le flux vers TARGET
# Indices : outputMode, checkpointLocation, trigger(availableNow=True), toTable(...)

query = (
    enriched.writeStream
    # ...
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Contrôle

# COMMAND ----------

df = spark.table(TARGET)

n_rows = df.count()
n_files = df.select("_source_file").distinct().count()
n_rescued = df.filter(F.col("_rescued_data").isNotNull()).count()

print(f"lignes                        : {n_rows:>10,}".replace(",", " "))
print(f"fichiers sources distincts    : {n_files:>10}")
print(f"lignes avec _rescued_data     : {n_rescued:>10,}".replace(",", " "))
print()
print("types des colonnes source :")
for name, dtype in df.dtypes:
    if not name.startswith("_"):
        print(f"    {name:32s} {dtype}")

# COMMAND ----------

display(
    df.groupBy("_source_file")
      .agg(F.count("*").alias("lignes"),
           F.sum(F.when(F.col("_rescued_data").isNotNull(), 1).otherwise(0)).alias("rescued"))
      .orderBy("_source_file")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Journalisation

# COMMAND ----------

def log_run(catalog, task_name, source_name, started_at, status,
            rows_written=0, rows_rescued=0, files_processed=0, notes=None, run_id=None):
    row = [(run_id or str(uuid.uuid4()), task_name, source_name, started_at, datetime.now(),
            status, int(rows_written), int(rows_rescued), int(files_processed), notes)]
    schema = ("run_id string, task_name string, source_name string, started_at timestamp, "
              "ended_at timestamp, status string, rows_written bigint, rows_rescued bigint, "
              "files_processed bigint, notes string")
    spark.createDataFrame(row, schema).write.mode("append").saveAsTable(f"{catalog}.ops.pipeline_runs")


log_run(CATALOG, FLOW, "orders_csv", STARTED_AT, "SUCCESS",
        rows_written=n_rows, rows_rescued=n_rescued, files_processed=n_files,
        notes=f"batch {BATCH_ID}", run_id=BATCH_ID)

display(spark.table(f"{CATALOG}.ops.pipeline_runs").orderBy(F.col("started_at").desc()).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Analyse des lignes sauvées — et de celles qui ne le sont pas
# MAGIC
# MAGIC Regarde le contenu réel de `_rescued_data` avant de répondre.

# COMMAND ----------

display(
    df.filter(F.col("_rescued_data").isNotNull())
      .select("order_line_id", "shipping_address", "_rescued_data", "_source_file")
      .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Environ 1 087 lignes du fichier portent un `;` non échappé dans `shipping_address`
# MAGIC et comptent donc **16 champs au lieu de 14**. Va les chercher.
# MAGIC
# MAGIC Une adresse saine s'écrit `<rue>, <code postal> <ville>` — elle contient une
# MAGIC virgule. Celles qui n'en ont plus sont exactement les lignes amputées.

# COMMAND ----------

display(
    df.filter(~F.col("shipping_address").contains(","))
      .select("order_line_id", "shipping_address", "_rescued_data", "_source_file")
      .limit(20)
)

print("lignes à l'adresse tronquée :",
      df.filter(~F.col("shipping_address").contains(",")).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tes réponses
# MAGIC
# MAGIC **1. Combien de lignes ont un `_rescued_data` non nul ? Explique le résultat, sachant
# MAGIC que toutes tes colonnes source sont en `STRING`.**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **2. Où sont passés les deux champs en trop des 1 087 lignes ? Le nombre de lignes
# MAGIC de la table a-t-il bougé ? Qu'est-ce qui, dans la table, permet encore de les repérer ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **3. Un `_rescued_data` nul garantit-il que la ligne est saine ? Contre-exemple tiré du fichier.**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **4. Après la vague W2, la table contient-elle des doublons ? Est-ce un problème à ce stade ?**
# MAGIC
# MAGIC > …

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Réparation : récupérer ce que le lecteur a jeté
# MAGIC
# MAGIC Constater la perte ne suffit pas. Bronze promet que rien ne se perd — **au niveau du
# MAGIC champ, pas seulement de la ligne.** Il faut donc aller rechercher les fragments
# MAGIC d'adresse dans les fichiers, puisque le lecteur CSV les a abandonnés.
# MAGIC
# MAGIC ### Pourquoi une passe séparée
# MAGIC
# MAGIC On ne touche pas au flux principal. Lui déclarer un schéma explicite avec des
# MAGIC colonnes de réserve désactiverait l'inférence — et la vague W3 fera justement
# MAGIC apparaître deux vraies colonnes dans l'en-tête. Les colonnes de réserve entreraient
# MAGIC en collision avec elles.
# MAGIC
# MAGIC C'est aussi le motif réaliste : quand un loader natif abîme une source qu'on ne
# MAGIC contrôle pas, on ne bricole pas le loader — on **réconcilie à côté**.
# MAGIC
# MAGIC ### La ruse de lecture
# MAGIC
# MAGIC Pour récupérer la ligne brute, lis le fichier en CSV avec un **séparateur qui
# MAGIC n'apparaît jamais** (`\u0001`) et sans en-tête : chaque ligne arrive entière dans une
# MAGIC colonne unique. Le format `text` ferait presque la même chose, mais il ne te laisse
# MAGIC pas déclarer l'encodage — et tes accents sont en `cp1252`.
# MAGIC
# MAGIC Deux détails qui te coûteront du temps si tu les oublies :
# MAGIC - la ligne d'en-tête arrive comme une ligne de données : filtre-la ;
# MAGIC - désactive la gestion des guillemets, sinon un `"` non apparié fusionnerait des lignes.
# MAGIC
# MAGIC ### Ce que tu dois produire
# MAGIC
# MAGIC `bronze.orders_address_repair`, une ligne par ligne mutilée :
# MAGIC
# MAGIC | Colonne | Type | Contenu |
# MAGIC |---|---|---|
# MAGIC | `order_line_id` | string | 2ᵉ champ de la ligne — intact, seul le dernier est amputé |
# MAGIC | `shipping_address_full` | string | l'adresse complète, reconstituée |
# MAGIC | `_source_file` | string | nom du fichier |
# MAGIC | `_repaired_at` | timestamp | horodatage de la passe |
# MAGIC
# MAGIC Environ **1 087 lignes** après W1 + W2.

# COMMAND ----------

# TODO E — lis les fichiers en ligne brute (séparateur \u0001, pas d'en-tête, cp1252)
# et garde les lignes qui comptent plus de 14 champs une fois découpées sur ";".

raw_lines = (
    spark.readStream
    .format("cloudFiles")
    # ... cloudFiles.format, schemaLocation, inferColumnTypes
    # ... sep = "\u0001", header = "false", encoding, quote désactivé
    .load(SOURCE_PATH)
)

# COMMAND ----------

# TODO F — reconstitue l'adresse et écris REPAIR_TARGET
#
# Indices :
#   F.split(col, ";")                 découpe la ligne
#   F.size(parts)                     nombre de champs
#   parts[1]                          order_line_id
#   F.slice(parts, 14, F.size(parts) - 13)   les fragments d'adresse, à partir du 14e
#   F.array_join(..., ";")            les recolle tels qu'ils étaient
#
# Pense à exclure la ligne d'en-tête, et à écrire en append avec CHECKPOINT_REPAIR.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contrôle de la réparation

# COMMAND ----------

rep = spark.table(REPAIR_TARGET)
print("lignes réparées :", rep.count())

display(
    spark.table(TARGET).alias("b")
    .join(rep.alias("r"), "order_line_id", "inner")
    .select("order_line_id",
            F.col("b.shipping_address").alias("tronquee"),
            F.col("r.shipping_address_full").alias("complete"))
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **5. La table de réparation compte-t-elle exactement autant de lignes que la table
# MAGIC bronze en compte de tronquées ? Si tu relances cette passe sans nouveau fichier,
# MAGIC que se passe-t-il — et pourquoi ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **6. On aurait pu, à la place, ne garder que la ligne brute en bronze et tout parser
# MAGIC en silver. Qu'est-ce que cette solution aurait coûté, au regard de ce que la vague
# MAGIC W3 fera au schéma ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **7. Deux tables bronze pour une source : est-ce ainsi qu'une équipe traiterait le
# MAGIC problème en production ?**
# MAGIC
# MAGIC > Prends le temps de celle-ci, elle vaut plus qu'un exercice de syntaxe. Cite au
# MAGIC > moins **trois** réponses possibles autres que celle qu'on vient d'implémenter, et
# MAGIC > pour chacune : dans quel contexte elle devient le bon choix, et ce qu'elle coûte.
# MAGIC >
# MAGIC > Puis tranche : laquelle retiendrais-tu si ce pipeline était le tien, et pourquoi
# MAGIC > celle-là ?
# MAGIC >
# MAGIC > Indice pour t'aiguiller sans te donner la réponse : le champ abîmé est le
# MAGIC > **dernier** de la ligne. Est-ce un hasard commode, ou est-ce ce qui rend une des
# MAGIC > options possible ? Et que se passerait-il si le `;` parasite tombait dans
# MAGIC > `payment_method`, au milieu ?
# MAGIC >
# MAGIC > Réponds **avant** d'ouvrir `FICHE-source-malformee.md`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Réinitialisation (à n'exécuter qu'en connaissance de cause)
# MAGIC
# MAGIC Le checkpoint mémorise les fichiers déjà traités. Supprimer la table ne suffit pas
# MAGIC à repartir de zéro : il faut supprimer le checkpoint **et** le schéma inféré.

# COMMAND ----------

# dbutils.fs.rm(f"/Volumes/{CATALOG}/ops/checkpoints/{FLOW}", True)
# spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
# print("flux réinitialisé")

# COMMAND ----------

# Le flux de réparation a son propre état : le réinitialiser est une décision distincte.
# dbutils.fs.rm(f"/Volumes/{CATALOG}/ops/checkpoints/{FLOW_REPAIR}", True)
# spark.sql(f"DROP TABLE IF EXISTS {REPAIR_TARGET}")
# print("flux de réparation réinitialisé")
