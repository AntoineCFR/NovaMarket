# Databricks notebook source
# MAGIC %md
# MAGIC # Corrigé — M1.2 événements et M1.3 référentiels

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

CATALOG = "novamarket"
LANDING = f"/Volumes/{CATALOG}/landing/files"
BATCH_ID = str(uuid.uuid4())

# COMMAND ----------

# MAGIC %md
# MAGIC ## M1.2 — événements
# MAGIC
# MAGIC ### Pourquoi `inferColumnTypes=true` ici alors qu'on le refusait pour le CSV ?
# MAGIC
# MAGIC Parce que les deux options ne jouent pas le même rôle selon le format.
# MAGIC
# MAGIC En CSV, tout est du texte sur le disque : inférer un type, c'est **ajouter** une
# MAGIC interprétation que la source n'a jamais donnée — et donc risquer de perdre les
# MAGIC valeurs qui n'y rentrent pas.
# MAGIC
# MAGIC En JSON, les types sont **portés par le format lui-même** : `"qty": 3` est un entier
# MAGIC dans le document, `{"user": {...}}` est un objet. Refuser l'inférence reviendrait à
# MAGIC aplatir le document en une chaîne, donc à détruire de l'information qui existe
# MAGIC réellement dans la source. C'est l'inverse de l'objectif.
# MAGIC
# MAGIC La règle bronze n'est pas « tout en string ». C'est « ne rien ajouter, ne rien perdre ».

# COMMAND ----------

FLOW = "bronze_events"
SOURCE_PATH = f"{LANDING}/events"
TARGET = f"{CATALOG}.bronze.events_raw"
CHECKPOINT = f"/Volumes/{CATALOG}/ops/checkpoints/{FLOW}/checkpoint"
SCHEMA_LOCATION = f"/Volumes/{CATALOG}/ops/checkpoints/{FLOW}/schema"

raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("multiLine", "false")
    .option("rescuedDataColumn", "_rescued_data")
    .load(SOURCE_PATH)          # aucune option pour le gzip : détecté via l'extension
)

enriched = (
    raw
    .withColumn("_source_file", F.col("_metadata.file_name"))
    .withColumn("_source_file_modification_time", F.col("_metadata.file_modification_time"))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_ingest_batch_id", F.lit(BATCH_ID))
)

query = (
    enriched.writeStream
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TARGET)
)
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deux colonnes, deux mécanismes — à ne pas confondre
# MAGIC
# MAGIC C'est **la** distinction à retenir de ce notebook, et elle tombe à l'examen.
# MAGIC
# MAGIC | Colonne | Se remplit quand | La ligne est |
# MAGIC |---|---|---|
# MAGIC | `_corrupt_record` | l'enregistrement **n'a pas pu être analysé** | tous champs à `null` |
# MAGIC | `_rescued_data` | l'enregistrement est lisible mais **s'écarte du schéma** | par ailleurs complète |
# MAGIC
# MAGIC **1. JSON tronqué — 389 lignes.** L'enregistrement est coupé en plein milieu, comme
# MAGIC par un transfert interrompu. Le lecteur ne peut rien en tirer : il produit une ligne
# MAGIC à champs nuls et conserve le texte d'origine dans **`_corrupt_record`**, pas dans
# MAGIC `_rescued_data`.
# MAGIC
# MAGIC ```sql
# MAGIC SELECT count(*) FROM novamarket.bronze.events_raw WHERE event_id IS NULL   -- 389
# MAGIC ```
# MAGIC
# MAGIC C'est bien le contrat de bronze qui est tenu : **rien ne se perd**. La ligne existe,
# MAGIC son contenu est récupérable, et M3 pourra la mettre en quarantaine avec son texte.
# MAGIC
# MAGIC Compare avec les commandes : là, les champs en trop d'un CSV étaient purement et
# MAGIC simplement **jetés**. Le JSON s'en sort mieux, et pour une raison de fond — une ligne
# MAGIC JSON illisible est un échec *visible*, alors qu'un champ CSV excédentaire est un
# MAGIC surplus que le parseur croit pouvoir ignorer sans conséquence.
# MAGIC
# MAGIC **2. Conflits de type — `event_ts`.** Le champ est tantôt une chaîne ISO, tantôt un
# MAGIC entier epoch en millisecondes (1 269 occurrences). Si l'inférence retient `string` —
# MAGIC ce qui est le cas observé — les entiers sont convertis en chaînes de chiffres et
# MAGIC restent **dans la colonne**, sans rescue. M3 devra traiter les deux formes.
# MAGIC
# MAGIC Vérifie ce que tu as obtenu avant de passer à M3 :
# MAGIC
# MAGIC ```python
# MAGIC dict(spark.table(f"{CATALOG}.bronze.events_raw").dtypes)["event_ts"]
# MAGIC ```
# MAGIC
# MAGIC Note aussi que `items[].qty` et `items[].price` sont parfois sérialisés en chaîne.
# MAGIC À l'intérieur d'un tableau d'objets, ces conflits-là ne remontent pas forcément au
# MAGIC rescue : ils sont absorbés par le type inféré de l'élément. C'est du travail pour M3.

# COMMAND ----------

# MAGIC %md
# MAGIC ## M1.3 — référentiels
# MAGIC
# MAGIC ### Pourquoi pas Auto Loader
# MAGIC
# MAGIC Auto Loader répond à la question « quels fichiers n'ai-je pas encore lus ? ». Sur une
# MAGIC source **snapshot**, la bonne question est « quel est l'état courant ? ». Chaque
# MAGIC nouvelle livraison remplace la précédente ; l'empiler produirait 8 000 lignes de plus
# MAGIC à chaque fois, avec des `product_id` en doublon et aucun moyen simple de savoir
# MAGIC laquelle fait foi.
# MAGIC
# MAGIC Le motif adapté est une lecture batch + `overwrite`.
# MAGIC
# MAGIC ### L'inconvénient, et il est réel
# MAGIC
# MAGIC L'`overwrite` écrase l'état précédent : si un produit change de catégorie ou de prix
# MAGIC catalogue, l'ancienne valeur disparaît. On ne peut plus répondre à « dans quelle
# MAGIC catégorie ce produit était-il classé en février ? ».
# MAGIC
# MAGIC Deux réponses, qu'on verra en M4 : le Change Data Feed de Delta (qui garde la trace
# MAGIC des différences entre deux versions) et l'historisation SCD2 explicite en silver.
# MAGIC En attendant, `DESCRIBE HISTORY` et le *time travel* Delta permettent déjà de
# MAGIC remonter aux versions précédentes de la table bronze.

# COMMAND ----------

REF_PATH = f"{LANDING}/ref"
REFS = {"categories": "ref_categories_raw", "sellers": "ref_sellers_raw", "products": "ref_products_raw"}


def load_ref(name: str, target_table: str) -> int:
    src = f"{REF_PATH}/{name}.csv"

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", ",")
        .option("encoding", "UTF-8")
        .option("inferSchema", "false")          # tout en STRING, comme les autres tables bronze
        .option("rescuedDataColumn", "_rescued_data")
        .load(src)
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .withColumn("_source_file_modification_time", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingest_batch_id", F.lit(BATCH_ID))
    )

    (df.write
       .mode("overwrite")                        # snapshot : on remplace, on n'ajoute pas
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.bronze.{target_table}"))

    return spark.table(f"{CATALOG}.bronze.{target_table}").count()


for name, table in REFS.items():
    print(f"{table:24s} {load_ref(name, table):>7,} lignes".replace(",", " "))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vérifier que l'historique reste accessible malgré l'overwrite

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {CATALOG}.bronze.ref_products_raw"))
