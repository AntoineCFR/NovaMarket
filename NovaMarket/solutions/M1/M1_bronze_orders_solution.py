# Databricks notebook source
# MAGIC %md
# MAGIC # Corrigé — M1.1 bronze commandes
# MAGIC
# MAGIC À ne consulter qu'après avoir fait passer le grader, ou après y avoir sérieusement passé du temps.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

CATALOG = "novamarket"
SOURCE_PATH = f"/Volumes/{CATALOG}/landing/files/orders"
TARGET = f"{CATALOG}.bronze.orders_raw"

FLOW = "bronze_orders"
CHECKPOINT = f"/Volumes/{CATALOG}/ops/checkpoints/{FLOW}/checkpoint"
SCHEMA_LOCATION = f"/Volumes/{CATALOG}/ops/checkpoints/{FLOW}/schema"

FLOW_REPAIR = "bronze_orders_repair"
REPAIR_TARGET = f"{CATALOG}.bronze.orders_address_repair"
CHECKPOINT_REPAIR = f"/Volumes/{CATALOG}/ops/checkpoints/{FLOW_REPAIR}/checkpoint"
SCHEMA_LOCATION_REPAIR = f"/Volumes/{CATALOG}/ops/checkpoints/{FLOW_REPAIR}/schema"

# Separateur qui n'apparait dans aucun fichier : toute la ligne tient en une colonne.
RAW_SEP = "\u0001"

BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO A — distribution du nombre de séparateurs par ligne

# COMMAND ----------

import collections, os

sample = sorted(os.listdir(SOURCE_PATH))[0]
counts = collections.Counter()
with open(f"{SOURCE_PATH}/{sample}", "r", encoding="cp1252") as fh:
    for i, line in enumerate(fh):
        if i > 2000:
            break
        counts[line.count(";")] += 1

print(dict(sorted(counts.items())))
# {13: 1993, 15: 8}  ->  8 lignes portent 2 séparateurs de trop.
# En-tête et lignes saines : 13 séparateurs = 14 colonnes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO B — le flux Auto Loader
# MAGIC
# MAGIC `cloudFiles.inferColumnTypes=false` : les **noms** de colonnes sont bien inférés
# MAGIC depuis l'en-tête, mais tous les types restent `STRING`. C'est ce qu'on veut en bronze.
# MAGIC
# MAGIC `rescuedDataColumn` n'a pas de préfixe `cloudFiles.` : c'est une option du lecteur
# MAGIC CSV/JSON sous-jacent, pas d'Auto Loader.

# COMMAND ----------

raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.inferColumnTypes", "false")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("header", "true")
    .option("sep", ";")
    .option("encoding", "windows-1252")
    .option("multiLine", "false")
    .option("rescuedDataColumn", "_rescued_data")
    .load(SOURCE_PATH)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO C — colonnes de métadonnées
# MAGIC
# MAGIC `_metadata` est une colonne cachée : elle n'apparaît pas dans `printSchema()` de la
# MAGIC source, mais elle est sélectionnable. `file_name` donne le nom seul, `file_path`
# MAGIC l'URI complète.

# COMMAND ----------

enriched = (
    raw
    .withColumn("_source_file", F.col("_metadata.file_name"))
    .withColumn("_source_file_modification_time", F.col("_metadata.file_modification_time"))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_ingest_batch_id", F.lit(BATCH_ID))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO D — écriture
# MAGIC
# MAGIC `mergeSchema` permet à la table d'accueillir les colonnes ajoutées par
# MAGIC `schemaEvolutionMode=addNewColumns` — utile dès la vague W3.

# COMMAND ----------

query = (
    enriched.writeStream
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TARGET)
)

query.awaitTermination()

print(f"lignes écrites sur ce batch : {query.lastProgress['numInputRows'] if query.lastProgress else 0}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO E — relire la ligne brute
# MAGIC
# MAGIC Le lecteur CSV a jeté les champs en trop. Pour les récupérer il faut la ligne
# MAGIC **entière**, donc empêcher le parseur de la découper : un séparateur qui n'apparaît
# MAGIC nulle part, et pas d'en-tête.
# MAGIC
# MAGIC Pourquoi pas `format("text")`, qui ferait la même chose plus simplement ? Parce
# MAGIC qu'il ne prend pas d'option d'encodage : les fichiers sont en `cp1252`, et toutes
# MAGIC les adresses accentuées reviendraient abîmées. Le lecteur CSV, lui, sait lire un
# MAGIC encodage — on le détourne pour ne produire qu'une colonne.

# COMMAND ----------

raw_lines = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION_REPAIR)
    .option("cloudFiles.inferColumnTypes", "false")
    .option("sep", RAW_SEP)
    .option("header", "false")
    .option("encoding", "windows-1252")
    .option("quote", "\u0000")      # sinon un " non apparié fusionnerait deux lignes
    .option("multiLine", "false")
    .load(SOURCE_PATH)
)

# La colonne unique s'appelle `_c0`.
line = F.col("_c0")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO F — reconstituer l'adresse
# MAGIC
# MAGIC `slice` est en base 1 : à partir du 14ᵉ champ, sur `size - 13` éléments. On les
# MAGIC recolle avec `;`, exactement comme ils étaient dans le fichier.

# COMMAND ----------

parts = F.split(line, ";")

repair = (
    raw_lines
    .withColumn("_parts", parts)
    .withColumn("_n", F.size(F.col("_parts")))
    # l'en-tête arrive comme une ligne de données : il commence par le nom de la 1re colonne
    .filter(~line.startswith("order_id;"))
    .filter(F.col("_n") > 14)
    .select(
        F.col("_parts")[1].alias("order_line_id"),
        F.array_join(
            F.slice(F.col("_parts"), 14, F.col("_n") - 13), ";"
        ).alias("shipping_address_full"),
        F.col("_metadata.file_name").alias("_source_file"),
        F.current_timestamp().alias("_repaired_at"),
    )
)

query_repair = (
    repair.writeStream
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_REPAIR)
    .trigger(availableNow=True)
    .toTable(REPAIR_TARGET)
)

query_repair.awaitTermination()

print("lignes réparées :", spark.table(REPAIR_TARGET).count())   # 1 087 après W1 + W2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Réponses aux questions d'analyse
# MAGIC
# MAGIC **1. Combien de lignes ont un `_rescued_data` non nul ?**
# MAGIC
# MAGIC > **Zéro.** Et c'est la bonne réponse, aussi contre-intuitive soit-elle.
# MAGIC >
# MAGIC > La colonne de sauvetage capture ce qui **ne correspond pas au schéma** : une valeur
# MAGIC > qui ne se laisse pas convertir dans le type déclaré, une colonne absente du schéma,
# MAGIC > une casse qui ne correspond pas. Or ici tu as déclaré les 14 colonnes en `STRING`.
# MAGIC > Toute suite d'octets est une chaîne valide : **aucun écart de type n'est possible**,
# MAGIC > donc rien à sauver.
# MAGIC >
# MAGIC > Le corollaire est important pour l'examen : `rescuedDataColumn` et
# MAGIC > `inferColumnTypes` se répondent. Sans inférence de type, la colonne de sauvetage
# MAGIC > n'a presque plus rien à faire sur un CSV. Elle reprend tout son sens sur du JSON,
# MAGIC > où une ligne peut être illisible indépendamment du typage — c'est ce que tu
# MAGIC > vérifieras sur `events_raw`.
# MAGIC
# MAGIC **2. Où sont passés les deux champs en trop des 1 087 lignes ?**
# MAGIC
# MAGIC > **Ils ont été jetés, sans le moindre signal.** Le lecteur CSV découpe 16 jetons,
# MAGIC > en mappe 14 sur le schéma, et abandonne les deux derniers. Il ne les sauve pas, il
# MAGIC > ne lève pas d'erreur, il n'incrémente aucun compteur.
# MAGIC >
# MAGIC > Le nombre de lignes, lui, **n'a pas bougé** : 287 785, exactement le nombre de
# MAGIC > lignes des fichiers. C'est ce qui rend la perte si difficile à voir — tous les
# MAGIC > contrôles de volumétrie passent au vert.
# MAGIC >
# MAGIC > Ce qui reste pour les repérer : `shipping_address` est le **dernier** champ, donc
# MAGIC > le seul mutilé. Une adresse saine vaut `"<rue>, <code postal> <ville>"` ; sur ces
# MAGIC > lignes elle vaut `"12 rue X"`, sans virgule ni code postal.
# MAGIC >
# MAGIC > ```python
# MAGIC > df.filter(~F.col("shipping_address").contains(",")).count()   # 1 087
# MAGIC > ```
# MAGIC >
# MAGIC > Retiens la leçon générale, elle vaut bien au-delà de ce fichier : **un compte de
# MAGIC > lignes juste ne prouve rien sur le contenu des lignes.** La fidélité de bronze est
# MAGIC > une propriété qu'il faut mesurer champ par champ, pas une garantie du format.
# MAGIC >
# MAGIC > Mais constater la perte ne suffit pas : bronze promet que rien ne se perd, et la
# MAGIC > promesse doit tenir **au niveau du champ**. C'est l'objet de la passe de
# MAGIC > réparation ci-dessus. Le compte de lignes tronquées n'est donc pas une métrique
# MAGIC > qu'on surveille en s'en accommodant — c'est le compte de lignes à récupérer.
# MAGIC
# MAGIC **3. Un `_rescued_data` nul garantit-il que la ligne est saine ?**
# MAGIC
# MAGIC > Non, et c'est le piège central du module. Comme **toutes** les colonnes sont
# MAGIC > déclarées `STRING`, aucune valeur ne peut provoquer d'échec de type. Exemples de
# MAGIC > lignes parfaitement « propres » du point de vue du rescue :
# MAGIC >
# MAGIC > - `order_ts = "2026-13-45 99:99:99"` ou `""` — une date impossible reste une chaîne valide
# MAGIC > - `unit_price = "74,48 €"` ou `"EUR 94,55"` — polluée par le symbole monétaire
# MAGIC > - `unit_price = " 88,73"` — précédée d'une **espace insécable** (U+00A0), invisible
# MAGIC > - `quantity = "-2"` — négative
# MAGIC > - `customer_id = "C999123"` — clé étrangère orpheline
# MAGIC >
# MAGIC > `_rescued_data` ne détecte que des écarts **au schéma déclaré**, jamais des
# MAGIC > incohérences sémantiques — et ici, où tout est `STRING`, il n'y a pas même d'écart
# MAGIC > de schéma possible. C'est exactement pour ça que la couche silver a besoin de sa
# MAGIC > propre table de quarantaine (M3) : les deux mécanismes sont complémentaires, et
# MAGIC > sur cette source, seul le second travaille réellement.
# MAGIC
# MAGIC **4. Doublons après W2 : problème ou pas ?**
# MAGIC
# MAGIC > Oui, il y a des doublons : 3 394 lignes strictement identiques rejouées à
# MAGIC > l'intérieur des fichiers, plus 58 lignes du 1ᵉʳ juin re-émises dans le fichier du
# MAGIC > 2 juin. Vérifiable par `count(*) - count(distinct order_line_id)`.
# MAGIC >
# MAGIC > Ce n'est **pas** un problème en bronze, c'en serait un de les supprimer ici. La
# MAGIC > couche bronze doit rester une image fidèle de ce que la source a réellement émis :
# MAGIC > si demain on découvre que le rejeu du 2 juin était volontaire et porteur de
# MAGIC > corrections, il faut pouvoir le prouver. Dédupliquer, c'est arbitrer — donc c'est
# MAGIC > du métier, donc silver.
# MAGIC >
# MAGIC > Corollaire pratique : `order_line_id` est déclaré « clé unique » dans le contrat
# MAGIC > d'interface (`docs/02-sources-et-modele.md`). Il ne l'est pas. Première entrée
# MAGIC > pour ton registre d'écarts au contrat.
# MAGIC
# MAGIC **5. La table de réparation a-t-elle le bon compte ? Et si on relance à vide ?**
# MAGIC
# MAGIC > 1 087 lignes, exactement le nombre de lignes tronquées dans `bronze.orders_raw`.
# MAGIC > Les deux comptes doivent coïncider — s'ils divergent, c'est que le filtre de la
# MAGIC > passe de réparation et le détecteur de troncature ne parlent pas de la même chose,
# MAGIC > et l'un des deux est faux.
# MAGIC >
# MAGIC > Relancée sans nouveau fichier, la passe **n'ajoute rien** : elle a son propre
# MAGIC > checkpoint, qui a mémorisé les huit fichiers déjà lus. C'est la raison pour
# MAGIC > laquelle on lui en a donné un séparé plutôt que de partager celui du flux
# MAGIC > principal — les deux doivent pouvoir être rejoués indépendamment.
# MAGIC >
# MAGIC > Attention à la nuance : le flux est idempotent **par fichier**, pas par ligne. Si
# MAGIC > tu supprimais le checkpoint de réparation sans vider la table, tu doublerais les
# MAGIC > 1 087 lignes. C'est pourquoi les deux commandes de réinitialisation vont par paire.
# MAGIC
# MAGIC **6. Pourquoi ne pas simplement garder la ligne brute en bronze ?**
# MAGIC
# MAGIC > C'est la solution la plus fidèle qui soit, et elle est défendable : bronze ne
# MAGIC > contiendrait qu'une colonne de texte, et silver parserait tout. Rien ne pourrait
# MAGIC > plus se perdre à l'ingestion, par construction.
# MAGIC >
# MAGIC > Ce qu'elle coûte, c'est **tout ce qu'Auto Loader apporte au-dessus du texte**. En
# MAGIC > particulier l'évolution de schéma : la vague W3 ajoute `promo_code` et `channel` à
# MAGIC > l'en-tête, et c'est `cloudFiles.schemaEvolutionMode` qui doit les faire apparaître
# MAGIC > dans la table — un mécanisme qu'on ne peut ni observer ni apprendre sur une colonne
# MAGIC > de texte unique. On perdrait aussi la colonne de sauvetage, le typage, et la
# MAGIC > lisibilité de la table pour quiconque l'ouvre.
# MAGIC >
# MAGIC > D'où l'arbitrage retenu : **le flux principal reste déclaratif et typé, la passe de
# MAGIC > réparation traite l'exception.** Une exception mesurée à 0,38 % des lignes ne
# MAGIC > justifie pas de dégrader les 99,62 % restantes.
# MAGIC
# MAGIC **7. Deux tables bronze : est-ce ce qu'une équipe ferait en production ?**
# MAGIC
# MAGIC > **Non — c'est la quatrième option sur quatre.** Traitée en détail dans
# MAGIC > `modules/M1-bronze/FICHE-source-malformee.md`. En résumé :
# MAGIC >
# MAGIC > 1. **Faire corriger la source.** Toujours, et en premier. Ce n'est pas un cas
# MAGIC >    limite : toute bibliothèque CSV échappe un champ contenant le séparateur. Mais
# MAGIC >    le délai ne t'appartient pas, et l'historique déjà livré reste cassé.
# MAGIC > 2. **Parser correctement du premier coup.** `F.split(ligne, ";", 14)` : au plus 14
# MAGIC >    éléments, le dernier absorbe le reste. Une passe, une table, zéro perte. C'est
# MAGIC >    la meilleure réponse technique **parce que le champ abîmé est le dernier** — si
# MAGIC >    le `;` parasite tombait dans `payment_method`, l'option disparaîtrait.
# MAGIC > 3. **Normaliser avant d'ingérer.** Le fichier reçu reste intact pour l'audit, on
# MAGIC >    travaille sur une copie assainie. Coûte une copie complète et un saut de plus.
# MAGIC > 4. **Table de réparation à côté.** Motif de *remédiation* : le pipeline tourne
# MAGIC >    déjà, on découvre les dégâts sur l'historique, on répare à côté et on ouvre un
# MAGIC >    ticket pour retirer la rustine.
# MAGIC >
# MAGIC > **Pourquoi la 4 ici** : W3 doit faire jouer `cloudFiles.schemaEvolutionMode`, un
# MAGIC > objectif d'examen que l'option 2 supprimerait du programme. C'est un arbitrage
# MAGIC > pédagogique assumé. En production, sur cette source, ce serait **2 doublée de 1**.
# MAGIC >
# MAGIC > **Ce qui distingue une bonne réponse d'une réponse récitée** : avoir vu que la
# MAGIC > position du champ abîmé — dernier ou milieu — décide à elle seule de ce qui est
# MAGIC > possible. Et avoir nommé la fragilité du motif retenu : une réconciliation exige
# MAGIC > une clé fiable, or `order_line_id` ne l'est pas.
