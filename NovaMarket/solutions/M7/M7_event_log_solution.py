# Databricks notebook source
# MAGIC %md
# MAGIC # Corrigé — M7 : journal d'événements et bilan

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, LongType

CATALOG = "novamarket"
LDP_SCHEMA = f"{CATALOG}.ldp"

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO A — extraction des métriques d'attentes
# MAGIC
# MAGIC Trois pièges dans cette extraction :
# MAGIC
# MAGIC - `expectations` est un **tableau** d'objets, pas un objet : il faut l'exploser ;
# MAGIC - le journal contient **toutes** les exécutions ; sommer sans filtrer donne des
# MAGIC   compteurs multipliés par le nombre de mises à jour ;
# MAGIC - une même mise à jour émet plusieurs événements `flow_progress` ; ce sont les
# MAGIC   compteurs du **dernier** qui font foi.

# COMMAND ----------

events = spark.sql(f"SELECT * FROM event_log(TABLE({LDP_SCHEMA}.order_line_silver))")
events.createOrReplaceTempView("ldp_events")

EXPECTATION_SCHEMA = ArrayType(StructType([
    StructField("name", StringType()),
    StructField("dataset", StringType()),
    StructField("passed_records", LongType()),
    StructField("failed_records", LongType()),
]))

# COMMAND ----------

raw = (
    events
    .filter(F.col("event_type") == "flow_progress")
    .withColumn("_exp_json",
                F.get_json_object("details", "$.flow_progress.data_quality.expectations"))
    .filter(F.col("_exp_json").isNotNull())
)

# La derniere mise a jour uniquement.
last_update = raw.agg(F.max("origin.update_id")).first()[0]
last_ts = (raw.filter(F.col("origin.update_id") == last_update)
              .agg(F.max("timestamp")).first()[0])

expectations = (
    raw
    .filter((F.col("origin.update_id") == last_update) & (F.col("timestamp") == last_ts))
    .withColumn("_exp", F.explode(F.from_json("_exp_json", EXPECTATION_SCHEMA)))
    .select(
        F.col("_exp.name").alias("expectation_name"),
        F.coalesce(F.col("_exp.dataset"), F.col("origin.flow_name")).alias("dataset"),
        F.col("_exp.passed_records").alias("passed_records"),
        F.col("_exp.failed_records").alias("failed_records"),
        F.col("origin.update_id").alias("update_id"),
        F.current_timestamp().alias("extracted_at"),
    )
)

(expectations.write.mode("append").option("mergeSchema", "true")
             .saveAsTable(f"{CATALOG}.ops.ldp_expectations"))

display(spark.table(f"{CATALOG}.ops.ldp_expectations").orderBy(F.col("extracted_at").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC > Si `event_log()` n'est pas disponible sur ton workspace, publie le journal vers une
# MAGIC > table Unity Catalog depuis les paramètres du pipeline et remplace la première
# MAGIC > cellule par un `spark.table(...)`. Le reste du code est identique : la structure
# MAGIC > des événements ne change pas.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Réponses
# MAGIC
# MAGIC **1. Ce que je n'ai pas eu à écrire**
# MAGIC
# MAGIC > - La gestion des checkpoints et de l'emplacement de schéma d'Auto Loader.
# MAGIC > - Le `trigger(availableNow=True)` et le `awaitTermination()`.
# MAGIC > - L'ordre d'exécution : le moteur déduit le graphe de `dlt.read()`. Aucune
# MAGIC >   dépendance à déclarer, aucun DAG à maintenir quand j'ajoute un dataset.
# MAGIC > - La création des tables et la gestion des schémas (`overwriteSchema`, `mergeSchema`).
# MAGIC > - **Toute l'instrumentation de M6** pour ce périmètre : lignes lues, lignes
# MAGIC >   écrites, lignes rejetées par règle, durée par flux, tout est publié sans une
# MAGIC >   ligne de code. M6 m'a pris trois heures ; ici c'est gratuit.
# MAGIC > - L'idempotence : relancer le pipeline ne duplique rien, sans que j'aie eu à y
# MAGIC >   penser.
# MAGIC >
# MAGIC > Comptabilisé en lignes de code : environ 120 pour le pipeline contre 400 pour
# MAGIC > l'équivalent M1 + M3 + la part de M6 qui le concerne.
# MAGIC
# MAGIC **2. Ce que j'ai perdu**
# MAGIC
# MAGIC > **Le principal : la quarantaine n'est pas un concept du modèle.** `expect_or_drop`
# MAGIC > compte les lignes écartées et les jette. Pour les conserver, j'ai dû construire un
# MAGIC > dataset intermédiaire et un second dataset miroir — soit exactement le travail que
# MAGIC > le déclaratif était censé m'épargner, avec en prime les prédicats exprimés deux
# MAGIC > fois. Le modèle est conçu autour de l'idée qu'une ligne invalide se jette. Ce n'est
# MAGIC > pas mon cas d'usage.
# MAGIC >
# MAGIC > **Les contrôles inter-tables.** M6 vérifie que `gold.fact_order_line` n'a aucune
# MAGIC > clé orpheline dans `gold.dim_seller`. Une attente porte sur *un* dataset ; elle ne
# MAGIC > sait pas exprimer une contrainte entre deux. Contournable par un dataset dédié qui
# MAGIC > matérialise la jointure anti — mais on sort du modèle.
# MAGIC >
# MAGIC > **Les contrôles bloquants avant écriture.** L'`assert` de M5 sur la jointure
# MAGIC > temporelle arrête le traitement avant d'écrire quoi que ce soit. `expect_or_fail`
# MAGIC > s'en rapproche, mais s'évalue par ligne, pas sur une propriété agrégée du dataset
# MAGIC > (« le référentiel n'est pas vide », « le compte n'a pas chuté de 40 % »).
# MAGIC >
# MAGIC > **La lisibilité du débogage.** Une erreur dans un notebook se déboguent cellule par
# MAGIC > cellule. Une erreur dans un pipeline se lit dans le journal d'événements.
# MAGIC
# MAGIC **3. Quelle règle mériterait un `expect_or_fail` ?**
# MAGIC
# MAGIC > La réponse évidente serait `valid_timestamp`, parce que c'est la règle la plus
# MAGIC > violée. C'est la mauvaise réponse : 1 422 lignes sur 284 333, soit 0,5 %, c'est un
# MAGIC > bruit de fond stable depuis six mois. Faire échouer tout le pipeline chaque nuit
# MAGIC > pour 0,5 % de lignes datées `0000-00-00` priverait l'entreprise de 99,5 % de ses
# MAGIC > données pour un problème connu, documenté et sans urgence.
# MAGIC >
# MAGIC > La bonne réponse est **`known_status`** — la seule règle actuellement à zéro échec.
# MAGIC >
# MAGIC > Le raisonnement : un statut inconnu ne signifie pas « ligne sale », il signifie
# MAGIC > **« la source a changé de vocabulaire »**. Le jour où `REFUNDED` apparaît, un
# MAGIC > `expect_or_drop` écarterait silencieusement les remboursements et le CA baisserait
# MAGIC > sans que personne ne sache pourquoi. Pire : `is_revenue` est calculé par exclusion
# MAGIC > (`NOT IN ('CANCELLED', 'RETURNED')`), donc un `REFUNDED` qui passerait compterait
# MAGIC > comme du chiffre d'affaires.
# MAGIC >
# MAGIC > Le critère général : on arrête sur ce qui révèle un **changement de contrat**, pas
# MAGIC > sur ce qui mesure un taux de saleté connu. Le premier exige une décision humaine ;
# MAGIC > le second n'exige rien du tout.
# MAGIC
# MAGIC **4. Pourquoi le silver est-il recalculé intégralement ?**
# MAGIC
# MAGIC > À cause de la déduplication. `row_number()` sur toutes les versions d'un
# MAGIC > `order_line_id` a besoin de voir **toutes** les occurrences, y compris celles
# MAGIC > arrivées dans un lot précédent. Un traitement de streaming ne voit que le nouveau
# MAGIC > micro-lot : il ne peut pas savoir que la ligne qu'il traite est le rejeu d'une
# MAGIC > ligne d'il y a deux jours.
# MAGIC >
# MAGIC > Une vue matérialisée relit tout à chaque exécution et n'a pas ce problème. Sur
# MAGIC > 288 000 lignes, c'est quelques secondes — le bon compromis ici.
# MAGIC >
# MAGIC > Pour le rendre incrémental, il faudrait maintenir un **état** : soit
# MAGIC > `dropDuplicatesWithinWatermark` (qui borne la mémoire et accepte donc de laisser
# MAGIC > passer un doublon au-delà de la fenêtre — inacceptable si le rejeu peut survenir
# MAGIC > des mois plus tard), soit un `MERGE` sur clé qui remplace la ligne existante.
# MAGIC >
# MAGIC > On ne l'a pas fait parce que le volume ne le justifie pas, et parce que la version
# MAGIC > incrémentale échange une propriété très confortable — le résultat ne dépend pas de
# MAGIC > l'historique des exécutions — contre une performance dont on n'a pas besoin.
# MAGIC > À 300 millions de lignes, l'arbitrage s'inverse.
# MAGIC
# MAGIC **5. Pipeline ou notebooks ?**
# MAGIC
# MAGIC > Les deux, sur des parties différentes, et le critère est : **le degré de contrôle
# MAGIC > requis sur le traitement des rejets.**
# MAGIC >
# MAGIC > **Pipeline déclaratif** pour `landing → bronze` et pour les agrégats gold. Ce sont
# MAGIC > des transformations où l'on veut de l'incrémental fiable, de l'observabilité
# MAGIC > gratuite et zéro plomberie. Je n'y ai aucune décision métier à prendre.
# MAGIC >
# MAGIC > **Notebooks** pour `bronze → silver` et pour le SCD2 de M4. Ce sont les endroits
# MAGIC > où la logique métier est dense, où les rejets doivent être conservés et rejouables,
# MAGIC > et où j'ai besoin d'écrire des `assert` avant d'écrire. Le `MERGE` SCD2 en deux
# MAGIC > temps de M4 n'a d'ailleurs pas d'équivalent naturel dans le modèle déclaratif —
# MAGIC > `AUTO CDC` (ex-`APPLY CHANGES`) le fait, mais avec ses propres conventions, et il
# MAGIC > aurait fallu s'y plier plutôt que de comprendre ce qu'on faisait.
# MAGIC >
# MAGIC > Formulé autrement : le déclaratif est excellent là où le métier est **simple et le
# MAGIC > volume élevé**, et contraignant là où le métier est **subtil et le volume faible**.
# MAGIC > Sur NovaMarket, c'est le second cas qui domine — mais ce serait l'inverse sur un
# MAGIC > flux de télémétrie.
