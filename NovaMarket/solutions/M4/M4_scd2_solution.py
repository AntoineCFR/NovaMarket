# Databricks notebook source
# MAGIC %md
# MAGIC # Corrigé — M4 : historisation SCD2

# COMMAND ----------

from pyspark.sql import functions as F, Window as W
from datetime import datetime
import uuid

CATALOG = "novamarket"
BATCH_ID = str(uuid.uuid4())

CUSTOMER_TRACKED = ["first_name", "last_name", "email", "country", "city", "zip_code",
                    "segment", "is_opt_in", "is_deleted"]
SELLER_TRACKED = ["seller_name", "seller_country", "seller_city", "main_top_category",
                  "plan_code", "is_active"]
CUSTOMER_CARRIED = ["created_at"]
SELLER_CARRIED = ["onboarded_at"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO A — l'empreinte
# MAGIC
# MAGIC Deux précautions qui ont l'air anodines et ne le sont pas :
# MAGIC
# MAGIC - `coalesce` vers un jeton explicite. Sans lui, `concat_ws` **ignore** les `null`,
# MAGIC   ce qui rend `("Paris", null)` et `(null, "Paris")` indiscernables.
# MAGIC - un séparateur qui ne peut pas apparaître dans les données. `||` conviendrait mal
# MAGIC   pour du texte libre ; `U+001F` (unit separator, un caractère de contrôle) est le
# MAGIC   choix habituel : il ne survit à aucune saisie humaine.

# COMMAND ----------

SEP = "\u001f"          # unit separator, absent de toute saisie humaine
NULL_TOKEN = "\u001fNULL"


def scd_hash(cols):
    return F.sha2(
        F.concat_ws(SEP, *[F.coalesce(F.col(c).cast("string"), F.lit(NULL_TOKEN)) for c in cols]),
        256,
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO B — reconstruction complète
# MAGIC
# MAGIC `tracked + carried` donne exactement l'ordre de colonnes imposé par le schéma :
# MAGIC les attributs suivis d'abord, les attributs transportés ensuite.
# MAGIC
# MAGIC Le tri porte sur `(updated_at, _extracted_at)`. Le second critère n'est pas
# MAGIC cosmétique : l'extraction en `>=` de M2 produit des lignes portant exactement le
# MAGIC même `updated_at` que celles déjà présentes. Sans lui, l'ordre des versions dépend
# MAGIC du plan d'exécution, donc l'historique change d'une exécution à l'autre.

# COMMAND ----------


def rebuild_scd2(journal, key, tracked, carried):
    order = [F.col("updated_at").asc(), F.col("_extracted_at").asc()]
    w = W.partitionBy(key).orderBy(*order)

    hashed = journal.withColumn("_scd_hash", scd_hash(tracked))

    # Fusion : on ne garde une version que si son empreinte diffère de la précédente.
    collapsed = (hashed
                 .withColumn("_prev_hash", F.lag("_scd_hash").over(w))
                 .filter(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("_scd_hash"))))

    return (collapsed
            .withColumn("valid_from", F.col("updated_at"))
            .withColumn("valid_to", F.lead("updated_at").over(w))
            .withColumn("is_current", F.col("valid_to").isNull())
            .withColumn("_processed_at", F.current_timestamp())
            .select(key, *tracked, *carried,
                    "valid_from", "valid_to", "is_current", "_scd_hash", "_processed_at"))


# COMMAND ----------

# MAGIC %md
# MAGIC Piège à connaître : la fenêtre du `lead` s'applique à `collapsed`, **après** la
# MAGIC fusion. Si on calculait `valid_to` avant de fusionner, une version fantôme viendrait
# MAGIC clore prématurément la version réelle qui la précède, et on obtiendrait des
# MAGIC intervalles de durée nulle.

# COMMAND ----------

(rebuild_scd2(spark.table(f"{CATALOG}.bronze.app_sellers_raw"),
              "seller_id", SELLER_TRACKED, SELLER_CARRIED)
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.silver.seller_scd2"))

(rebuild_scd2(spark.table(f"{CATALOG}.bronze.app_customers_raw"),
              "customer_id", CUSTOMER_TRACKED, CUSTOMER_CARRIED)
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.silver.customer_scd2"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO C — contrôles d'intégrité temporelle

# COMMAND ----------


def integrity_checks(table, key):
    df = spark.table(f"{CATALOG}.silver.{table}")
    w = W.partitionBy(key).orderBy("valid_from")

    chained = (df.withColumn("_next_from", F.lead("valid_from").over(w))
                 .withColumn("_prev_hash", F.lag("_scd_hash").over(w)))

    return {
        "une seule version courante par clé":
            df.groupBy(key).agg(F.sum(F.col("is_current").cast("int")).alias("n"))
              .filter("n <> 1").count(),
        "aucune version courante avec valid_to":
            df.filter("is_current AND valid_to IS NOT NULL").count(),
        "aucun intervalle vide ou inversé":
            df.filter("valid_to IS NOT NULL AND valid_to <= valid_from").count(),
        "chaînage sans rupture":
            chained.filter(F.col("_next_from").isNotNull())
                   .filter(F.col("valid_to") != F.col("_next_from")).count(),
        "aucune empreinte consécutive identique":
            chained.filter(F.col("_prev_hash") == F.col("_scd_hash")).count(),
    }


for table, key in [("seller_scd2", "seller_id"), ("customer_scd2", "customer_id")]:
    print(table)
    for name, n in integrity_checks(table, key).items():
        print(f"    {'OK ' if n == 0 else 'KO '} {name:44s} {n}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO D — le delta à appliquer
# MAGIC
# MAGIC Une clé entre dans le delta si :
# MAGIC
# MAGIC - elle est inconnue du SCD2 (nouveau vendeur, nouveau client), **ou**
# MAGIC - son empreinte diffère de celle de sa version courante, et sa dernière version
# MAGIC   observée est postérieure à cette version courante.
# MAGIC
# MAGIC La seconde condition sur `updated_at` protège contre un delta arrivé en retard qui
# MAGIC tenterait de réécrire le passé — cas que ce jeu de données ne produit pas, mais qui
# MAGIC arrive en production dès qu'une extraction est rejouée manuellement.

# COMMAND ----------


def compute_delta(journal, scd2_table, key, tracked):
    current = (spark.table(scd2_table).filter("is_current")
               .select(key,
                       F.col("_scd_hash").alias("_cur_hash"),
                       F.col("valid_from").alias("_cur_from")))

    w = W.partitionBy(key).orderBy(F.col("updated_at").desc(), F.col("_extracted_at").desc())
    latest = (journal
              .withColumn("_scd_hash", scd_hash(tracked))
              .withColumn("_rn", F.row_number().over(w))
              .filter("_rn = 1").drop("_rn"))

    return (latest.join(F.broadcast(current), on=key, how="left")
            .filter(F.col("_cur_hash").isNull()
                    | ((F.col("_scd_hash") != F.col("_cur_hash"))
                       & (F.col("updated_at") > F.col("_cur_from")))))


# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO E — le `MERGE` SCD2 en deux temps
# MAGIC
# MAGIC Un `MERGE` ne peut pas, sur une même correspondance, mettre à jour une ligne **et**
# MAGIC en insérer une autre. On duplique donc chaque changement dans la source :
# MAGIC
# MAGIC - une copie avec `_merge_key = clé` → correspond à la version courante → `UPDATE`,
# MAGIC   qui la ferme ;
# MAGIC - une copie avec `_merge_key = null` → ne correspond à rien → `INSERT` de la
# MAGIC   nouvelle version.
# MAGIC
# MAGIC `null` ne correspond jamais dans une jointure : c'est précisément ce qu'on exploite.

# COMMAND ----------


def merge_scd2(delta, scd2_table, key, tracked, carried):
    if delta.isEmpty():
        print(f"{scd2_table} : aucun changement à appliquer")
        return

    cols = [key] + tracked + carried

    staged = (
        delta.withColumn("_merge_key", F.col(key))
        .unionByName(delta.withColumn("_merge_key", F.lit(None).cast("string")))
        .select(*cols, "_scd_hash",
                F.col("updated_at").alias("valid_from"),
                "_merge_key")
    )
    staged.createOrReplaceTempView("_scd_staged")

    insert_cols = ", ".join(cols + ["valid_from", "valid_to", "is_current", "_scd_hash", "_processed_at"])
    insert_vals = ", ".join([f"s.{c}" for c in cols]
                            + ["s.valid_from", "NULL", "true", "s._scd_hash", "current_timestamp()"])

    spark.sql(f"""
        MERGE INTO {scd2_table} t
        USING _scd_staged s
          ON t.{key} = s._merge_key AND t.is_current
        WHEN MATCHED AND t._scd_hash <> s._scd_hash THEN
            UPDATE SET t.valid_to = s.valid_from, t.is_current = false
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
    """)


# COMMAND ----------

for journal, table, key, tracked, carried in [
    (f"{CATALOG}.bronze.app_sellers_raw", f"{CATALOG}.silver.seller_scd2",
     "seller_id", SELLER_TRACKED, SELLER_CARRIED),
    (f"{CATALOG}.bronze.app_customers_raw", f"{CATALOG}.silver.customer_scd2",
     "customer_id", CUSTOMER_TRACKED, CUSTOMER_CARRIED),
]:
    delta = compute_delta(spark.table(journal), table, key, tracked)
    print(f"{table} : {delta.count()} changement(s)")
    merge_scd2(delta, table, key, tracked, carried)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO F — vérification croisée
# MAGIC
# MAGIC Le seul test qui prouve qu'un pipeline incrémental est correct.
# MAGIC `_processed_at` est exclu de la comparaison : il diffère forcément.

# COMMAND ----------


def cross_check(journal_table, scd2_table, key, tracked, carried):
    merged = spark.table(scd2_table)
    rebuilt = rebuild_scd2(spark.table(journal_table), key, tracked, carried)

    cols = [key] + tracked + carried + ["valid_from", "valid_to", "is_current", "_scd_hash"]
    a, b = merged.select(*cols), rebuilt.select(*cols)

    only_merged = a.exceptAll(b).count()
    only_rebuilt = b.exceptAll(a).count()
    print(f"{scd2_table}")
    print(f"    présent après MERGE mais pas dans la reconstruction : {only_merged}")
    print(f"    présent dans la reconstruction mais pas après MERGE : {only_rebuilt}")
    return only_merged == 0 and only_rebuilt == 0


assert cross_check(f"{CATALOG}.bronze.app_sellers_raw", f"{CATALOG}.silver.seller_scd2",
                   "seller_id", SELLER_TRACKED, SELLER_CARRIED)
assert cross_check(f"{CATALOG}.bronze.app_customers_raw", f"{CATALOG}.silver.customer_scd2",
                   "customer_id", CUSTOMER_TRACKED, CUSTOMER_CARRIED)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO G — Change Data Feed

# COMMAND ----------

spark.sql(f"ALTER TABLE {CATALOG}.silver.seller_scd2 "
          f"SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# Version courante au moment de l'activation : le CDF ne remonte pas avant.
from_version = (spark.sql(f"DESCRIBE HISTORY {CATALOG}.silver.seller_scd2")
                .agg(F.max("version")).first()[0])

# ... rejouer un MERGE ici pour produire des changements observables ...

(spark.read.format("delta")
 .option("readChangeFeed", "true")
 .option("startingVersion", from_version)
 .table(f"{CATALOG}.silver.seller_scd2")
 .select("seller_id", "plan_code", "valid_from", "valid_to", "is_current",
         "_change_type", "_commit_version", "_commit_timestamp")
 .write.mode("append").option("mergeSchema", "true")
 .saveAsTable(f"{CATALOG}.ops.scd2_change_log"))

display(spark.table(f"{CATALOG}.ops.scd2_change_log")
        .groupBy("_commit_version", "_change_type").count()
        .orderBy("_commit_version", "_change_type"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Réponses
# MAGIC
# MAGIC **1. `valid_to` à `null` ou à `9999-12-31` ?**
# MAGIC
# MAGIC > `null` dit la vérité : on ne sait pas quand cette version cessera d'être valide.
# MAGIC > `9999-12-31` affirme une date de fin qui n'existe pas.
# MAGIC >
# MAGIC > L'argument d'en face est pratique et sérieux : avec une date sentinelle, la
# MAGIC > jointure temporelle s'écrit `ts >= valid_from AND ts < valid_to`, sans cas
# MAGIC > particulier. Avec `null`, il faut écrire `AND (valid_to IS NULL OR ts < valid_to)`
# MAGIC > — trois fois sur quatre, quelqu'un oubliera la moitié `IS NULL` et perdra
# MAGIC > silencieusement toutes les lignes courantes. C'est une erreur qu'on ne voit pas
# MAGIC > passer : le résultat n'est pas vide, juste amputé.
# MAGIC >
# MAGIC > Mon arbitrage : `null` dans la table, parce qu'une table doit rester honnête, et
# MAGIC > une **vue** exposée aux analystes qui matérialise `coalesce(valid_to, '9999-12-31')`.
# MAGIC > On paie la complexité une fois, à un endroit, plutôt que dans chaque requête.
# MAGIC
# MAGIC **2. D'où viennent les 401 et 41 versions fusionnées ?**
# MAGIC
# MAGIC > Directement de l'arbitrage `>=` de M2. Chaque extraction reprend les lignes
# MAGIC > situées **pile** sur le watermark, y compris celles qui n'ont pas changé :
# MAGIC >
# MAGIC > | Extraction | Clients ré-extraits inchangés | Vendeurs |
# MAGIC > |---|---|---|
# MAGIC > | 2 | 21 (lignes sur le watermark initial) | 1 |
# MAGIC > | 3 | 380 (les lignes modifiées en D1, dont l'horodatage est devenu le watermark) | 40 |
# MAGIC >
# MAGIC > La seconde ligne est la plus instructive : les 385 lignes modifiées lors de D1
# MAGIC > portent toutes `updated_at = 2026-06-05 08:15:00`, qui devient le watermark. À
# MAGIC > l'extraction suivante, un filtre `>=` les reprend intégralement. Cinq d'entre
# MAGIC > elles ont changé à nouveau en D2 — restent 380 doublons stricts.
# MAGIC >
# MAGIC > Pouvait-on les éviter en amont ? Oui, en stockant en plus du watermark la liste
# MAGIC > des clés déjà vues à cet horodatage exact, et en les excluant. C'est faisable,
# MAGIC > c'est ce que font certains outils de CDC — et c'est un état supplémentaire à
# MAGIC > maintenir, à purger et à restaurer après incident.
# MAGIC >
# MAGIC > Le calcul coût/bénéfice penche nettement de l'autre côté : la déduplication par
# MAGIC > empreinte coûte une fonction de fenêtrage, elle est stateless, elle est testable,
# MAGIC > et elle protège **aussi** contre les rejeux manuels et les doubles exécutions de
# MAGIC > job — que la solution amont ne couvrirait pas. On préfère un pipeline tolérant
# MAGIC > aux doublons à un pipeline qui prétend ne jamais en produire.
# MAGIC
# MAGIC **3. Les trois clients à trois versions**
# MAGIC
# MAGIC ```sql
# MAGIC SELECT customer_id, valid_from, valid_to, is_current, segment, city, is_deleted
# MAGIC FROM novamarket.silver.customer_scd2
# MAGIC WHERE customer_id IN (
# MAGIC     SELECT customer_id FROM novamarket.silver.customer_scd2
# MAGIC     GROUP BY customer_id HAVING count(*) = 3)
# MAGIC ORDER BY customer_id, valid_from
# MAGIC ```
# MAGIC
# MAGIC > Ce sont les trois clients touchés par les **deux** journées d'activité : version
# MAGIC > d'origine (`valid_from` = leur date de création), version D1 du 5 juin, version D2
# MAGIC > du 6 juin. Ils démontrent que la chaîne se construit correctement sur plus de deux
# MAGIC > maillons — un SCD2 buggé passe souvent le cas à deux versions et casse au
# MAGIC > troisième, parce que le `lead` ou la borne de fermeture est écrit en dur.
# MAGIC
# MAGIC **4. Le `MERGE` est-il rejouable ?**
# MAGIC
# MAGIC > Oui, et c'est vérifiable en une cellule : relancer `compute_delta` juste après le
# MAGIC > `MERGE` renvoie **0 ligne**. La raison est structurelle : le delta est défini par
# MAGIC > différence entre le journal et l'état courant du SCD2. Une fois le changement
# MAGIC > appliqué, l'empreinte courante est celle du journal, la condition `_scd_hash <>
# MAGIC > _cur_hash` devient fausse, et il n'y a plus rien à faire.
# MAGIC >
# MAGIC > C'est ce qui distingue un pipeline **idempotent** d'un pipeline simplement
# MAGIC > incrémental. Un delta défini par « ce qui est arrivé depuis hier » n'aurait pas
# MAGIC > cette propriété : rejoué deux fois, il fermerait la version qu'il vient d'ouvrir
# MAGIC > et créerait un intervalle de durée nulle. La différence tient au fait que l'état
# MAGIC > est lu dans la table cible, pas dans une horloge.
# MAGIC
# MAGIC **5. Un vendeur supprimé de la source**
# MAGIC
# MAGIC > Sa version courante reste ouverte indéfiniment, `is_current = true`, `valid_to`
# MAGIC > à `null`. Le SCD2 continue d'affirmer qu'il est actif aujourd'hui.
# MAGIC >
# MAGIC > Est-ce voulu ? **Pour ce pipeline, oui** — et c'est même la seule chose correcte à
# MAGIC > faire, parce qu'on n'a aucun moyen de distinguer « ce vendeur a été supprimé » de
# MAGIC > « ce vendeur n'a pas changé ». Inventer une date de fin serait inventer une donnée.
# MAGIC >
# MAGIC > La suppression douce de la source nous couvre pour les clients (`is_deleted`
# MAGIC > devient un attribut suivi comme un autre, et la suppression crée une version
# MAGIC > normale). La table `app_sellers` n'a pas cet équivalent : c'est un trou dans le
# MAGIC > contrat d'interface, troisième entrée pour ton registre d'écarts.
# MAGIC >
# MAGIC > Si on devait le combler sans toucher à la source : comparer périodiquement
# MAGIC > l'ensemble des clés vues dans un snapshot complet avec les clés courantes du SCD2,
# MAGIC > et fermer les absentes. C'est un `MERGE` de plus, à fréquence hebdomadaire, et il
# MAGIC > faut assumer que la date de fermeture est celle de la détection, pas celle de la
# MAGIC > suppression réelle.
