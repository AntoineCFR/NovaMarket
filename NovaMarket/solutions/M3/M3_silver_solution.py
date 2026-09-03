# Databricks notebook source
# MAGIC %md
# MAGIC # Corrigé — M3 : couche silver

# COMMAND ----------

from pyspark.sql import functions as F, Window as W
from datetime import datetime
import uuid

CATALOG = "novamarket"
VALID_STATUSES = ["DELIVERED", "SHIPPED", "PENDING", "CANCELLED", "RETURNED"]
NON_REVENUE_STATUSES = ["CANCELLED", "RETURNED"]
TS_FORMAT = "yyyy-MM-dd HH:mm:ss"

BATCH_ID = str(uuid.uuid4())

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO A — inventaire du bruit sur `unit_price`
# MAGIC
# MAGIC Le rendu à l'écran ment : `" 88,73"` et `"88,73"` sont visuellement identiques.
# MAGIC Il faut regarder les codes de caractères.

# COMMAND ----------

bronze = spark.table(f"{CATALOG}.bronze.orders_raw")

display(
    bronze.filter(~F.col("unit_price").rlike(r"^[0-9]+,[0-9]{2}$"))
          .select("unit_price",
                  F.ascii("unit_price").alias("code_1er_caractere"),
                  F.length("unit_price").alias("longueur"))
          .groupBy("code_1er_caractere")
          .agg(F.count("*").alias("n"), F.first("unit_price").alias("exemple"))
          .orderBy("code_1er_caractere")
)

# MAGIC %md
# MAGIC Quatre familles, 1 102 lignes au total :
# MAGIC
# MAGIC | Forme | Exemple | Piège |
# MAGIC |---|---|---|
# MAGIC | Symbole monétaire suffixé | `74,48 €` | visible |
# MAGIC | Préfixe `EUR` | `EUR 94,55` | visible |
# MAGIC | Point décimal anglo-saxon | `88.73` | discret |
# MAGIC | **Espace insécable en tête** | ` 88,73` | **invisible** (code 160, pas 32) |
# MAGIC
# MAGIC Le quatrième cas est celui qui casse les nettoyages écrits à la main : un `trim()`
# MAGIC ne retire pas U+00A0, et `ltrim` non plus. D'où le choix d'une règle par liste
# MAGIC blanche (« je ne garde que ce que je sais lire ») plutôt que par liste noire
# MAGIC (« je retire les caractères qui me gênent ») — on ne peut pas énumérer ce qu'on
# MAGIC n'a pas vu.

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO C — expressions de nettoyage

# COMMAND ----------


# ATTENTION — le mode ANSI est ACTIF sur le compute serverless (verifie le 5 aout 2026).
# Un `cast` qui echoue ne rend plus NULL : il LEVE (CAST_INVALID_INPUT 22018,
# CANNOT_PARSE_TIMESTAMP 22007). Or toute la quarantaine de ce module repose sur
# `isNull()` — sans protection, le notebook s'arrete sur la premiere valeur sale,
# c'est-a-dire exactement celle qu'on voulait mettre de cote.
# D'ou `try_cast` et `try_to_timestamp` partout ci-dessous, au lieu de `cast` et
# `to_timestamp`. Detail dans docs/01-contraintes-free-edition.md.


def clean_decimal(col):
    """Chaine polluee -> decimal(10,2). Liste blanche : chiffres, virgule, point, signe."""
    stripped = F.regexp_replace(col, r"[^0-9,.\-]", "")
    normalized = F.regexp_replace(stripped, ",", ".")
    return normalized.try_cast("decimal(10,2)")


def clean_status(col):
    return F.upper(F.trim(col))


def parse_ts(col):
    # Format strict : on veut detecter ce qui ne respecte pas le contrat, pas le rattraper.
    return F.try_to_timestamp(col, F.lit(TS_FORMAT))


def clean_int(col):
    return F.regexp_replace(col, r"[^0-9\-]", "").try_cast("int")


# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO B — déduplication
# MAGIC
# MAGIC `dropDuplicates(["order_line_id"])` fonctionnerait sur ce jeu de données, mais ne
# MAGIC garantit rien : la ligne conservée dépend du plan d'exécution. Deux exécutions
# MAGIC peuvent retenir des lignes différentes — ici sans conséquence puisque les doublons
# MAGIC sont des copies conformes, mais c'est une dette qu'on ne veut pas.
# MAGIC
# MAGIC Une fenêtre avec un ordre explicite rend le choix reproductible et **documenté** :
# MAGIC en cas de conflit, la plus ancienne occurrence fait foi.

# COMMAND ----------

dedup_window = W.partitionBy("order_line_id").orderBy(
    F.col("_source_file").asc(),
    F.col("_source_file_modification_time").asc(),
)

deduped = (bronze
           .withColumn("_rn", F.row_number().over(dedup_window))
           .filter(F.col("_rn") == 1)
           .drop("_rn"))

print(f"{bronze.count():,} -> {deduped.count():,} (attendu 284 333)".replace(",", " "))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO D — motifs de quarantaine
# MAGIC
# MAGIC `array_compact` élimine les `null` : chaque motif contribue au tableau seulement
# MAGIC s'il est déclenché. Un tableau vide = la ligne est saine.

# COMMAND ----------

validated = deduped.withColumn(
    "quarantine_reasons",
    F.array_compact(F.array(
        F.when(parse_ts("order_ts").isNull(), F.lit("INVALID_TIMESTAMP")),
        F.when((clean_int("quantity").isNull()) | (clean_int("quantity") <= 0),
               F.lit("INVALID_QUANTITY")),
        F.when((clean_decimal("unit_price").isNull()) | (clean_decimal("unit_price") <= 0),
               F.lit("INVALID_PRICE")),
        F.when(~clean_status("order_status").isin(VALID_STATUSES), F.lit("UNKNOWN_STATUS")),
    ))
)
# Pas de .cache() ici : PERSIST est indisponible sur serverless (Free Edition).

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO E — quarantaine

# COMMAND ----------

SOURCE_COLS = ["order_id", "order_line_id", "order_ts", "customer_id", "seller_id",
               "product_id", "quantity", "unit_price", "discount_amount", "currency",
               "shipping_country", "payment_method", "order_status", "shipping_address"]

(validated
 .filter(F.size("quarantine_reasons") > 0)
 .select(*SOURCE_COLS, "_rescued_data", "_source_file",
         "quarantine_reasons",
         F.current_timestamp().alias("quarantined_at"))
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.ops.quarantine_order_line"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO F — silver.order_line
# MAGIC
# MAGIC Deux points d'implémentation :
# MAGIC
# MAGIC - **jointure gauche** pour les orphelins. Une jointure interne supprimerait
# MAGIC   exactement les lignes qu'on veut signaler — l'erreur classique, et la plus
# MAGIC   silencieuse : la table paraît propre et le CA a fondu de 0,6 %.
# MAGIC - `broadcast` sur les deux référentiels (25 060 et 8 000 lignes) : ça évite deux
# MAGIC   shuffles sur 284 000 lignes.

# COMMAND ----------

known_customers = (spark.table(f"{CATALOG}.bronze.app_customers_raw")
                   .select("customer_id").distinct().withColumn("_c_known", F.lit(True)))
known_products = (spark.table(f"{CATALOG}.bronze.ref_products_raw")
                  .select("product_id").distinct().withColumn("_p_known", F.lit(True)))

# Adresses reparees en M1. Le `.distinct()` n'est PAS decoratif : la table porte 1 087
# lignes pour 1 073 cles, parce que quatorze lignes defectueuses etaient elles-memes
# dupliquees dans les fichiers. Sans lui, la jointure gauche ajoute 14 lignes a
# silver.order_line — assez pour rater le compte attendu, trop peu pour se voir.
address_repair = (spark.table(f"{CATALOG}.bronze.orders_address_repair")
                  .select("order_line_id", "shipping_address_full")
                  .distinct())

clean = validated.filter(F.size("quarantine_reasons") == 0)

# Les expressions de nettoyage sont calculees une fois dans des colonnes temporaires
# prefixees par `_`, puis projetees. Les appeler directement dans le `select` final
# fonctionnerait, mais recalculerait `parse_ts("order_ts")` trois fois.
silver = (
    clean
    .join(F.broadcast(known_customers), on="customer_id", how="left")
    .join(F.broadcast(known_products), on="product_id", how="left")
    .join(F.broadcast(address_repair), on="order_line_id", how="left")
    .withColumn("_ts", parse_ts("order_ts"))
    .withColumn("_qty", clean_int("quantity"))
    .withColumn("_unit", clean_decimal("unit_price"))
    .withColumn("_disc", F.coalesce(clean_decimal("discount_amount"),
                                    F.lit(0).cast("decimal(10,2)")))
    .withColumn("_gross", (F.col("_qty") * F.col("_unit")).cast("decimal(12,2)"))
    .select(
        F.col("order_line_id"),
        F.col("order_id"),
        F.col("_ts").alias("order_ts"),
        F.col("_ts").cast("date").alias("order_date"),
        F.col("customer_id"),
        F.col("seller_id"),
        F.col("product_id"),
        F.col("_qty").alias("quantity"),
        F.col("_unit").alias("unit_price"),
        F.col("_disc").alias("discount_amount"),
        F.col("_gross").alias("gross_amount"),
        (F.col("_gross") - F.col("_disc")).cast("decimal(12,2)").alias("net_amount"),
        F.col("currency"),
        F.col("shipping_country"),
        F.col("payment_method"),
        clean_status("order_status").alias("order_status"),
        (~clean_status("order_status").isin(NON_REVENUE_STATUSES)).alias("is_revenue"),
        F.col("_c_known").isNull().alias("is_orphan_customer"),
        F.col("_p_known").isNull().alias("is_orphan_product"),
        F.coalesce(F.col("shipping_address_full"),
                   F.col("shipping_address")).alias("shipping_address"),
        F.col("_source_file"),
        F.current_timestamp().alias("_silver_processed_at"),
    )
)

(silver.write.mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.silver.order_line"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Partie 2 — événements

# COMMAND ----------

ev_bronze = spark.table(f"{CATALOG}.bronze.events_raw")

# TODO B — quarantaine des JSON illisibles : ils n'ont pas d'event_id.
#
# Le texte d'origine est dans `_corrupt_record`, PAS dans `_rescued_data` : un
# enregistrement illisible n'est pas un ecart au schema, c'est un echec d'analyse.
# `_rescued_data` est vide sur cette source (verifie le 03/08/2026).
# On prend la premiere des deux colonnes presentes, pour rester robuste.
brut = F.coalesce(*[F.col(c) for c in ("_corrupt_record", "_rescued_data")
                    if c in ev_bronze.columns])

(ev_bronze
 .filter(F.col("event_id").isNull())
 .select(brut.alias("raw_record"), "_source_file",
         F.array(F.lit("MALFORMED_JSON")).alias("quarantine_reasons"),
         F.current_timestamp().alias("quarantined_at"))
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.ops.quarantine_event"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO C — l'horodatage à deux visages
# MAGIC
# MAGIC 1 269 événements portent un epoch en millisecondes au lieu d'une chaîne ISO.
# MAGIC L'inférence retient `string` pour `event_ts` : les deux formes cohabitent donc dans
# MAGIC la colonne, les entiers convertis en chaînes de chiffres. L'expression ci-dessous
# MAGIC suffit, sans avoir à aller chercher quoi que ce soit ailleurs.
# MAGIC
# MAGIC Le test `rlike(r"^\d{12,13}$")` distingue les deux sans ambiguïté : un horodatage
# MAGIC ISO commence toujours par `2026-`.

# COMMAND ----------

# `try_to_timestamp` et non `to_timestamp` : sous ANSI, une chaine ISO malformee
# arreterait le notebook au lieu de rendre NULL. Le `cast("long")` est deja protege par
# le `rlike` qui le precede — la branche ne s'evalue que sur 12 ou 13 chiffres.
event_ts_expr = F.when(
    F.col("event_ts").cast("string").rlike(r"^\d{12,13}$"),
    (F.col("event_ts").cast("string").cast("long") / 1000).cast("timestamp"),
).otherwise(
    F.try_to_timestamp(F.col("event_ts").cast("string"),
                       F.lit("yyyy-MM-dd'T'HH:mm:ss'Z'"))
)

# Si un jour l'inference retenait un type numerique, les chaines ISO partiraient au
# rescue et il faudrait aller les y rechercher :
# event_ts_expr = F.coalesce(
#     event_ts_expr,
#     F.to_timestamp(F.get_json_object("_rescued_data", "$.event_ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO D — aplatissement et déduplication

# COMMAND ----------

ev_dedup_window = W.partitionBy("event_id").orderBy(
    F.col("_source_file").asc(), F.col("_source_file_modification_time").asc()
)

ev_clean = (ev_bronze
            .filter(F.col("event_id").isNotNull())
            .withColumn("_rn", F.row_number().over(ev_dedup_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn"))

event = ev_clean.select(
    F.col("event_id"),
    event_ts_expr.alias("event_ts"),
    event_ts_expr.cast("date").alias("event_date"),
    F.col("event_type"),
    F.col("user.customer_id").alias("customer_id"),
    F.col("user.session_id").alias("session_id"),
    F.col("user.segment").alias("segment"),
    F.col("device.os").alias("os"),
    F.col("device.app_version").alias("app_version"),
    F.col("device.is_mobile").cast("boolean").alias("is_mobile"),
    F.col("context.page").alias("page"),
    F.col("context.referrer").alias("referrer"),
    F.col("context.utm.source").alias("utm_source"),
    F.col("context.utm.medium").alias("utm_medium"),
    F.col("context.utm.campaign").alias("utm_campaign"),
    F.col("search_term"),
    F.col("order_id"),
    F.coalesce(F.size("items"), F.lit(0)).alias("n_items"),
    F.col("_source_file"),
    F.current_timestamp().alias("_silver_processed_at"),
)

(event.write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{CATALOG}.silver.event"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO E — explosion des items
# MAGIC
# MAGIC `posexplode` (et non `posexplode_outer`) : une table fille ne contient que les
# MAGIC lignes filles. Un événement sans item n'a rien à y faire — l'information « cet
# MAGIC événement n'a pas d'item » est déjà portée par `n_items` dans la table mère.
# MAGIC
# MAGIC `qty` et `price` réutilisent les mêmes fonctions de nettoyage que les commandes.
# MAGIC Écrire deux fois la même règle de nettoyage, c'est garantir qu'elles divergeront.

# COMMAND ----------

event_item = (
    ev_clean
    .select("event_id", F.posexplode("items").alias("item_index", "item"))
    .select(
        F.col("event_id"),
        F.col("item_index").cast("int").alias("item_index"),
        F.col("item.product_id").cast("string").alias("product_id"),
        clean_int(F.col("item.qty").cast("string")).alias("qty"),
        clean_decimal(F.col("item.price").cast("string")).alias("price"),
        F.current_timestamp().alias("_silver_processed_at"),
    )
)

(event_item.write.mode("overwrite").option("overwriteSchema", "true")
           .saveAsTable(f"{CATALOG}.silver.event_item"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Réponses
# MAGIC
# MAGIC **1. Les six lignes à double motif**
# MAGIC
# MAGIC ```sql
# MAGIC SELECT order_line_id, order_ts, quantity, quarantine_reasons
# MAGIC FROM novamarket.ops.quarantine_order_line
# MAGIC WHERE size(quarantine_reasons) > 1
# MAGIC ```
# MAGIC
# MAGIC > Six lignes cumulent `INVALID_TIMESTAMP` et `INVALID_QUANTITY`. Rien de mystérieux :
# MAGIC > les défauts sont injectés indépendamment sur ~0,5 % et ~0,3 % des lignes, donc
# MAGIC > leur coïncidence est attendue sur environ 284 333 × 0,005 × 0,003 ≈ 4 lignes.
# MAGIC > Six est parfaitement dans l'ordre de grandeur.
# MAGIC >
# MAGIC > Le point à retenir n'est pas leur origine mais le fait qu'**il faut les compter
# MAGIC > une fois**. Une quarantaine construite par `union` de quatre `filter` en aurait
# MAGIC > produit 2 235 lignes dont 6 doublons — et l'invariant aurait cassé.
# MAGIC
# MAGIC **2. Fallait-il implémenter deux motifs qui ne se déclenchent jamais ?**
# MAGIC
# MAGIC > Oui, et pour deux raisons distinctes.
# MAGIC >
# MAGIC > `UNKNOWN_STATUS` : le fait qu'il vaille 0 est précisément **le résultat du test**.
# MAGIC > Il démontre que la normalisation de casse couvre 100 % des variantes présentes.
# MAGIC > Sans lui, on ne saurait pas si la liste `VALID_STATUSES` est complète — on
# MAGIC > l'espérerait. Et le jour où la source ajoutera `REFUNDED`, il se déclenchera au
# MAGIC > lieu de laisser passer une valeur inconnue dans le calcul de `is_revenue`.
# MAGIC >
# MAGIC > `INVALID_PRICE` : il vaut 0 aujourd'hui parce que le nettoyage par liste blanche
# MAGIC > est robuste. Il ne vaudra pas 0 le jour où la source enverra `"N/A"` ou un prix
# MAGIC > en centimes. Un compteur à zéro qui bouge est une alerte ; un contrôle absent
# MAGIC > n'alerte jamais.
# MAGIC >
# MAGIC > Ces deux compteurs sont exactement ce qu'on branchera sur le tableau de bord
# MAGIC > qualité en M6.
# MAGIC
# MAGIC **3. Processus de reprise des 2 229 lignes**
# MAGIC
# MAGIC > Les deux motifs ne se traitent pas de la même façon.
# MAGIC >
# MAGIC > Les 1 422 horodatages invalides sont *partiellement* récupérables : `06/02/2026`
# MAGIC > et `2026-06-02T17:50` portent une date exploitable, `0000-00-00` et la chaîne
# MAGIC > vide non. On peut aussi replier sur la date du fichier source (`_source_file`
# MAGIC > porte la date du lot) : la commande a forcément eu lieu ce jour-là. C'est une
# MAGIC > décision métier — accepter une date à la journée près plutôt que perdre la vente.
# MAGIC >
# MAGIC > Les 813 quantités invalides ne se devinent pas. Elles doivent remonter à l'équipe
# MAGIC > source, avec le `order_line_id` et le fichier d'origine.
# MAGIC >
# MAGIC > Mécanique de rejeu : la table de quarantaine est **recalculée intégralement** à
# MAGIC > chaque exécution (`overwrite`) à partir de bronze, qui est la source de vérité.
# MAGIC > Corriger revient donc à corriger la règle, pas la table. Une ligne corrigée
# MAGIC > rentre naturellement en silver au passage suivant, et l'unicité de
# MAGIC > `order_line_id` est préservée par construction puisque silver est lui aussi
# MAGIC > recalculé. C'est le principal avantage d'un silver idempotent sur un silver
# MAGIC > incrémental — avantage qu'on perdra en partie en M4 avec le SCD2, et c'est
# MAGIC > justement là que ça deviendra intéressant.
# MAGIC >
# MAGIC > Fréquence : revue hebdomadaire de la quarantaine, avec seuil d'alerte. Décideur :
# MAGIC > le propriétaire de la donnée côté métier, pas l'équipe data — nous produisons le
# MAGIC > constat, pas l'arbitrage.
# MAGIC
# MAGIC **4. Seuil d'échec sur l'orphelinat**
# MAGIC
# MAGIC > 1 721 lignes orphelines sur 282 104, soit 0,61 %. Un taux stable et faible : c'est
# MAGIC > du bruit de désynchronisation entre deux systèmes, pas un incident.
# MAGIC >
# MAGIC > Le seuil pertinent n'est pas une valeur absolue mais une **variation**. Un
# MAGIC > pipeline qui échouerait à 1 % passerait sans broncher de 0,6 % à 0,9 % — alors que
# MAGIC > c'est un dérapage de 50 %. À l'inverse, un pic à 15 % un lundi matin signale
# MAGIC > presque toujours la même chose : le référentiel n'a pas été livré, et on est en
# MAGIC > train de comparer les commandes du jour à un catalogue vide.
# MAGIC >
# MAGIC > La règle que je retiens : alerte si le taux dépasse 2× la moyenne des 7 derniers
# MAGIC > jours, **échec bloquant** au-delà de 10 % ou si le référentiel joint est vide.
# MAGIC > Ce dernier contrôle — « ma table de jointure est-elle plausible ? » — attrape à
# MAGIC > lui seul la majorité des incidents réels, et il est trivial à écrire.
# MAGIC > On l'implémentera en M6.
