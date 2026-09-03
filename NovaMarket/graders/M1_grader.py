# Databricks notebook source
# MAGIC %md
# MAGIC # Grader — M1 : couche bronze
# MAGIC
# MAGIC À exécuter **après** avoir téléversé la vague W2 et relancé les trois notebooks
# MAGIC d'ingestion. Les comptages attendus correspondent à W1 + W2.

# COMMAND ----------

# MAGIC %run ./_grader_lib

# COMMAND ----------

dbutils.widgets.text("catalog", "novamarket", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F

g = Grader(f"M1 — couche bronze ({CATALOG})")

# Valeurs de référence, produites par le générateur (W1_initial + W2).
EXP_ORDERS_ROWS = 287_785
EXP_ORDERS_FILES = 8
# 1 087 lignes du fichier portent des champs en trop (`;` non echappe dans l'adresse).
# Verifie le 31/07/2026 sur Databricks : le lecteur CSV **ne les sauve pas**, il les
# tronque. `_rescued_data` reste vide sur les commandes, et la mutilation ne se voit
# que sur `shipping_address`, dernier champ, qui perd sa partie apres le premier `;`.
# Une adresse saine vaut "<rue>, <cp> <ville>" : celles qui n'ont plus de virgule sont
# exactement les lignes amputees.
EXP_ORDERS_TRUNCATED = 1_087
EXP_EVENTS_ROWS = 131_068
EXP_EVENTS_FILES = 15
EXP_EVENTS_MALFORMED = 389      # lignes JSON illisibles ; le rescue peut en contenir plus

SOURCE_COLS = [
    "order_id", "order_line_id", "order_ts", "customer_id", "seller_id", "product_id",
    "quantity", "unit_price", "discount_amount", "currency", "shipping_country",
    "payment_method", "order_status", "shipping_address",
]
TECH_COLS = [
    "_rescued_data", "_source_file", "_source_file_modification_time",
    "_ingested_at", "_ingest_batch_id",
]

# COMMAND ----------


def table(name):
    return spark.table(f"{CATALOG}.bronze.{name}")


def dtypes(name):
    return dict(table(name).dtypes)


def rescued_count(name):
    return table(name).filter(F.col("_rescued_data").isNotNull()).count()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Commandes

# COMMAND ----------

g.truthy("orders_raw existe", lambda: table("orders_raw") is not None)
g.truthy("orders_raw : 14 colonnes source présentes",
         lambda: set(SOURCE_COLS).issubset(dtypes("orders_raw")))
g.truthy("orders_raw : colonnes source toutes en STRING",
         lambda: all(dtypes("orders_raw").get(c) == "string" for c in SOURCE_COLS),
         hint="aucun cast en bronze")
g.truthy("orders_raw : colonnes techniques présentes",
         lambda: set(TECH_COLS).issubset(dtypes("orders_raw")))
g.equals("orders_raw : _source_file_modification_time typé",
         lambda: dtypes("orders_raw").get("_source_file_modification_time"), "timestamp")
g.equals("orders_raw : nombre de lignes", lambda: table("orders_raw").count(), EXP_ORDERS_ROWS)
g.equals("orders_raw : fichiers sources distincts",
         lambda: table("orders_raw").select("_source_file").distinct().count(), EXP_ORDERS_FILES)
g.equals("orders_raw : aucun _source_file vide",
         lambda: table("orders_raw").filter(
             F.col("_source_file").isNull() | (F.trim("_source_file") == "")).count(), 0)
g.equals("orders_raw : _source_file porte un nom de fichier, pas un chemin complet",
         lambda: table("orders_raw").filter(F.col("_source_file").contains("/")).count(), 0)
g.equals("orders_raw : _rescued_data vide sur les commandes (aucun écart de type possible "
         "quand tout est STRING)", lambda: rescued_count("orders_raw"), 0,
         hint="si tu as des lignes sauvées ici, c'est que des colonnes sont typées")
g.band("orders_raw : lignes à l'adresse tronquée par le lecteur CSV",
       lambda: table("orders_raw").filter(~F.col("shipping_address").contains(",")).count(),
       EXP_ORDERS_TRUNCATED)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Réparation des adresses tronquées

# COMMAND ----------


def repair():
    return spark.table(f"{CATALOG}.bronze.orders_address_repair")


g.truthy("orders_address_repair existe", lambda: repair() is not None,
         hint="section 8 du notebook des commandes")
g.truthy("orders_address_repair : colonnes attendues",
         lambda: {"order_line_id", "shipping_address_full", "_source_file",
                  "_repaired_at"}.issubset(dict(repair().dtypes)))
g.band("orders_address_repair : lignes récupérées",
       lambda: repair().count(), EXP_ORDERS_TRUNCATED)
g.equals("orders_address_repair : autant de lignes que de tronquées en bronze",
         lambda: repair().count()
                 - table("orders_raw").filter(~F.col("shipping_address").contains(",")).count(),
         0, hint="les deux comptes doivent coïncider exactement")
# Attention : 14 des lignes tronquees sont elles-memes dupliquees dans les fichiers.
# La table de reparation herite donc de ces doublons, et c'est correct : bronze ne
# dedoublonne pas. 1 087 lignes pour 1 073 cles distinctes. Toute jointure sur
# order_line_id devra en tenir compte (cf. M3).
g.band("orders_address_repair : clés distinctes",
       lambda: repair().select("order_line_id").distinct().count(), 1_073)
# Attention : une adresse REPAREE ne ressemble pas a une adresse saine. La source ecrit
# le defaut avec des points-virgules et sans virgule :
#   saine    -> "130 quai des Chartrons, 28001 Madrid"
#   tronquee -> "38 quai des Chartrons"
#   reparee  -> "38 quai des Chartrons; Batiment D; 80331 Munich"
# On verifie donc que le recollage a bien eu lieu (le separateur est revenu), et que le
# resultat est strictement plus long que ce que bronze avait garde.
g.equals("orders_address_repair : le recollage a bien eu lieu",
         lambda: repair().filter(~F.col("shipping_address_full").contains(";")).count(), 0,
         hint="array_join doit remettre le ';' entre les fragments")
g.equals("orders_address_repair : l'adresse reconstituée est plus complète que la tronquée",
         lambda: (repair().alias("r")
                  .join(table("orders_raw").alias("b"), "order_line_id")
                  .filter(F.length("r.shipping_address_full")
                          <= F.length("b.shipping_address"))
                  .count()), 0,
         hint="slice mal borné : vérifie que tu pars bien du 14e champ")
# La reparation ne sert a rien si elle ne se raccroche pas au bronze.
g.equals("orders_address_repair : chaque clé existe bien dans orders_raw",
         lambda: repair().join(table("orders_raw").select("order_line_id"),
                               "order_line_id", "left_anti").count(), 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Événements

# COMMAND ----------

g.equals("events_raw : nombre de lignes", lambda: table("events_raw").count(), EXP_EVENTS_ROWS)
g.equals("events_raw : fichiers sources distincts",
         lambda: table("events_raw").select("_source_file").distinct().count(), EXP_EVENTS_FILES)
g.truthy("events_raw : user est un STRUCT",
         lambda: dtypes("events_raw").get("user", "").startswith("struct"))
g.truthy("events_raw : device est un STRUCT",
         lambda: dtypes("events_raw").get("device", "").startswith("struct"))
g.truthy("events_raw : context est un STRUCT imbriqué (utm)",
         lambda: "utm" in dtypes("events_raw").get("context", ""))
g.truthy("events_raw : items est un ARRAY",
         lambda: dtypes("events_raw").get("items", "").startswith("array"))
g.truthy("events_raw : colonnes techniques présentes",
         lambda: set(TECH_COLS).issubset(dtypes("events_raw")))
# Verifie le 03/08/2026 sur Databricks : un enregistrement JSON **illisible** ne part
# PAS dans `_rescued_data`. Il produit une ligne a champs nuls, et son texte brut est
# conserve dans `_corrupt_record`. Les deux colonnes repondent a deux questions
# differentes :
#   _rescued_data    -> la ligne est lisible, mais s'ecarte du schema (type, colonne en trop)
#   _corrupt_record  -> la ligne n'a pas pu etre analysee du tout
# On compte donc les lignes sans `event_id`, ce qui est vrai quelle que soit la colonne
# retenue par le runtime pour porter le texte brut.
g.band("events_raw : enregistrements illisibles conservés (lignes sans event_id)",
       lambda: table("events_raw").filter(F.col("event_id").isNull()).count(),
       EXP_EVENTS_MALFORMED)
# La ligne existe : encore faut-il que son contenu n'ait pas disparu avec elle.
g.truthy("events_raw : le texte brut des enregistrements illisibles est conservé",
         lambda: any(c in table("events_raw").columns
                     for c in ("_corrupt_record", "_rescued_data")),
         hint="_corrupt_record ou _rescued_data doit porter la ligne d'origine")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Référentiels

# COMMAND ----------

g.equals("ref_products_raw : lignes", lambda: table("ref_products_raw").count(), 8_000)
g.equals("ref_sellers_raw : lignes", lambda: table("ref_sellers_raw").count(), 600)
# 39, pas 42 : 8 top-categories qui portent 6+6+5+5+5+4+4+4 sous-categories.
# Valeur verifiee contre `graders/expected/W0_ref.json`, produit par le generateur.
g.equals("ref_categories_raw : lignes", lambda: table("ref_categories_raw").count(), 39)
# Un mauvais separateur donnerait le bon nombre de lignes et UNE seule colonne :
# le compte seul ne prouve rien sur ce fichier.
g.truthy("ref_categories_raw : les 4 colonnes du référentiel sont bien séparées",
         lambda: {"category_id", "category_label", "top_category_code",
                  "top_category_label"}.issubset(dtypes("ref_categories_raw")),
         hint="ce fichier n'a ni le séparateur ni les fins de ligne des commandes")
g.truthy("ref_products_raw : colonnes techniques présentes",
         lambda: set(TECH_COLS).issubset(dtypes("ref_products_raw")))
g.equals("ref_products_raw : clé product_id unique (snapshot non cumulé)",
         lambda: table("ref_products_raw").select("product_id").distinct().count(), 8_000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Journal d'exécution

# COMMAND ----------


def logged_sources():
    return {r[0] for r in spark.table(f"{CATALOG}.ops.pipeline_runs")
            .select("task_name").distinct().collect()}


g.truthy("ops.pipeline_runs : une entrée par flux",
         lambda: {"bronze_orders", "bronze_events", "bronze_ref"}.issubset(logged_sources()),
         hint="bronze_orders, bronze_events, bronze_ref")
g.equals("ops.pipeline_runs : aucune exécution en échec non traitée",
         lambda: spark.table(f"{CATALOG}.ops.pipeline_runs")
                      .filter("status NOT IN ('SUCCESS','FAILED')").count(), 0)

# COMMAND ----------

g.report()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contrôle d'idempotence (manuel)
# MAGIC
# MAGIC Le grader ne peut pas le vérifier seul : relance `M1_bronze_orders.py` et
# MAGIC `M1_bronze_events.py` **sans rien téléverser**, puis réexécute ce grader.
# MAGIC Les comptages doivent être identiques au caractère près.
# MAGIC
# MAGIC Si les lignes ont doublé, ton checkpoint n'est pas utilisé — ou tu écris en
# MAGIC `overwrite` là où il faut de l'`append`, ou l'inverse.
