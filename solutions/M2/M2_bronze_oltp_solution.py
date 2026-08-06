# Databricks notebook source
# MAGIC %md
# MAGIC # Corrigé — M2 : ingestion de la source applicative

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

dbutils.widgets.dropdown("source_mode", "file", ["file", "lakebase"], "Voie d'accès")
dbutils.widgets.dropdown("file_version", "v1", ["v1", "v2"], "Version des fichiers")

SOURCE_MODE = dbutils.widgets.get("source_mode")
FILE_VERSION = dbutils.widgets.get("file_version")

CATALOG = "novamarket"
OLTP_PATH = f"/Volumes/{CATALOG}/landing/files/oltp"
LAKEBASE_CATALOG, LAKEBASE_SCHEMA = "novamarket_oltp", "public"

BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO A — la table de watermarks

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.ops.ingest_watermarks (
        source_name      STRING    COMMENT 'Source logique extraite',
        watermark_column STRING    COMMENT 'Colonne portant le watermark',
        watermark_value  TIMESTAMP COMMENT 'Valeur maximale deja ingeree',
        updated_at       TIMESTAMP COMMENT 'Derniere avancee du watermark'
    )
    COMMENT 'Etat des extractions incrementales'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO B — lecture et écriture du watermark
# MAGIC
# MAGIC `get_watermark` renvoie `None` sur une source jamais extraite : c'est ce `None` qui
# MAGIC distingue la charge initiale d'un delta, sans avoir besoin d'un drapeau séparé.

# COMMAND ----------


def get_watermark(source_name: str):
    row = (spark.table(f"{CATALOG}.ops.ingest_watermarks")
           .filter(F.col("source_name") == source_name)
           .select("watermark_value")
           .first())
    return row[0] if row else None


def set_watermark(source_name: str, column: str, value):
    (spark.createDataFrame(
        [(source_name, column, value, datetime.now())],
        "source_name string, watermark_column string, watermark_value timestamp, updated_at timestamp")
     .createOrReplaceTempView("_new_wm"))

    spark.sql(f"""
        MERGE INTO {CATALOG}.ops.ingest_watermarks t
        USING _new_wm s ON t.source_name = s.source_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Accès à la source

# COMMAND ----------


def read_source(table: str):
    if SOURCE_MODE == "lakebase":
        return spark.table(f"{LAKEBASE_CATALOG}.{LAKEBASE_SCHEMA}.{table}")
    suffix = "" if FILE_VERSION == "v1" else "_v2"
    return (spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .load(f"{OLTP_PATH}/{table}{suffix}.csv"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO C — typage
# MAGIC
# MAGIC `cast` sur une colonne déjà du bon type est un no-op : la même fonction convient
# MAGIC donc aux deux voies, sans branchement.

# COMMAND ----------


def typed_customers(df):
    return df.select(
        F.col("customer_id").cast("string"),
        F.col("first_name").cast("string"),
        F.col("last_name").cast("string"),
        F.col("email").cast("string"),
        F.col("country").cast("string"),
        F.col("city").cast("string"),
        F.col("zip_code").cast("string"),
        F.col("segment").cast("string"),
        F.col("is_opt_in").cast("boolean"),
        F.col("is_deleted").cast("boolean"),
        F.col("created_at").cast("timestamp"),
        F.col("updated_at").cast("timestamp"),
    )


def typed_sellers(df):
    return df.select(
        F.col("seller_id").cast("string"),
        F.col("seller_name").cast("string"),
        F.col("seller_country").cast("string"),
        F.col("seller_city").cast("string"),
        F.col("main_top_category").cast("string"),
        F.col("plan_code").cast("string"),
        F.col("is_active").cast("boolean"),
        F.col("onboarded_at").cast("date"),
        F.col("updated_at").cast("timestamp"),
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO D — l'extraction incrémentale
# MAGIC
# MAGIC Points d'implémentation qui comptent :
# MAGIC
# MAGIC - le filtre est `>=`, pas `>` (voir les réponses en fin de notebook) ;
# MAGIC - le nouveau watermark est calculé sur **ce qui a été écrit**, jamais sur la table
# MAGIC   source : sinon on avancerait au-delà de ce qu'on a réellement ingéré. Et comme un
# MAGIC   DataFrame est un plan et non de la donnée, « le delta » n'est pas non plus une
# MAGIC   base fiable dès que la source est vivante — d'où la lecture en retour ;
# MAGIC - un delta vide ne fait rien avancer et n'écrit rien ;
# MAGIC - le watermark est écrit **après** l'écriture des données. Si le job meurt entre
# MAGIC   les deux, la prochaine exécution ré-extrait le même delta et le ré-ajoute au
# MAGIC   journal : on obtient des doublons, ce qui est rattrapable en aval. Dans l'ordre
# MAGIC   inverse, on obtiendrait un trou définitif dans les données.

# COMMAND ----------


def ingest(source_name: str, table: str, key: str, target: str, typer):
    watermark = get_watermark(source_name)
    df = typer(read_source(table))

    if watermark is not None:
        df = df.filter(F.col("updated_at") >= F.lit(watermark))

    delta = (df
             .withColumn("_extracted_at", F.current_timestamp())
             .withColumn("_ingest_batch_id", F.lit(BATCH_ID))
             .withColumn("_source_system", F.lit(SOURCE_MODE)))

    # `delta` est un PLAN, pas de la donnee : chaque action le rejoue depuis la source.
    # Un `.cache()` figerait le resultat, mais PERSIST est refuse sur serverless
    # (NOT_SUPPORTED_WITH_SERVERLESS). Avec un count + un write + un agg, on emettrait
    # donc trois requetes a trois instants differents — et sur une source vivante
    # (voie Lakebase), l'agg pourrait voir une ligne que le write n'a pas ecrite :
    # le watermark depasserait alors la donnee reellement ingeree, ce qui creuse un
    # trou definitif. Une seule action sur la source, donc : l'ecriture. Le compte et
    # le nouveau watermark se lisent EN RETOUR sur ce qui vient d'etre commite, ce qui
    # rend litterale la regle « le watermark se calcule sur ce qui a ete ecrit ».
    delta.write.mode("append").option("mergeSchema", "false").saveAsTable(target)

    written = (spark.table(target)
               .filter(F.col("_ingest_batch_id") == BATCH_ID)
               .agg(F.count("*").alias("n"), F.max("updated_at").alias("wm"))
               .first())
    n, new_wm = written["n"], written["wm"]

    if n == 0:
        print(f"{source_name} : rien de neuf, watermark inchangé ({watermark})")
        return 0, watermark

    set_watermark(source_name, "updated_at", new_wm)
    return n, new_wm


# COMMAND ----------

n_customers, wm_customers = ingest("app_customers", "app_customers", "customer_id",
                                   f"{CATALOG}.bronze.app_customers_raw", typed_customers)
n_sellers, wm_sellers = ingest("app_sellers", "app_sellers", "seller_id",
                               f"{CATALOG}.bronze.app_sellers_raw", typed_sellers)

print(f"clients  : {n_customers:>6} ligne(s), watermark -> {wm_customers}")
print(f"vendeurs : {n_sellers:>6} ligne(s), watermark -> {wm_sellers}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Réponses
# MAGIC
# MAGIC **1. Combien de lignes en `>` ? En `>=` ?**
# MAGIC
# MAGIC > Watermark clients après la charge initiale : `2026-04-30 09:00:00`.
# MAGIC >
# MAGIC > | Filtre | Clients | Vendeurs |
# MAGIC > |---|---|---|
# MAGIC > | `updated_at > watermark` | 385 | 40 |
# MAGIC > | `updated_at >= watermark` | 411 | 41 |
# MAGIC >
# MAGIC > L'écart de 26 lignes se décompose en 21 lignes déjà vues et inchangées, plus
# MAGIC > **5 lignes réellement modifiées** que le `>` strict ne verra jamais.
# MAGIC
# MAGIC **2. Coût de chaque erreur ?**
# MAGIC
# MAGIC > `>` strict : on perd silencieusement toute ligne validée à un horodatage
# MAGIC > inférieur ou égal au watermark après notre lecture. Ce n'est pas un cas d'école —
# MAGIC > c'est le comportement normal d'une transaction ouverte avant l'extraction et
# MAGIC > validée après. La perte est **définitive** : aucune exécution ultérieure ne
# MAGIC > repassera sur cette plage. Et rien ne l'annonce : la table a l'air saine.
# MAGIC >
# MAGIC > `>=` : on ré-extrait les lignes déjà vues qui portent exactement l'horodatage du
# MAGIC > watermark. Ici, 21 lignes inutiles. Le coût est une redondance bornée et visible.
# MAGIC >
# MAGIC > Une perte invisible contre une redondance visible : l'arbitrage n'est pas
# MAGIC > symétrique. En production on va souvent plus loin en reculant volontairement le
# MAGIC > watermark d'une marge de sécurité (`watermark - 5 minutes`) pour couvrir les
# MAGIC > transactions longues et les décalages d'horloge entre serveurs.
# MAGIC
# MAGIC **3. En quoi le journal append-only change-t-il l'arbitrage ?**
# MAGIC
# MAGIC > Il le rend évident. Puisque bronze empile les versions observées sans arbitrer,
# MAGIC > ré-ingérer une ligne identique ne corrompt rien : elle ajoute une version de plus,
# MAGIC > que la déduplication de silver éliminera de toute façon en ne gardant que la
# MAGIC > dernière version par clé. Le surcoût du `>=` est donc quasi nul.
# MAGIC >
# MAGIC > Si bronze faisait un `MERGE` sur la clé, le raisonnement serait le même mais le
# MAGIC > débat deviendrait plus délicat : on écraserait une version par une version
# MAGIC > identique, et on perdrait la trace du fait qu'on l'a revue.
# MAGIC
# MAGIC **4. Quel cas un watermark sur `updated_at` ne détecte-t-il pas ?**
# MAGIC
# MAGIC > La **suppression physique** (`DELETE FROM app_customers WHERE ...`). Une ligne
# MAGIC > effacée n'a plus d'`updated_at` à comparer : elle ne remonte dans aucun delta, et
# MAGIC > bronze la conserve indéfiniment comme si elle existait encore.
# MAGIC >
# MAGIC > Ici la source nous sauve en pratiquant la **suppression douce** : `is_deleted`
# MAGIC > passe à `true` et `updated_at` est bumpé, donc la suppression devient un
# MAGIC > changement comme un autre. C'est une propriété de la source, pas de notre pipeline.
# MAGIC >
# MAGIC > Sans elle, les options sont : un CDC natif branché sur le journal de transactions,
# MAGIC > un trigger applicatif alimentant une table d'audit, ou une réconciliation
# MAGIC > périodique complète des clés (coûteuse mais simple). C'est exactement le genre
# MAGIC > d'hypothèse qu'il faut écrire noir sur blanc dans un contrat d'interface — et
# MAGIC > `docs/02-sources-et-modele.md` ne le fait pas. Deuxième entrée pour ton registre
# MAGIC > d'écarts au contrat.
