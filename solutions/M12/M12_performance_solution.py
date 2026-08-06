# Databricks notebook source
# MAGIC %md
# MAGIC # Corrigé — M12 : performance, monitoring et optimisation

# COMMAND ----------

from pyspark.sql import functions as F, Window as W
from datetime import datetime
import time

CATALOG = "novamarket"
HOT_SELLER = "S0001"
AMPLIFICATION = 400

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO A — mesurer honnêtement
# MAGIC
# MAGIC Trois pièges dans une fonction de six lignes :
# MAGIC
# MAGIC - `count()` seul ne force pas toujours le calcul complet : le moteur sait compter
# MAGIC   sans matérialiser toutes les colonnes. `foreach` traverse réellement les lignes.
# MAGIC - Le cache d'une variante fausse la suivante — d'où le `clearCache()`.
# MAGIC - La première exécution paie la compilation du plan. En toute rigueur on mesure
# MAGIC   deux fois et on garde la seconde ; ici on s'en tient à une passe, en le sachant.

# COMMAND ----------


def measure(df):
    spark.catalog.clearCache()
    start = time.perf_counter()
    rows = df.count()
    df.foreach(lambda _: None)          # force la traversée reelle des lignes
    duration_ms = int((time.perf_counter() - start) * 1000)
    return duration_ms, rows


spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.ops.perf_measurements (
        scenario    STRING    COMMENT 'Ce qu on compare',
        variant     STRING    COMMENT 'La variante mesuree',
        config      STRING    COMMENT 'Le reglage applique',
        duration_ms BIGINT    COMMENT 'Duree mesuree',
        rows_out    BIGINT    COMMENT 'Lignes produites — doit etre identique entre variantes',
        measured_at TIMESTAMP
    )
    COMMENT 'Journal des mesures de performance. Aucune affirmation sans mesure.'
""")


def record(scenario, variant, config, duration_ms, rows_out):
    spark.createDataFrame(
        [(scenario, variant, config, int(duration_ms), int(rows_out), datetime.now())],
        "scenario string, variant string, config string, duration_ms bigint, "
        "rows_out bigint, measured_at timestamp",
    ).write.mode("append").saveAsTable(f"{CATALOG}.ops.perf_measurements")
    print(f"{scenario:22s} {variant:18s} {duration_ms:>7} ms  {rows_out:>10,} lignes"
          .replace(",", " "))


# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO C — fabriquer le déséquilibre

# COMMAND ----------

order_line = spark.table(f"{CATALOG}.silver.order_line")

hot = (order_line.filter(F.col("seller_id") == HOT_SELLER)
       .withColumn("_copy", F.explode(F.sequence(F.lit(1), F.lit(AMPLIFICATION))))
       .drop("_copy"))

(order_line.unionByName(hot)
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.ops.skew_demo"))

share = spark.sql(f"""
    SELECT round(max(n) / sum(n), 4) FROM (
        SELECT seller_id, count(*) AS n FROM {CATALOG}.ops.skew_demo GROUP BY seller_id)
""").first()[0]
print(f"part de la cle la plus lourde : {share:.1%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO D et E — la jointure et ses trois variantes
# MAGIC
# MAGIC L'agrégation après jointure force un *shuffle* : c'est là que le déséquilibre se
# MAGIC voit. Sans agrégation, une jointure diffusée ne mélange rien et le skew reste
# MAGIC invisible.

# COMMAND ----------


def skew_join():
    sellers = spark.table(f"{CATALOG}.gold.dim_seller").filter("is_current")
    return (spark.table(f"{CATALOG}.ops.skew_demo").alias("o")
            .join(sellers.alias("d"), "seller_id")
            .groupBy("seller_id", "main_top_category")
            .agg(F.sum("net_amount").alias("ca"), F.count("*").alias("n")))


VARIANTS = [
    ("defaut", None, None),
    ("shuffle_join", "spark.sql.autoBroadcastJoinThreshold", "-1"),
    ("broadcast_join", "spark.sql.autoBroadcastJoinThreshold", str(100 * 1024 * 1024)),
]

original = spark.conf.get("spark.sql.autoBroadcastJoinThreshold", None)

for variant, key, value in VARIANTS:
    config = "par defaut"
    if key:
        try:
            spark.conf.set(key, value)
            config = f"{key}={value}"
        except Exception as exc:
            config = f"{key} REFUSE ({type(exc).__name__})"
            print(f"  -> {config}")
    duration, rows = measure(skew_join())
    record("jointure_desequilibree", variant, config, duration, rows)

if original is not None:
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", original)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO F et G — liquid clustering
# MAGIC
# MAGIC Deux tables plutôt qu'une mesure avant/après : le cache du premier passage
# MAGIC fausserait le second, et on conclurait à un gain qui n'est qu'un effet de cache.

# COMMAND ----------

fact = spark.table(f"{CATALOG}.gold.fact_order_line")

(fact.write.mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(f"{CATALOG}.ops.perf_baseline"))

spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.ops.perf_clustered")
spark.sql(f"""
    CREATE TABLE {CATALOG}.ops.perf_clustered
    CLUSTER BY (order_date, seller_id)
    AS SELECT * FROM {CATALOG}.gold.fact_order_line
""")
spark.sql(f"OPTIMIZE {CATALOG}.ops.perf_clustered")

for table in ["perf_baseline", "perf_clustered"]:
    d = spark.sql(f"DESCRIBE DETAIL {CATALOG}.ops.{table}").first()
    print(f"{table:16s} {d['numFiles']:>4} fichier(s)  regroupement : {d['clusteringColumns']}")

# COMMAND ----------


def filtered(table):
    return (spark.table(f"{CATALOG}.ops.{table}")
            .filter((F.col("order_date").between("2026-03-01", "2026-03-31"))
                    & (F.col("seller_id").isin("S0042", "S0100", "S0250")))
            .select("order_line_id", "net_amount"))


for table, variant in [("perf_baseline", "sans_regroupement"),
                       ("perf_clustered", "avec_regroupement")]:
    duration, rows = measure(filtered(table))
    record("clustering_liquide", variant, f"table={table}", duration, rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO H — tendances de jobs

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.ops.job_perf_trend AS
    WITH d AS (
        SELECT task_name, started_at,
               timestampdiff(MILLISECOND, started_at, ended_at) AS duration_ms
        FROM {CATALOG}.ops.pipeline_runs
        WHERE ended_at IS NOT NULL
    ),
    ranked AS (
        SELECT task_name, duration_ms,
               row_number() OVER (PARTITION BY task_name ORDER BY started_at DESC) AS rn,
               avg(duration_ms) OVER (
                   PARTITION BY task_name ORDER BY started_at
                   ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS avg_before
        FROM d
    )
    SELECT
        task_name,
        cast(duration_ms AS BIGINT)                      AS last_duration_ms,
        cast(coalesce(avg_before, duration_ms) AS BIGINT) AS avg_duration_ms,
        round(duration_ms / nullif(coalesce(avg_before, duration_ms), 0), 3) AS deviation
    FROM ranked WHERE rn = 1
""")

display(spark.table(f"{CATALOG}.ops.job_perf_trend").orderBy(F.col("deviation").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Réponses
# MAGIC
# MAGIC **1. L'AQE a-t-elle corrigé le skew toute seule ?**
# MAGIC
# MAGIC > Oui, en grande partie — et c'est la réponse de la question 1 de l'examen blanc du
# MAGIC > guide officiel. L'*adaptive query execution* détecte les partitions anormalement
# MAGIC > grosses **à l'exécution** et les découpe. On le vérifie dans le plan : chercher
# MAGIC > `AQEShuffleRead` avec la mention d'un traitement de skew, ou comparer le nombre
# MAGIC > de partitions annoncé au plan initial et celui réellement exécuté.
# MAGIC >
# MAGIC > La conséquence pratique compte plus que le mécanisme : **le premier réflexe face
# MAGIC > à un skew n'est plus de saler les clés à la main**. On vérifie d'abord que l'AQE
# MAGIC > est active et qu'elle a fait son travail. Le salage reste utile quand le
# MAGIC > déséquilibre est extrême, ou dans les cas que l'AQE ne couvre pas.
# MAGIC >
# MAGIC > Et ce qu'elle ne corrige pas : le déséquilibre en **écriture**. Une partition de
# MAGIC > sortie énorme reste un fichier énorme.
# MAGIC
# MAGIC **2. Effet de `autoBroadcastJoinThreshold = -1`**
# MAGIC
# MAGIC > Interdire le *broadcast* force une jointure par *shuffle* : les deux côtés sont
# MAGIC > redistribués par clé sur le réseau. Sur cette jointure — une grosse table de faits
# MAGIC > contre une dimension de 665 lignes — c'est un pur gaspillage, et la mesure le
# MAGIC > montre.
# MAGIC >
# MAGIC > Le *broadcast* devient une mauvaise idée quand la table diffusée ne tient plus
# MAGIC > confortablement en mémoire **sur chaque exécuteur**, puisqu'elle y est copiée
# MAGIC > intégralement. Le seuil par défaut est de l'ordre de 10 Mo, ce qui est prudent ;
# MAGIC > on monte couramment à 100 Mo. Au-delà, le risque bascule : on troque un *shuffle*
# MAGIC > coûteux contre une saturation mémoire des exécuteurs, qui est bien pire — un
# MAGIC > *shuffle* est lent, un OOM fait tout échouer.
# MAGIC >
# MAGIC > Le piège à connaître : le seuil s'applique à la taille **estimée**, et
# MAGIC > l'estimation peut être très fausse sur une table sans statistiques ou après un
# MAGIC > filtre sélectif.
# MAGIC
# MAGIC **3. Ce que le serverless refuse**
# MAGIC
# MAGIC > À relever soi-même — le comportement évolue. Ce qui est structurel : tout ce qui
# MAGIC > touche au **dimensionnement physique** (`spark.executor.memory`,
# MAGIC > `spark.driver.memory`, nombre d'exécuteurs) n'a pas de sens sur un compute où l'on
# MAGIC > ne possède pas les machines. Ce qui touche au **plan logique**
# MAGIC > (`autoBroadcastJoinThreshold`, `shuffle.partitions`) reste généralement réglable.
# MAGIC >
# MAGIC > Ce que ça dit du modèle : le serverless échange du contrôle contre de la
# MAGIC > simplicité. On n'a plus à dimensionner, et on ne peut plus mal dimensionner. Pour
# MAGIC > l'immense majorité des charges, c'est un bon échange.
# MAGIC >
# MAGIC > Les cas où l'on choisit encore un compute configurable : besoin d'une
# MAGIC > bibliothèque native à installer sur les nœuds, contrainte réseau imposant un
# MAGIC > sous-réseau précis, matériel spécifique (GPU), ou charge stable et longue où un
# MAGIC > cluster réservé revient moins cher. Ce sont des cas réels, mais minoritaires.
# MAGIC
# MAGIC **4. Le gain du clustering liquide**
# MAGIC
# MAGIC > Sur 285 000 lignes, il sera faible et peut-être nul — la table tient dans quelques
# MAGIC > fichiers, et l'élagage n'a presque rien à élaguer.
# MAGIC >
# MAGIC > **Ce n'est pas une preuve d'inutilité, et il faut se retenir de conclure.** Le
# MAGIC > gain du clustering est proportionnel au nombre de fichiers qu'on évite de lire.
# MAGIC > Sur une table de quelques fichiers, il n'y a rien à gagner ; sur une table de
# MAGIC > 50 000 fichiers avec un filtre sélectif, on divise le volume lu par plusieurs
# MAGIC > ordres de grandeur.
# MAGIC >
# MAGIC > La façon honnête de trancher : ne pas regarder la durée mais **le nombre de
# MAGIC > fichiers lus**, dans le *query profile*. Si le ratio fichiers lus / fichiers
# MAGIC > totaux baisse, le mécanisme fonctionne — même si l'horloge ne bouge pas, parce
# MAGIC > qu'à cette échelle tout est dominé par les coûts fixes de démarrage.
# MAGIC >
# MAGIC > C'est le piège classique du benchmark sur jeu de test : mesurer un mécanisme sur
# MAGIC > un volume où il ne peut rien produire, et en conclure qu'il ne sert à rien.
# MAGIC
# MAGIC **5. Plus de compute, meilleur plan, moins de données**
# MAGIC
# MAGIC > Par rapport coût/bénéfice décroissant :
# MAGIC >
# MAGIC > 1. **Moins de données.** Filtrer plus tôt, élaguer des partitions, ne lire que les
# MAGIC >    colonnes utiles. Gain souvent d'un ordre de grandeur, coût nul, et le bénéfice
# MAGIC >    est permanent.
# MAGIC > 2. **Meilleur plan.** Corriger une jointure, un déséquilibre, un `collect()`
# MAGIC >    inutile. Gain important, coût = du temps d'ingénieur, bénéfice permanent.
# MAGIC > 3. **Plus de compute.** Gain au mieux linéaire, souvent sous-linéaire à cause du
# MAGIC >    *shuffle*, et coût récurrent **à chaque exécution, pour toujours**.
# MAGIC >
# MAGIC > Et pourtant c'est le troisième qu'on essaie en premier, presque toujours. Pour
# MAGIC > une raison parfaitement rationnelle à court terme : c'est le seul qui se fait en
# MAGIC > trente secondes sans comprendre le problème. Les deux autres exigent d'avoir lu
# MAGIC > le plan d'exécution.
# MAGIC >
# MAGIC > Le coût réel de ce réflexe n'est pas la facture : c'est qu'**il masque le
# MAGIC > problème**. Une requête mal écrite qui tourne sur un gros cluster reste mal
# MAGIC > écrite, et elle redeviendra lente quand le volume aura doublé — sauf qu'on aura
# MAGIC > alors doublé le cluster aussi.
