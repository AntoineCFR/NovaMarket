# Databricks notebook source
# MAGIC %md
# MAGIC # Corrigé — M13 : méthodes d'ingestion

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

CATALOG = "novamarket"
SOURCE_PATH = f"/Volumes/{CATALOG}/landing/files/orders"
TARGET = f"{CATALOG}.bronze.orders_copyinto"

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO A — le journal

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.ops.ingestion_comparison (
        method       STRING    COMMENT 'COPY_INTO ou AUTO_LOADER',
        target_table STRING,
        run_number   INT       COMMENT '1 puis 2',
        rows_after   BIGINT    COMMENT 'Lignes en cible apres execution',
        rows_added   BIGINT    COMMENT 'Lignes ajoutees par cette execution',
        notes        STRING,
        measured_at  TIMESTAMP
    )
    COMMENT 'Comparaison des mecanismes d ingestion sur les memes fichiers.'
""")


def record(method, target_table, run_number, rows_after, rows_added, notes):
    spark.createDataFrame(
        [(method, target_table, int(run_number), int(rows_after), int(rows_added),
          notes, datetime.now())],
        "method string, target_table string, run_number int, rows_after bigint, "
        "rows_added bigint, notes string, measured_at timestamp",
    ).write.mode("append").saveAsTable(f"{CATALOG}.ops.ingestion_comparison")
    print(f"{method:12s} run {run_number} : {rows_after:>8,} lignes "
          f"(+{rows_added:,})".replace(",", " "))


# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO B — la table cible
# MAGIC
# MAGIC Créée sans colonnes : la première exécution les découvre. C'est possible **parce
# MAGIC que** `mergeSchema` est activé côté `COPY_OPTIONS`.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
spark.sql(f"CREATE TABLE {TARGET} "
          f"COMMENT 'Meme source que bronze.orders_raw, chargee par COPY INTO.'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO C, D, E — les deux exécutions
# MAGIC
# MAGIC Noter la séparation des deux blocs d'options : `FORMAT_OPTIONS` décrit **comment
# MAGIC lire le fichier**, `COPY_OPTIONS` décrit **comment se comporter vis-à-vis de la
# MAGIC cible**. La fusion de schéma est une décision sur la cible, donc dans le second.

# COMMAND ----------

COPY_SQL = f"""
    COPY INTO {TARGET}
    FROM '{SOURCE_PATH}'
    FILEFORMAT = CSV
    FORMAT_OPTIONS (
        'header' = 'true',
        'sep' = ';',
        'encoding' = 'windows-1252',
        'inferSchema' = 'false',
        'rescuedDataColumn' = '_rescued_data'
    )
    COPY_OPTIONS ('mergeSchema' = 'true')
"""

for run in (1, 2):
    before = spark.table(TARGET).count() if run > 1 else 0
    result = spark.sql(COPY_SQL)
    display(result)                       # COPY INTO renvoie fichiers et lignes traites
    after = spark.table(TARGET).count()
    record("COPY_INTO", TARGET, run, after, after - before,
           "premiere charge" if run == 1 else "relance sans nouveau fichier")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO F — le pendant Auto Loader
# MAGIC
# MAGIC Relance le notebook de M1 sans rien téléverser entre les deux passages, puis
# MAGIC consigne les deux mesures ici.

# COMMAND ----------

n = spark.table(f"{CATALOG}.bronze.orders_raw").count()
record("AUTO_LOADER", f"{CATALOG}.bronze.orders_raw", 1, n, n, "charge initiale (M1)")
record("AUTO_LOADER", f"{CATALOG}.bronze.orders_raw", 2, n, 0,
       "relance sans nouveau fichier : le checkpoint fait son travail")

display(spark.table(f"{CATALOG}.ops.ingestion_comparison").orderBy("method", "run_number"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Le tableau
# MAGIC
# MAGIC | | `COPY INTO` | Auto Loader |
# MAGIC |---|---|---|
# MAGIC | Comment il sait ce qu'il a déjà lu | Il consigne les fichiers chargés dans les **métadonnées de la table cible** | Un **checkpoint** qui mémorise les fichiers vus |
# MAGIC | Où vit cet état | Dans le journal Delta de la table, avec les données | Dans un répertoire que **tu** désignes, séparé de la table |
# MAGIC | Si on le perd | Impossible sans perdre la table elle-même | La relance **recharge tout** et duplique |
# MAGIC | Millions de fichiers | Se dégrade : il liste le répertoire à chaque exécution | Conçu pour, avec état incrémental et notification de fichier |
# MAGIC | Évolution de schéma | `mergeSchema` : ajoute les colonnes nouvelles | Plusieurs modes, dont le sauvetage et l'échec contrôlé |
# MAGIC | Streaming | Non — c'est une commande batch | Oui, c'est une source de streaming |
# MAGIC
# MAGIC La troisième ligne est la plus intéressante et la moins connue. L'état de
# MAGIC `COPY INTO` étant **dans la table**, il est sauvegardé, répliqué et restauré avec
# MAGIC elle. Celui d'Auto Loader est un répertoire à part, que rien n'oblige à
# MAGIC sauvegarder — et un `RESTORE` de la table ne le remet pas dans l'état correspondant.
# MAGIC
# MAGIC ---
# MAGIC ## Réponses
# MAGIC
# MAGIC **1. Quand choisir `COPY INTO` alors qu'Auto Loader est disponible ?**
# MAGIC
# MAGIC > Trois situations concrètes :
# MAGIC >
# MAGIC > - **Un chargement ponctuel.** Reprise d'historique, migration, correction d'un
# MAGIC >   lot. Trois lignes de SQL, aucun état à créer ni à nettoyer ensuite.
# MAGIC > - **Un lot quotidien de quelques fichiers.** Auto Loader fonctionnerait, mais on
# MAGIC >   paie un checkpoint à gérer pour un bénéfice nul.
# MAGIC > - **Une équipe qui travaille en SQL.** `COPY INTO` est du SQL pur, il tient dans
# MAGIC >   une tâche de requête SQL d'un job. Auto Loader impose un notebook et du PySpark.
# MAGIC >
# MAGIC > Le critère qui résume les trois : **le nombre de fichiers et la fréquence**, pas
# MAGIC > le volume de données.
# MAGIC
# MAGIC **2. Perdre l'état de chacun**
# MAGIC
# MAGIC > **Auto Loader** : supprimer le checkpoint et relancer recharge **tous** les
# MAGIC > fichiers. En écriture `append`, on double la table. C'est exactement la cellule de
# MAGIC > réinitialisation de M1, et c'est pour ça qu'elle est commentée.
# MAGIC >
# MAGIC > **`COPY INTO`** : son historique est dans les métadonnées de la table cible, donc
# MAGIC > on ne peut pas le supprimer sans supprimer la table. Il n'y a pas de fichier
# MAGIC > d'état à égarer.
# MAGIC >
# MAGIC > Nuance qui fait une bonne question d'examen : `TRUNCATE` sur la cible ne remet
# MAGIC > **pas** le compteur à zéro. La table est vide, `COPY INTO` considère toujours les
# MAGIC > fichiers comme chargés, et une relance ne remet rien. Pour vraiment tout
# MAGIC > recharger, il faut `COPY_OPTIONS ('force' = 'true')`, ou supprimer et recréer.
# MAGIC >
# MAGIC > Cette asymétrie est un vrai argument de conception : `COPY INTO` a un état plus
# MAGIC > difficile à corrompre, Auto Loader un état plus facile à manipuler délibérément.
# MAGIC
# MAGIC **3. Un million de fichiers**
# MAGIC
# MAGIC > `COPY INTO` se dégrade en premier, et nettement. À chaque exécution, il doit
# MAGIC > **lister le répertoire** pour savoir ce qui est nouveau, puis comparer à son
# MAGIC > historique. Le listage de stockage objet coûte cher et croît avec le nombre de
# MAGIC > fichiers : le coût de la commande devient proportionnel à l'historique total,
# MAGIC > pas au delta.
# MAGIC >
# MAGIC > Auto Loader tient un état incrémental, et surtout il propose le mode **notification
# MAGIC > de fichier** : le stockage prévient qu'un fichier est arrivé, plus personne ne
# MAGIC > liste quoi que ce soit. Le coût devient proportionnel au **delta**, ce qui est la
# MAGIC > seule façon de tenir à cette échelle.
# MAGIC >
# MAGIC > Le mode notification n'est pas disponible en Free Edition — il exige des
# MAGIC > ressources dans ton compte cloud. Le mode par défaut, *directory listing*, est
# MAGIC > celui qu'on a utilisé en M1.
# MAGIC
# MAGIC **4. Dépôts irréguliers, trois fois par jour**
# MAGIC
# MAGIC > Je choisis le **déclencheur d'arrivée de fichier**. Un déclencheur temporel devrait
# MAGIC > tourner assez souvent pour ne pas faire attendre, donc tournerait à vide la
# MAGIC > plupart du temps — et sur Free Edition, tourner à vide consomme le quota.
# MAGIC >
# MAGIC > **Le choix inverse, défendu honnêtement** : le déclencheur temporel est
# MAGIC > **prévisible**, et c'est une qualité qu'on sous-estime. On sait quand le pipeline
# MAGIC > tourne, donc quand les tableaux de bord bougent, donc quand intervenir sans gêner.
# MAGIC > Il traite aussi les rafales naturellement : trois fichiers déposés en dix minutes
# MAGIC > donnent une exécution, pas trois. Et si le partenaire dépose un fichier corrompu,
# MAGIC > on a jusqu'à la prochaine fenêtre pour l'intercepter.
# MAGIC >
# MAGIC > Le vrai discriminant est la **tolérance à la latence**. Trois heures de retard
# MAGIC > acceptables → temporel, plus simple à exploiter. Quinze minutes exigées →
# MAGIC > arrivée de fichier, avec le nombre d'exécutions concurrentes plafonné à 1 pour ne
# MAGIC > pas déclencher une cascade.
# MAGIC
# MAGIC **5. JDBC scripté ou connecteur managé**
# MAGIC
# MAGIC > Un seul critère domine, et ce n'est pas le coût de développement : **qui maintient
# MAGIC > l'extraction dans trois ans**.
# MAGIC >
# MAGIC > Un script JDBC Salesforce demande de gérer la pagination, les limites de débit,
# MAGIC > la rotation des jetons, les changements de schéma côté source, la reprise après
# MAGIC > incident et la détection des suppressions. M2 a montré ce que coûte le dernier
# MAGIC > point : le piège de bordure du watermark a fait perdre cinq lignes **sans aucun
# MAGIC > signal**. Ce bug-là n'existe pas avec un connecteur managé, qui tient son propre
# MAGIC > état de progression.
# MAGIC >
# MAGIC > Je prends le connecteur managé sauf si : le connecteur n'existe pas, il ne couvre
# MAGIC > pas les objets nécessaires, ou il faut transformer à l'ingestion — un connecteur
# MAGIC > réplique, il ne transforme pas.
# MAGIC >
# MAGIC > Le critère qu'on met en avant et qui compte le moins : « on sait le faire nous-mêmes ».
# MAGIC > On sait toujours. La question est de savoir si on veut en être responsable.
