# Databricks notebook source
# MAGIC %md
# MAGIC # M9 — Enquête
# MAGIC
# MAGIC > *« Bonjour, le CA de mercredi me paraît bizarre sur le tableau de bord.
# MAGIC > Tu peux vérifier ? Merci. »*
# MAGIC
# MAGIC Ce notebook est un carnet d'enquête, pas un tutoriel. Les cellules fournies sont
# MAGIC des points de départ ; c'est à toi de savoir où regarder ensuite.

# COMMAND ----------

from pyspark.sql import functions as F, Window as W
from datetime import datetime
import uuid

CATALOG = "novamarket"
RUN_ID = str(uuid.uuid4())
STARTED_AT = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Détecter
# MAGIC
# MAGIC ### 1.1 — Qu'ont dit tes contrôles ?
# MAGIC
# MAGIC Commence par là. Si la réponse est « tout est vert », c'est déjà une information.

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.ops.dq_metrics")
    .withColumn("_rank", F.dense_rank().over(W.orderBy(F.col("measured_at").desc())))
    .filter(F.col("_rank") <= 2)
    .select("measured_at", "table_name", "check_name", "metric_value", "threshold",
            "comparison", "status")
    .orderBy(F.col("measured_at").desc(), "table_name", "check_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 — La série, pas le total
# MAGIC
# MAGIC Un total ne dit rien. Une série dit tout. Regarde le chiffre d'affaires **par
# MAGIC jour** sur les trois dernières semaines.

# COMMAND ----------

# TODO A — CA net quotidien, sur les lignes de CA, pour les 21 derniers jours.
# Ajoute une moyenne mobile 7 jours et le rapport entre les deux : c'est ce rapport
# qui saute aux yeux, pas la valeur brute.


# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 — Et les autres indicateurs ?
# MAGIC
# MAGIC Le CA n'est pas le seul à avoir bougé. Compare la journée suspecte aux précédentes
# MAGIC sur : nombre de commandes, panier moyen, taux d'orphelins, volumétrie des tables de
# MAGIC référence.
# MAGIC
# MAGIC Une de ces séries a un décrochage encore plus brutal que le CA.

# COMMAND ----------

# TODO B — tableau de bord d'enquête : plusieurs indicateurs, même axe temporel


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Diagnostiquer
# MAGIC
# MAGIC ### 2.1 — Isoler
# MAGIC
# MAGIC Une anomalie de CA sur une journée se décompose toujours de la même façon : plus de
# MAGIC lignes, ou des lignes plus chères. Tranche d'abord ça, ensuite descends.
# MAGIC
# MAGIC Si ce sont les montants : est-ce diffus ou concentré ? Sur quels vendeurs, quels
# MAGIC produits, quel canal, quel fichier source ?

# COMMAND ----------

# TODO C — décomposition de l'écart


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 — Confronter au catalogue
# MAGIC
# MAGIC Tu as un référentiel qui donne le prix catalogue de chaque produit. Un prix de vente
# MAGIC légitime en est proche. Compare.

# COMMAND ----------

# TODO D — rapport entre unit_price et list_price, par jour et par vendeur


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 — Chiffrer
# MAGIC
# MAGIC Pour chaque anomalie : combien de lignes, quel impact en euros, quelle portée
# MAGIC temporelle, quelle portée fonctionnelle.
# MAGIC
# MAGIC C'est ce chiffre que tu enverras au métier. Il doit être juste.

# COMMAND ----------

# TODO E — quantification


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Contenir
# MAGIC
# MAGIC Le gold est publié et faux. Avant même de comprendre la cause, tu dois décider quoi
# MAGIC faire de la donnée que les analystes sont en train de lire.
# MAGIC
# MAGIC `DESCRIBE HISTORY` te dit à quelle version revenir. `RESTORE TABLE ... TO VERSION AS OF`
# MAGIC t'y ramène. Une vue repointée fait le même travail sans toucher aux tables.
# MAGIC
# MAGIC Choisis, applique, et note ce que tu **n'as pas** choisi et pourquoi.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {CATALOG}.gold.fact_order_line").limit(10))

# COMMAND ----------

# TODO F — mesure de confinement


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Réparer
# MAGIC
# MAGIC ### 4.1 — L'ordre compte
# MAGIC
# MAGIC Une des deux anomalies rend la seconde plus difficile à traiter tant qu'elle n'est
# MAGIC pas corrigée. Identifie-la et commence par elle.

# COMMAND ----------

# TODO G — première réparation


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 — Le motif `SUSPECTED_UNIT_SCALE`
# MAGIC
# MAGIC À ajouter aux règles de validation de M3, pas en traitement ponctuel :
# MAGIC
# MAGIC > Une ligne est suspecte si son `unit_price` nettoyé dépasse **10 fois** le
# MAGIC > `list_price` du produit au catalogue. La règle ne s'applique qu'aux produits
# MAGIC > présents dans le référentiel.
# MAGIC
# MAGIC Attention à ne pas casser les motifs existants : les comptages de M3 doivent rester
# MAGIC exacts sur les lignes antérieures.

# COMMAND ----------

# TODO H — nouvelle règle dans le silver, puis recalcul complet


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 — Rejouer l'aval
# MAGIC
# MAGIC Silver a changé : le gold doit suivre. C'est le moment de vérifier que ton pipeline
# MAGIC est bien idempotent — un simple relancement doit suffire.

# COMMAND ----------

# TODO I — recalcul du gold, puis contrôle


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Prévenir

# COMMAND ----------

# TODO J — ajoute daily_revenue_anomaly et reference_volume_drop à ton moteur de M6.
#
# Le second doit détecter ce que ton garde-fou de M6 a laissé passer. Regarde ce
# que tu avais écrit comme seuil avant d'écrire le nouveau.


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `ops.incident_log`

# COMMAND ----------

# TODO K — crée la table (schéma dans le README) et documente chaque anomalie.
# Une ligne par anomalie distincte.


# COMMAND ----------

display(spark.table(f"{CATALOG}.ops.incident_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Post-mortem
# MAGIC
# MAGIC **1. Délai entre l'arrivée de la donnée fausse et sa détection ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **2. Pourquoi ton garde-fou sur le référentiel est-il resté vert ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **3. Quelle famille de contrôles manque à ton dispositif ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **4. Quarantiner 55 commandes réelles : bon arbitrage ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **5. Le post-mortem en cinq lignes, pour un non-spécialiste**
# MAGIC
# MAGIC > …
