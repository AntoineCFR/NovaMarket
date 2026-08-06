# Databricks notebook source
# MAGIC %md
# MAGIC # Corrigé — M9 : capstone

# COMMAND ----------

from pyspark.sql import functions as F, Window as W
from datetime import datetime
import uuid

CATALOG = "novamarket"
RUN_ID = str(uuid.uuid4())
MEASURED_AT = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO A — la série quotidienne
# MAGIC
# MAGIC Le total du mois ne dit rien. La série dit tout — à condition de regarder le
# MAGIC **rapport** à la tendance et pas la valeur brute, sinon la saisonnalité noie le
# MAGIC signal.

# COMMAND ----------

w7 = W.orderBy("order_date").rowsBetween(-7, -1)

daily = (
    spark.table(f"{CATALOG}.silver.order_line")
    .filter("is_revenue")
    .groupBy("order_date")
    .agg(F.sum("net_amount").alias("ca"),
         F.countDistinct("order_id").alias("commandes"),
         F.count("*").alias("lignes"))
    .withColumn("moyenne_7j", F.avg("ca").over(w7))
    .withColumn("ratio", F.round(F.col("ca") / F.col("moyenne_7j"), 2))
    .orderBy(F.col("order_date").desc())
)

display(daily.limit(21))

# MAGIC %md
# MAGIC Le 2026-06-04 sort à **4,4 fois** la tendance, alors que le nombre de commandes et
# MAGIC de lignes est parfaitement normal. Plus d'argent pour autant de commandes : ce ne
# MAGIC sont pas les volumes, ce sont les montants.

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO B — les autres séries
# MAGIC
# MAGIC Le CA n'est pas le seul indicateur à avoir bougé, et ce n'est pas celui qui a le
# MAGIC décrochage le plus brutal.

# COMMAND ----------

display(spark.sql(f"""
    SELECT order_date,
           count(*)                                                   AS lignes,
           round(sum(CASE WHEN is_orphan_product THEN 1 ELSE 0 END)
                 / count(*), 4)                                       AS taux_orphelins_produit,
           round(avg(net_amount), 2)                                  AS panier_ligne_moyen
    FROM {CATALOG}.silver.order_line
    WHERE order_date >= '2026-05-25'
    GROUP BY order_date ORDER BY order_date DESC
"""))

# MAGIC %md
# MAGIC Le taux de produits orphelins passe de 0,2 % à plus de 90 %. C'est un décrochage
# MAGIC bien plus violent que celui du CA, et il désigne directement le référentiel :
# MAGIC `bronze.ref_products_raw` ne contient plus que 500 lignes au lieu de 8 000.
# MAGIC
# MAGIC Le `DESCRIBE HISTORY` confirme la re-livraison :

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {CATALOG}.bronze.ref_products_raw").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO C et D — décomposer et confronter au catalogue
# MAGIC
# MAGIC Un prix de vente légitime vaut entre 0,80 et 1,05 fois le prix catalogue. Le rapport
# MAGIC est le bon révélateur, et il isole immédiatement les responsables.

# COMMAND ----------

display(spark.sql(f"""
    SELECT o.seller_id,
           count(*)                                            AS lignes,
           round(avg(o.unit_price / p.list_price), 1)           AS rapport_moyen,
           round(sum(o.net_amount), 2)                          AS ca_declare
    FROM {CATALOG}.silver.order_line o
    JOIN {CATALOG}.gold.dim_product p ON o.product_id = p.product_id
    WHERE o.order_date = '2026-06-04'
    GROUP BY o.seller_id
    HAVING avg(o.unit_price / p.list_price) > 10
    ORDER BY ca_declare DESC
"""))

# MAGIC %md
# MAGIC 25 vendeurs, un rapport moyen autour de 100. Leur passerelle de facturation émet les
# MAGIC prix en **centimes** : `14681` au lieu de `146,81`. La valeur reste un nombre
# MAGIC parfaitement valide — aucun contrôle de type, de format ou de nullité ne peut la
# MAGIC distinguer d'un prix normal.
# MAGIC
# MAGIC Le fait que ce soit exactement 25 vendeurs sur 600, tous d'un coup, le même jour,
# MAGIC pointe une mise à jour logicielle côté source plutôt qu'une saisie erronée.

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO E — chiffrer

# COMMAND ----------

impact = spark.sql(f"""
    SELECT count(*)                                            AS lignes_touchees,
           round(sum(o.net_amount), 2)                          AS ca_declare,
           round(sum(o.net_amount) / 100, 2)                    AS ca_plausible,
           round(sum(o.net_amount) - sum(o.net_amount) / 100, 2) AS surestimation
    FROM {CATALOG}.silver.order_line o
    JOIN {CATALOG}.gold.dim_product p ON o.product_id = p.product_id
    WHERE o.order_date = '2026-06-04' AND o.unit_price > 10 * p.list_price
""")
display(impact)

# MAGIC %md
# MAGIC **55 lignes, 439 591,92 € de chiffre d'affaires qui n'existe pas.**
# MAGIC
# MAGIC (La division par 100 donne un ordre de grandeur du CA réel, pas une correction
# MAGIC validée : rien ne prouve encore que le facteur soit exactement 100 sur toutes les
# MAGIC lignes.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO F — contenir
# MAGIC
# MAGIC Le gold est publié et faux. Trois options, et je retiens la première :
# MAGIC
# MAGIC | Option | Pourquoi / pourquoi pas |
# MAGIC |---|---|
# MAGIC | **`RESTORE` à la version d'avant le job** | Retenue. Une opération, réversible, et les analystes retrouvent des chiffres justes en quelques secondes. Ils perdent la journée du 04, ce qui est très préférable à une journée fausse. |
# MAGIC | Corriger d'abord, publier ensuite | Rejetée. Diagnostiquer proprement prend une heure ; pendant ce temps le tableau de bord ment. |
# MAGIC | Repointer une vue de service | Élégante, mais elle n'existe pas encore. À construire pour la prochaine fois — c'est la vraie leçon. |

# COMMAND ----------

version_avant = spark.sql(f"""
    SELECT max(version) AS v FROM (DESCRIBE HISTORY {CATALOG}.gold.fact_order_line)
    WHERE timestamp < '2026-06-05 00:00:00'
""").first()["v"]

print(f"restauration vers la version {version_avant}")
# spark.sql(f"RESTORE TABLE {CATALOG}.gold.fact_order_line TO VERSION AS OF {version_avant}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO G — réparer, dans le bon ordre
# MAGIC
# MAGIC **Le référentiel d'abord.** La règle de vraisemblance compare le prix de vente au
# MAGIC prix catalogue : avec 500 produits sur 8 000, elle n'aurait de quoi comparer que
# MAGIC pour 6 % des lignes et n'attraperait que 3 ou 4 des 55 lignes fautives.
# MAGIC
# MAGIC Réparer dans le mauvais ordre ne produit pas une erreur : ça produit une réparation
# MAGIC partielle qui a l'air d'avoir marché.

# COMMAND ----------

# 1. Re-livrer le referentiel complet dans le volume, depuis la vague W0 :
#
#    databricks fs cp "data/waves/W0_ref/ref/products.csv" \
#      "dbfs:/Volumes/novamarket/landing/files/ref/products.csv" --overwrite
#
# 2. Relancer le chargement des referentiels (M1_bronze_ref).

print(spark.table(f"{CATALOG}.bronze.ref_products_raw").count(), "produits (attendu 8 000)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO H — la règle de vraisemblance
# MAGIC
# MAGIC Elle s'ajoute aux quatre motifs de M3, dans la même construction de
# MAGIC `quarantine_reasons`. Pas en traitement ponctuel : demain, la source peut recommencer.
# MAGIC
# MAGIC Le `left join` est obligatoire — un produit absent du catalogue ne doit pas
# MAGIC disparaître du calcul, il doit simplement échapper à cette règle-là.

# COMMAND ----------

SCALE_FACTOR = 10

catalog_prices = (spark.table(f"{CATALOG}.bronze.ref_products_raw")
                  .select("product_id",
                          F.col("list_price").cast("decimal(10,2)").alias("_list_price"))
                  .distinct())

# A intégrer dans M3_silver_order_line, juste avant le calcul de quarantine_reasons :
#
# deduped = deduped.join(F.broadcast(catalog_prices), on="product_id", how="left")
#
# puis, dans le F.array(...) des motifs :
#
#     F.when(
#         F.col("_list_price").isNotNull()
#         & (clean_decimal("unit_price") > SCALE_FACTOR * F.col("_list_price")),
#         F.lit("SUSPECTED_UNIT_SCALE")),

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO I — rejouer l'aval
# MAGIC
# MAGIC Silver et gold sont recalculés intégralement depuis bronze (M3 et M5). C'est le
# MAGIC moment où l'idempotence choisie en M3 se rembourse : la réparation est un simple
# MAGIC relancement, il n'y a aucun état à rattraper, aucune ligne à supprimer à la main.
# MAGIC
# MAGIC Contrôle final attendu :

# COMMAND ----------

for label, got, expected in [
    ("silver.order_line", spark.table(f"{CATALOG}.silver.order_line").count(), 284_909),
    ("quarantaine", spark.table(f"{CATALOG}.ops.quarantine_order_line").count(), 2_305),
    ("gold.fact_order_line", spark.table(f"{CATALOG}.gold.fact_order_line").count(), 284_909),
]:
    print(f"{'OK ' if got == expected else 'KO '} {label:24s} {got:>8,} / {expected:>8,}"
          .replace(",", " "))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO J — les contrôles manquants
# MAGIC
# MAGIC ### `reference_volume_drop`
# MAGIC
# MAGIC Mon garde-fou de M6 était `row_count >= 1`. Il est passé au vert avec 500 lignes,
# MAGIC et il serait passé au vert avec 1. Il ne vérifiait pas que le référentiel était
# MAGIC **plausible**, seulement qu'il n'était pas **vide** — deux choses différentes que
# MAGIC j'avais confondues.
# MAGIC
# MAGIC Le bon contrôle compare la livraison à la précédente. Le *time travel* Delta donne
# MAGIC accès à l'état d'avant sans avoir à stocker quoi que ce soit.

# COMMAND ----------

v = spark.sql(f"SELECT max(version) AS v FROM (DESCRIBE HISTORY "
              f"{CATALOG}.bronze.ref_products_raw)").first()["v"]

drop_ratio_sql = f"""
    SELECT round(1 - (SELECT count(*) FROM {CATALOG}.bronze.ref_products_raw)
                   / nullif((SELECT count(*) FROM {CATALOG}.bronze.ref_products_raw
                             VERSION AS OF {max(v - 1, 0)}), 0), 4)
"""

# ("reference_volume_drop", seuil 0.20, comparaison "<=")
# Une livraison qui perd plus de 20 % de ses lignes est suspecte par construction.
print("part de lignes perdues :", spark.sql(drop_ratio_sql).first()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ### `daily_revenue_anomaly`
# MAGIC
# MAGIC Le rapport entre le CA du dernier jour et la moyenne des sept précédents. Un seuil
# MAGIC à 2 laisse passer les pics de soldes et de fin d'année tout en attrapant un facteur
# MAGIC 4,4 sans discussion.

# COMMAND ----------

daily_anomaly_sql = f"""
    WITH j AS (
        SELECT order_date, sum(net_amount) AS ca
        FROM {CATALOG}.silver.order_line WHERE is_revenue GROUP BY order_date
    ), r AS (
        SELECT order_date, ca,
               avg(ca) OVER (ORDER BY order_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS m7
        FROM j
    )
    SELECT round(ca / nullif(m7, 0), 4) FROM r ORDER BY order_date DESC LIMIT 1
"""

# ("daily_revenue_anomaly", seuil 2.0, comparaison "<=")
print("rapport du dernier jour a la tendance :", spark.sql(daily_anomaly_sql).first()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO K — `ops.incident_log`

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.ops.incident_log (
        incident_id   STRING,
        detected_at   TIMESTAMP,
        detected_by   STRING,
        severity      STRING,
        title         STRING,
        symptom       STRING,
        root_cause    STRING,
        affected_rows BIGINT,
        impact_amount DECIMAL(18,2),
        containment   STRING,
        remediation   STRING,
        prevention    STRING,
        status        STRING
    )
    COMMENT 'Journal des incidents de donnees. Une ligne par anomalie distincte.'
""")

incidents = [
    ("INC-2026-06-05-01", MEASURED_AT, "alerte metier (controle de gestion)", "CRITICAL",
     "Prix unitaires emis en centimes par 25 vendeurs",
     "CA du 2026-06-04 a 4,4x la tendance, a volume de commandes inchange",
     "La passerelle de facturation de 25 vendeurs a bascule l unite de l euro au centime "
     "sans changement de contrat ni preavis. Les valeurs restent des nombres valides : "
     "aucun controle de type, de format ou de nullite ne pouvait les distinguer.",
     55, "439591.92",
     "RESTORE de gold.fact_order_line a la version anterieure au job",
     "Ajout du motif de quarantaine SUSPECTED_UNIT_SCALE dans les regles silver, "
     "puis recalcul complet de silver et gold",
     "Controle daily_revenue_anomaly : rapport du CA quotidien a la moyenne mobile 7 jours, "
     "seuil 2.0, bloquant avant publication du gold",
     "RESOLVED"),

    ("INC-2026-06-05-02", MEASURED_AT, "controle automatique (taux d orphelins)", "HIGH",
     "Referentiel produits re-livre ampute de 94 % de ses lignes",
     "Taux de produits orphelins passe de 0,2 % a plus de 90 % sur la journee",
     "Le fichier products.csv a ete re-livre avec 500 lignes au lieu de 8 000, sous le "
     "meme nom. Le garde-fou existant ne verifiait que la non-vacuite du referentiel, "
     "pas sa plausibilite.",
     0, "0.00",
     "Aucune : l anomalie ne fausse pas les montants, seulement les rattachements",
     "Re-livraison du referentiel complet et rechargement de bronze.ref_products_raw",
     "Controle reference_volume_drop : part de lignes perdues d une livraison a l autre, "
     "seuil 20 %, appuye sur le time travel Delta",
     "RESOLVED"),

    ("INC-2026-06-05-03", MEASURED_AT, "quarantaine automatique (M3)", "LOW",
     "Fichier de commandes tronque en fin de transfert",
     "Une ligne du fichier du 2026-06-04 s arrete en plein milieu",
     "Transfert interrompu cote source. La ligne partielle a ete lue avec ses colonnes "
     "manquantes a null.",
     1, "0.00",
     "Aucune : la ligne a ete quarantinee automatiquement des l ingestion",
     "Aucune. Le dispositif existant a fait son travail.",
     "Deja couvert par les motifs INVALID_TIMESTAMP, INVALID_QUANTITY, INVALID_PRICE et "
     "UNKNOWN_STATUS, declenches simultanement sur cette ligne.",
     "RESOLVED"),
]

schema = ("incident_id string, detected_at timestamp, detected_by string, severity string, "
          "title string, symptom string, root_cause string, affected_rows bigint, "
          "impact_amount string, containment string, remediation string, prevention string, "
          "status string")

(spark.createDataFrame(incidents, schema)
      .withColumn("impact_amount", F.col("impact_amount").cast("decimal(18,2)"))
      .write.mode("append").saveAsTable(f"{CATALOG}.ops.incident_log"))

display(spark.table(f"{CATALOG}.ops.incident_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Post-mortem
# MAGIC
# MAGIC **1. Délai de détection**
# MAGIC
# MAGIC > Les données fausses sont arrivées dans la nuit du 4 au 5. Le job les a publiées
# MAGIC > vers 4 h 30. Le contrôle de gestion a écrit à 8 h 15. **Environ quatre heures**,
# MAGIC > et surtout : la détection n'est venue d'aucun de mes contrôles. Elle est venue
# MAGIC > d'un humain qui a trouvé un chiffre bizarre.
# MAGIC >
# MAGIC > Ce qui l'aurait raccourci, par ordre de rentabilité :
# MAGIC >
# MAGIC > - Le contrôle `daily_revenue_anomaly` : détection **avant publication**, délai nul,
# MAGIC >   coût une requête. C'est le seul qui compte vraiment.
# MAGIC > - Une barrière qualité réellement placée avant le gold (question 3 de M8) : sans
# MAGIC >   elle, même un contrôle qui se déclenche n'empêche rien.
# MAGIC > - Une alerte sur le taux d'orphelins : elle aurait signalé le second incident sans
# MAGIC >   rien dire du premier.
# MAGIC >
# MAGIC > À l'inverse, augmenter la fréquence du job n'aurait servi à rien : le problème
# MAGIC > n'est pas la latence, c'est l'absence de contrôle métier.
# MAGIC
# MAGIC **2. Pourquoi le garde-fou est-il resté vert ?**
# MAGIC
# MAGIC > Parce que je l'avais écrit comme `row_count >= 1`, et que j'avais appelé ça un
# MAGIC > « garde-fou du référentiel vide ». Le nom était juste, la protection aussi — pour
# MAGIC > le cas du fichier vide. Elle ne couvrait pas le cas du fichier **incomplet**, qui
# MAGIC > est pourtant beaucoup plus fréquent : un transfert coupé, un export filtré par
# MAGIC > erreur, une pagination oubliée.
# MAGIC >
# MAGIC > Ce que ça m'apprend, et c'est plus large que ce contrôle : j'avais choisi mes
# MAGIC > seuils en pensant au **mode de défaillance que j'imaginais**, pas à la grandeur que
# MAGIC > je voulais surveiller. Un seuil absolu répond à « est-ce nul ? ». Un seuil relatif
# MAGIC > répond à « est-ce cohérent avec hier ? ». La seconde question est presque toujours
# MAGIC > la bonne, et j'avais déjà écrit ça noir sur blanc en M6 — sans l'appliquer à ce
# MAGIC > contrôle-là.
# MAGIC
# MAGIC **3. Quelle famille de contrôles manque ?**
# MAGIC
# MAGIC > Les contrôles de **vraisemblance métier**. Mon dispositif ne savait vérifier que
# MAGIC > des propriétés techniques : types, formats, nullité, unicité, intégrité
# MAGIC > référentielle. Tout ça était irréprochable le 4 juin — les 55 lignes fautives
# MAGIC > passaient chacune des seize vérifications.
# MAGIC >
# MAGIC > Ce qui manquait : « un prix de vente ressemble-t-il au prix catalogue ? », « le CA
# MAGIC > d'un jour ressemble-t-il à celui d'hier ? », « un panier moyen de 8 000 € est-il
# MAGIC > plausible sur une marketplace généraliste ? ». Aucune de ces questions n'est
# MAGIC > exprimable en termes de schéma.
# MAGIC >
# MAGIC > C'est systématiquement la dernière famille qu'on écrit, pour trois raisons qui se
# MAGIC > renforcent : elle demande de comprendre le métier et pas seulement la donnée ;
# MAGIC > elle n'a pas de seuil évident, donc elle oblige à un arbitrage qu'on préfère
# MAGIC > reporter ; et elle produit des faux positifs, donc elle coûte du temps
# MAGIC > d'investigation tout de suite pour un incident hypothétique plus tard.
# MAGIC >
# MAGIC > Les trois sont de vraies objections. Et elles pèsent zéro face à 439 592 € de CA
# MAGIC > fantôme dans un tableau de bord de direction.
# MAGIC
# MAGIC **4. Quarantiner 55 commandes réelles : bon arbitrage ?**
# MAGIC
# MAGIC > Oui, à court terme, et c'est un arbitrage inconfortable qu'il faut assumer
# MAGIC > explicitement plutôt que subir.
# MAGIC >
# MAGIC > Ces 55 commandes existent, les clients ont payé, et leur CA est maintenant absent
# MAGIC > du gold. J'ai donc **sous-estimé** le CA du 4 juin d'environ 4 400 € réels pour
# MAGIC > éviter de le **surestimer** de 439 592 €. Le rapport est de 1 à 100 : ce n'est pas
# MAGIC > un choix difficile.
# MAGIC >
# MAGIC > Ce qui rend l'arbitrage acceptable, c'est que la perte est **réversible et tracée**.
# MAGIC > Les lignes sont en quarantaine avec leur donnée brute ; le jour où la source
# MAGIC > confirme, elles rentrent.
# MAGIC >
# MAGIC > Si la source confirmait le facteur 100, je ne corrigerais pas dans le silver. Je
# MAGIC > demanderais une **re-livraison du fichier corrigé** : c'est la source qui doit
# MAGIC > porter la vérité, pas une règle de division cachée dans mon pipeline. Une
# MAGIC > correction en dur du type « si vendeur ∈ liste et date = 4 juin alors ÷ 100 »
# MAGIC > fonctionne le premier jour, et devient une ligne de code que plus personne n'ose
# MAGIC > toucher trois ans plus tard.
# MAGIC >
# MAGIC > Si la re-livraison était impossible, alors la correction se ferait — mais dans une
# MAGIC > table de correctifs explicite, datée, avec un propriétaire métier identifié, et
# MAGIC > jointe au silver plutôt qu'enfouie dedans.
# MAGIC
# MAGIC **5. Post-mortem en cinq lignes, pour un non-spécialiste**
# MAGIC
# MAGIC > *Mercredi 4 juin, 25 vendeurs sur 600 ont envoyé leurs prix en centimes au lieu
# MAGIC > d'euros, à cause d'une mise à jour de leur outil de facturation.*
# MAGIC >
# MAGIC > *Résultat : le chiffre d'affaires de la journée affichait 440 000 € de trop, soit
# MAGIC > 4 fois la réalité. Les tableaux de bord ont été corrigés jeudi matin ; aucune
# MAGIC > facture, aucun paiement et aucune commande client n'est concerné — seul
# MAGIC > l'affichage était faux.*
# MAGIC >
# MAGIC > *Nous avons mis en place une alerte automatique qui bloquera désormais la
# MAGIC > publication des chiffres si le CA d'une journée s'écarte trop de la tendance.*
# MAGIC >
# MAGIC > *Il faut prévenir les 25 vendeurs concernés : leurs propres statistiques de vente
# MAGIC > sont fausses tant qu'ils n'ont pas corrigé leur outil. La liste est en pièce
# MAGIC > jointe.*
# MAGIC >
# MAGIC > *Point d'attention : nous avons découvert cette erreur parce qu'une personne a
# MAGIC > trouvé un chiffre bizarre, pas parce qu'un contrôle s'est déclenché. C'est ce que
# MAGIC > nous corrigeons en priorité.*
