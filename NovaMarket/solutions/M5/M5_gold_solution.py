# Databricks notebook source
# MAGIC %md
# MAGIC # Corrigé — M5 : couche gold

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

CATALOG = "novamarket"
CALENDAR_START, CALENDAR_END = "2025-12-01", "2026-06-30"
BATCH_ID = str(uuid.uuid4())

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO A — `dim_date`
# MAGIC
# MAGIC `weekday()` renvoie 0 pour lundi ; `dayofweek()` renvoie 1 pour **dimanche**.
# MAGIC Confondre les deux décale toute l'analyse hebdomadaire d'un jour, et ça ne se voit
# MAGIC pas sur un graphique.

# COMMAND ----------

(spark.sql(f"""
    SELECT explode(sequence(to_date('{CALENDAR_START}'), to_date('{CALENDAR_END}'),
                            interval 1 day)) AS date_key
 """)
 .select(
     "date_key",
     F.year("date_key").alias("year"),
     F.month("date_key").alias("month"),
     F.date_format("date_key", "yyyy-MM").alias("year_month"),
     F.dayofmonth("date_key").alias("day_of_month"),
     (F.expr("weekday(date_key)") + 1).alias("day_of_week"),
     F.date_format("date_key", "EEEE").alias("day_name"),
     (F.expr("weekday(date_key)") >= 5).alias("is_weekend"),
     F.quarter("date_key").alias("quarter"),
 )
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.gold.dim_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO B — `dim_customer`
# MAGIC
# MAGIC On conserve les 35 clients marqués supprimés. Les retirer casserait l'intégrité
# MAGIC référentielle de faits parfaitement légitimes : le droit à l'effacement porte sur
# MAGIC les **données personnelles**, pas sur l'existence de la transaction. La bonne
# MAGIC réponse RGPD est d'anonymiser les attributs identifiants tout en gardant la clé —
# MAGIC ce que la source a d'ailleurs commencé à faire en vidant l'e-mail.

# COMMAND ----------

(spark.table(f"{CATALOG}.silver.customer_scd2")
 .filter("is_current")
 .select("customer_id", "first_name", "last_name", "email", "country", "city", "zip_code",
         "segment", "is_opt_in", "is_deleted", "created_at",
         F.current_timestamp().alias("_processed_at"))
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.gold.dim_customer"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO C — `dim_seller` et sa clé de version

# COMMAND ----------

(spark.table(f"{CATALOG}.silver.seller_scd2")
 .select(
     F.concat_ws("#", F.col("seller_id"),
                 F.date_format("valid_from", "yyyyMMddHHmmss")).alias("seller_sk"),
     "seller_id", "seller_name", "seller_country", "seller_city", "main_top_category",
     "plan_code", "is_active", "onboarded_at", "valid_from", "valid_to", "is_current",
     F.current_timestamp().alias("_processed_at"))
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.gold.dim_seller"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO D — `dim_product` et `ref_commission_plan`

# COMMAND ----------

products = spark.table(f"{CATALOG}.bronze.ref_products_raw")
categories = spark.table(f"{CATALOG}.bronze.ref_categories_raw")

(products.join(F.broadcast(categories), on="category_id", how="left")
 .select(
     "product_id", "product_name", "brand", "category_id", "category_label",
     "top_category_code", "top_category_label", "seller_id",
     F.col("list_price").cast("decimal(10,2)").alias("list_price"),
     F.col("is_discontinued").cast("boolean").alias("is_discontinued"),
     F.current_timestamp().alias("_processed_at"))
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.gold.dim_product"))

(spark.createDataFrame([("BASIC", "0.150"), ("PLUS", "0.115"), ("PREMIUM", "0.085")],
                       "plan_code string, rate string")
 .select("plan_code", F.col("rate").cast("decimal(5,3)").alias("commission_rate"))
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.gold.ref_commission_plan"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO E — `fact_order_line`
# MAGIC
# MAGIC Jointure **gauche** sur la dimension, pas interne. Une jointure interne qui perd
# MAGIC des lignes ne dit rien ; une jointure gauche suivie d'un contrôle sur les `null`
# MAGIC te dit exactement combien et lesquelles. La différence entre un pipeline qui
# MAGIC échoue bruyamment et un pipeline qui ment.

# COMMAND ----------

orders = spark.table(f"{CATALOG}.silver.order_line")
sellers = spark.table(f"{CATALOG}.gold.dim_seller")
rates = spark.table(f"{CATALOG}.gold.ref_commission_plan")

resolved = (
    orders.alias("o")
    .join(sellers.alias("d"),
          (F.col("o.seller_id") == F.col("d.seller_id"))
          & (F.col("o.order_ts") >= F.col("d.valid_from"))
          & (F.col("d.valid_to").isNull() | (F.col("o.order_ts") < F.col("d.valid_to"))),
          how="left")
    .join(F.broadcast(rates.alias("r")), F.col("d.plan_code") == F.col("r.plan_code"), how="left")
)

n_in, n_out = orders.count(), resolved.count()
n_unresolved = resolved.filter(F.col("d.seller_sk").isNull()).count()
print(f"entrée {n_in}  sortie {n_out}  non résolus {n_unresolved}")
assert n_in == n_out, "la jointure temporelle a dupliqué ou perdu des lignes"
assert n_unresolved == 0, "des commandes n'ont pas de version de vendeur en vigueur"

# COMMAND ----------

fact = resolved.select(
    F.col("o.order_line_id"), F.col("o.order_id"), F.col("o.order_date"), F.col("o.order_ts"),
    F.col("o.customer_id"), F.col("d.seller_sk"), F.col("o.seller_id"), F.col("o.product_id"),
    F.col("o.quantity"), F.col("o.unit_price"), F.col("o.discount_amount"),
    F.col("o.gross_amount"), F.col("o.net_amount"),
    F.col("r.commission_rate"),
    F.when(F.col("o.is_revenue"),
           F.round(F.col("o.net_amount") * F.col("r.commission_rate"), 2))
     .otherwise(F.lit(0)).cast("decimal(12,2)").alias("commission_amount"),
    F.col("o.order_status"), F.col("o.payment_method"), F.col("o.shipping_country"),
    F.col("o.is_revenue"), F.col("o.is_orphan_customer"), F.col("o.is_orphan_product"),
    F.current_timestamp().alias("_processed_at"),
)

(fact.write.mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(f"{CATALOG}.gold.fact_order_line"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO F — `agg_revenue_monthly`

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.gold.agg_revenue_monthly AS
    SELECT
        date_format(f.order_date, 'yyyy-MM')                  AS year_month,
        coalesce(p.top_category_code, 'UNKNOWN')              AS top_category_code,
        f.seller_id                                           AS seller_id,
        cast(sum(f.net_amount)        AS decimal(18,2))       AS net_amount,
        cast(sum(f.commission_amount) AS decimal(18,2))       AS commission_amount,
        count(*)                                              AS n_lines,
        count(DISTINCT f.order_id)                            AS n_orders
    FROM {CATALOG}.gold.fact_order_line f
    LEFT JOIN {CATALOG}.gold.dim_product p ON f.product_id = p.product_id
    WHERE f.is_revenue
    GROUP BY 1, 2, 3
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO G — `agg_funnel_source`
# MAGIC
# MAGIC `count(DISTINCT CASE WHEN ... END)` : le `CASE` renvoie `null` hors condition, et
# MAGIC `count(DISTINCT)` ignore les `null`. C'est le motif standard pour compter des
# MAGIC sessions par étape sans quatre sous-requêtes.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.gold.agg_funnel_source AS
    SELECT
        coalesce(utm_source, 'unknown') AS utm_source,
        count(DISTINCT session_id)                                                   AS sessions,
        count(DISTINCT CASE WHEN event_type = 'product_view'   THEN session_id END)  AS sessions_product_view,
        count(DISTINCT CASE WHEN event_type = 'add_to_cart'    THEN session_id END)  AS sessions_add_to_cart,
        count(DISTINCT CASE WHEN event_type = 'checkout_start' THEN session_id END)  AS sessions_checkout_start,
        count(DISTINCT CASE WHEN event_type = 'purchase'       THEN session_id END)  AS sessions_purchase,
        round(count(DISTINCT CASE WHEN event_type = 'purchase' THEN session_id END)
              / count(DISTINCT session_id), 4)                                       AS conversion_rate
    FROM {CATALOG}.silver.event
    GROUP BY 1
""")

display(spark.table(f"{CATALOG}.gold.agg_funnel_source").orderBy(F.col("sessions").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO H — `v_top_products_90d`
# MAGIC
# MAGIC La borne est **calculée**, pas codée en dur : la vue reste juste quand de nouvelles
# MAGIC commandes arrivent. Une vue qui contient une date en dur est une bombe à
# MAGIC retardement — elle ne casse pas, elle devient fausse en silence.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.gold.v_top_products_90d AS
    WITH bounds AS (
        SELECT date_sub(max(order_date), 90) AS cutoff FROM {CATALOG}.gold.fact_order_line
    ),
    scoped AS (
        SELECT f.* FROM {CATALOG}.gold.fact_order_line f, bounds b
        WHERE f.order_date > b.cutoff
    ),
    agg AS (
        SELECT
            product_id,
            cast(sum(CASE WHEN is_revenue THEN net_amount ELSE 0 END) AS decimal(18,2)) AS net_amount,
            count(*)                                                    AS n_lines,
            sum(CASE WHEN order_status = 'RETURNED' THEN 1 ELSE 0 END)  AS n_returned_lines
        FROM scoped GROUP BY product_id
    )
    SELECT
        a.product_id,
        p.product_name,
        coalesce(p.top_category_code, 'UNKNOWN') AS top_category_code,
        a.net_amount,
        a.n_lines,
        a.n_returned_lines,
        round(a.n_returned_lines / a.n_lines, 4) AS return_rate
    FROM agg a
    LEFT JOIN {CATALOG}.gold.dim_product p ON a.product_id = p.product_id
    ORDER BY a.net_amount DESC
    LIMIT 20
""")

display(spark.table(f"{CATALOG}.gold.v_top_products_90d"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO I — les trois vues restantes

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.gold.v_seller_quality_monthly AS
    SELECT
        date_format(order_date, 'yyyy-MM') AS year_month,
        seller_id,
        count(*)                                                        AS n_lines,
        sum(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END)     AS n_cancelled,
        sum(CASE WHEN order_status = 'RETURNED'  THEN 1 ELSE 0 END)     AS n_returned,
        round(sum(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END) / count(*), 4)
                                                                        AS cancellation_rate,
        round(sum(CASE WHEN order_status = 'RETURNED'  THEN 1 ELSE 0 END) / count(*), 4)
                                                                        AS return_rate
    FROM {CATALOG}.gold.fact_order_line
    GROUP BY 1, 2
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Panier moyen : le grain compte. On agrège d'abord **par commande**, puis on moyenne.
# MAGIC Prendre directement `avg(net_amount)` sur les lignes donnerait le panier moyen par
# MAGIC *ligne*, environ deux fois plus faible — et personne ne s'en apercevrait.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.gold.v_basket_by_segment AS
    WITH per_order AS (
        SELECT f.order_id, f.customer_id, f.shipping_country,
               cast(sum(f.net_amount) AS decimal(18,2)) AS order_amount
        FROM {CATALOG}.gold.fact_order_line f
        WHERE f.is_revenue
        GROUP BY 1, 2, 3
    )
    SELECT
        coalesce(c.segment, 'UNKNOWN') AS segment,
        o.shipping_country             AS country,
        count(*)                       AS n_orders,
        count(DISTINCT o.customer_id)  AS n_customers,
        round(avg(o.order_amount), 2)  AS avg_basket,
        cast(sum(o.order_amount) AS decimal(18,2)) AS net_amount
    FROM per_order o
    LEFT JOIN {CATALOG}.gold.dim_customer c ON o.customer_id = c.customer_id
    GROUP BY 1, 2
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.gold.v_customer_cohort AS
    WITH first_order AS (
        SELECT customer_id, date_format(min(order_date), 'yyyy-MM') AS cohort_month
        FROM {CATALOG}.gold.fact_order_line
        WHERE is_revenue
        GROUP BY customer_id
    ),
    activity AS (
        SELECT DISTINCT f.customer_id, date_format(f.order_date, 'yyyy-MM') AS active_month
        FROM {CATALOG}.gold.fact_order_line f
        WHERE f.is_revenue
    )
    SELECT
        fo.cohort_month,
        a.active_month,
        months_between(to_date(concat(a.active_month, '-01')),
                       to_date(concat(fo.cohort_month, '-01'))) AS month_offset,
        count(DISTINCT a.customer_id)                            AS active_customers,
        count(DISTINCT fo.customer_id)                           AS cohort_size
    FROM first_order fo
    JOIN activity a ON fo.customer_id = a.customer_id
    GROUP BY 1, 2, 3
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO J — documentation

# COMMAND ----------

spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.gold.fact_order_line IS
    'Fait au grain ligne de commande. Une ligne = un produit dans une commande.
     Le chiffre d affaires ne doit etre somme que sur is_revenue = true.'
""")

COLUMN_COMMENTS = {
    "seller_sk": (
        "Cle de la VERSION de vendeur en vigueur a la date de la commande, pas du vendeur. "
        "Joindre dim_seller sur seller_sk (et non sur seller_id) pour obtenir le plan et le "
        "taux de commission historiquement corrects. Une jointure sur seller_id + is_current "
        "fausse la commission de 45 765,97 EUR sur l historique."),
    "commission_rate": (
        "Taux de commission du plan du vendeur A LA DATE DE LA COMMANDE, resolu via seller_sk."),
    "commission_amount": (
        "net_amount * commission_rate arrondi au centime, ou 0.00 si la ligne ne genere pas "
        "de chiffre d affaires. Jamais nul."),
    "is_revenue": (
        "Faux pour les statuts CANCELLED et RETURNED. Tout calcul de chiffre d affaires ou "
        "de commission doit filtrer dessus."),
    "net_amount": (
        "quantity * unit_price - discount_amount. Montant TTC en euros."),
}

for column, comment in COLUMN_COMMENTS.items():
    spark.sql(f"ALTER TABLE {CATALOG}.gold.fact_order_line "
              f"ALTER COLUMN {column} COMMENT '{comment}'")

display(spark.sql(f"DESCRIBE TABLE {CATALOG}.gold.fact_order_line"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Réponses
# MAGIC
# MAGIC **1. Pourquoi `dim_customer` en SCD1 et `dim_seller` en SCD2 ?**
# MAGIC
# MAGIC > Parce qu'une seule des deux dimensions participe à un calcul monétaire dépendant
# MAGIC > du temps. Le plan du vendeur détermine un taux de commission : se tromper de
# MAGIC > version, c'est se tromper de 45 765,97 €. Le segment d'un client ne détermine
# MAGIC > aucun montant : se tromper de version, c'est au pire ranger une commande dans la
# MAGIC > mauvaise case d'un tableau croisé.
# MAGIC >
# MAGIC > Ce qu'on perd est réel : impossible de répondre à « quel CA les clients qui
# MAGIC > étaient VIP **à ce moment-là** ont-ils généré ? ». On ne peut répondre qu'à « quel
# MAGIC > CA les clients aujourd'hui VIP ont-ils généré », ce qui est une autre question —
# MAGIC > et une question qui réécrit le passé à chaque changement de segment.
# MAGIC >
# MAGIC > Le point important : **l'historique n'est pas perdu**, il est dans
# MAGIC > `silver.customer_scd2`. On a choisi de ne pas l'exposer en gold parce que personne
# MAGIC > ne l'a demandé et qu'une dimension SCD2 coûte cher en compréhension pour ses
# MAGIC > utilisateurs. Le jour où le besoin arrive, c'est une vue à écrire, pas une donnée
# MAGIC > à retrouver. C'est exactement pour ça qu'on ne construit pas gold en écrasant
# MAGIC > silver.
# MAGIC
# MAGIC **2. Les 1 721 lignes à client orphelin**
# MAGIC
# MAGIC > Trois approches classiques, par ordre de préférence :
# MAGIC >
# MAGIC > **Membre inféré** (le standard Kimball). On insère dans `dim_customer` une ligne
# MAGIC > par clé orpheline, avec les attributs à `null` et un drapeau `is_inferred`.
# MAGIC > L'intégrité référentielle est parfaite, les jointures internes fonctionnent, et
# MAGIC > le jour où le référentiel rattrape son retard, la ligne se remplit toute seule.
# MAGIC > C'est la seule approche qui ne demande rien aux analystes.
# MAGIC >
# MAGIC > **Membre inconnu unique** : une ligne `customer_id = '-1'` vers laquelle pointent
# MAGIC > tous les orphelins. Plus simple, mais on perd l'identifiant réel — donc la
# MAGIC > capacité de réconcilier plus tard.
# MAGIC >
# MAGIC > **Clé naturelle + drapeau** — ce qu'on a fait ici. Le fait conserve le
# MAGIC > `customer_id` réel et `is_orphan_customer`. Ça marche parce que notre fait n'utilise
# MAGIC > pas de clé de substitution pour le client. Le risque est reporté sur l'analyste :
# MAGIC > s'il écrit une jointure interne vers `dim_customer`, il perd 1 721 lignes et
# MAGIC > 0,6 % du CA sans aucun signal.
# MAGIC >
# MAGIC > Avec le recul, le membre inféré serait le bon choix pour une vraie mise en
# MAGIC > production. Notre approche est défendable à condition que la vue de service
# MAGIC > exposée aux analystes fasse la jointure gauche à leur place.
# MAGIC
# MAGIC **3. `agg_revenue_monthly` vaut-il son coût ?**
# MAGIC
# MAGIC > Non, pas sur ce volume. 13 333 lignes contre 282 104, soit un facteur 21 : sur un
# MAGIC > moteur qui lit du Parquet compressé avec élagage de fichiers, l'agrégat à la volée
# MAGIC > coûterait quelques centaines de millisecondes de plus. On a créé une table à
# MAGIC > maintenir, à rafraîchir, à surveiller, et une occasion de divergence avec le fait.
# MAGIC >
# MAGIC > La question devient sérieuse vers un facteur **1 000**, et surtout quand
# MAGIC > l'agrégat n'est pas qu'une somme : distincts approximés, fenêtres glissantes,
# MAGIC > jointures multiples. Là, on ne pré-calcule plus pour la vitesse, on pré-calcule
# MAGIC > pour figer une définition.
# MAGIC >
# MAGIC > L'argument qui sauve cet agrégat n'est donc pas la performance mais la
# MAGIC > **sémantique** : il matérialise une fois pour toutes le fait que le CA se somme
# MAGIC > sur `is_revenue` et que les orphelins vont dans `UNKNOWN`. Sur Databricks, une
# MAGIC > vue matérialisée exprimerait ça mieux qu'une table gérée à la main — mais la Free
# MAGIC > Edition n'autorise qu'un pipeline déclaratif actif, qu'on garde pour M7.
# MAGIC
# MAGIC **4. Le jour où les taux de commission changent**
# MAGIC
# MAGIC > `ref_commission_plan` n'a pas de dimension temporelle : un `UPDATE` du taux
# MAGIC > recalculerait **tout l'historique** au prochain rafraîchissement du fait. On aurait
# MAGIC > passé M4 à historiser le plan du vendeur pour se faire trahir par la table de
# MAGIC > taux. C'est le genre d'incohérence qui ne se voit qu'après coup, quand un
# MAGIC > contrôleur de gestion signale que le CA de janvier a bougé.
# MAGIC >
# MAGIC > Ce qui protège aujourd'hui : `commission_amount` est **matérialisé** dans le fait.
# MAGIC > Tant qu'on ne le recalcule pas, il est figé. C'est une protection par accident,
# MAGIC > pas par conception.
# MAGIC >
# MAGIC > La correction : donner à `ref_commission_plan` un `valid_from` / `valid_to`, et
# MAGIC > résoudre le taux par la même jointure temporelle que le vendeur. Le taux devient
# MAGIC > une dimension SCD2 de plus. Coût : deux colonnes et une condition de jointure.
# MAGIC > C'est la bonne réponse, et elle aurait dû être là dès le départ.
# MAGIC
# MAGIC **5. Vue ou table ?**
# MAGIC
# MAGIC > Le critère que j'ai appliqué : **une vue quand le résultat dépend de la date du
# MAGIC > jour**, une table quand il est stable.
# MAGIC >
# MAGIC > `v_top_products_90d` porte une fenêtre glissante. Matérialisée, elle serait
# MAGIC > périmée le lendemain — et périmée sans erreur, ce qui est pire. En vue, elle est
# MAGIC > juste par construction.
# MAGIC >
# MAGIC > `agg_revenue_monthly` porte des mois clos qui ne bougeront plus. Une table se
# MAGIC > justifie… si tant est que l'agrégat lui-même se justifie, ce dont je doute
# MAGIC > (question 3).
# MAGIC >
# MAGIC > Un second critère, plus décisif en pratique : **une vue ne peut pas être
# MAGIC > incohérente avec sa source**. Chaque table dérivée est une copie qui peut diverger,
# MAGIC > et qui divergera le jour où quelqu'un corrigera le fait sans relancer l'agrégat.
# MAGIC > Ma règle par défaut est donc : vue, sauf preuve du contraire — la preuve étant un
# MAGIC > temps de réponse mesuré, pas supposé.
