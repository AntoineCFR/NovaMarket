# Databricks notebook source
# MAGIC %md
# MAGIC # Corrigé — M6 : qualité, métadonnées et observabilité

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

CATALOG = "novamarket"
RUN_ID = str(uuid.uuid4())
MEASURED_AT = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO A — description des contrôles
# MAGIC
# MAGIC Chaque contrôle est une **donnée**, pas du code. C'est ce qui permet d'en ajouter un
# MAGIC dix-septième sans toucher au moteur, et de les lister dans une documentation.

# COMMAND ----------

C = CATALOG

CHECKS = [
    # (layer, table, check_name, sql, threshold, comparison)
    ("bronze", "bronze.orders_raw", "row_count",
     f"SELECT count(*) FROM {C}.bronze.orders_raw", 250_000, ">="),
    # Surtout pas `WHERE _rescued_data IS NOT NULL` : sur les commandes cette colonne est
    # vide, le controle vaudrait 0 et passerait au vert en ne mesurant rien du tout.
    # Le lecteur CSV a jete les champs en trop ; la seule trace est l'adresse tronquee.
    ("bronze", "bronze.orders_raw", "truncated_rows",
     f"SELECT count(*) FROM {C}.bronze.orders_raw "
     f"WHERE shipping_address NOT LIKE '%,%'", 3_000, "<="),
    ("bronze", "bronze.events_raw", "row_count",
     f"SELECT count(*) FROM {C}.bronze.events_raw", 100_000, ">="),
    ("bronze", "bronze.events_raw", "malformed_rows",
     f"SELECT count(*) FROM {C}.bronze.events_raw "
     f"WHERE event_id IS NULL", 1_500, "<="),
    ("bronze", "bronze.ref_products_raw", "row_count",
     f"SELECT count(*) FROM {C}.bronze.ref_products_raw", 1, ">="),
    ("bronze", "bronze.app_customers_raw", "row_count",
     f"SELECT count(*) FROM {C}.bronze.app_customers_raw", 1, ">="),

    ("silver", "silver.order_line", "row_count",
     f"SELECT count(*) FROM {C}.silver.order_line", 250_000, ">="),
    ("silver", "silver.order_line", "duplicate_keys",
     f"SELECT count(*) - count(DISTINCT order_line_id) FROM {C}.silver.order_line", 0, "=="),
    ("silver", "silver.order_line", "null_unit_price",
     f"SELECT count(*) FROM {C}.silver.order_line WHERE unit_price IS NULL", 0, "=="),
    ("silver", "silver.order_line", "orphan_customer_rows",
     f"SELECT count(*) FROM {C}.silver.order_line WHERE is_orphan_customer", 5_642, "<="),
    ("silver", "silver.order_line", "orphan_product_rows",
     f"SELECT count(*) FROM {C}.silver.order_line WHERE is_orphan_product", 2_821, "<="),
    ("ops", "ops.quarantine_order_line", "row_count",
     f"SELECT count(*) FROM {C}.ops.quarantine_order_line", 2_843, "<="),
    ("silver", "silver.seller_scd2", "multiple_current_versions",
     f"SELECT count(*) FROM (SELECT seller_id FROM {C}.silver.seller_scd2 "
     f"GROUP BY seller_id HAVING sum(cast(is_current AS int)) <> 1)", 0, "=="),
    ("silver", "silver.seller_scd2", "chain_breaks",
     f"""SELECT count(*) FROM (
            SELECT valid_to, lead(valid_from) OVER (PARTITION BY seller_id ORDER BY valid_from) AS nxt
            FROM {C}.silver.seller_scd2)
         WHERE nxt IS NOT NULL AND valid_to <> nxt""", 0, "=="),

    ("gold", "gold.fact_order_line", "row_count",
     f"SELECT count(*) FROM {C}.gold.fact_order_line", 250_000, ">="),
    ("gold", "gold.fact_order_line", "orphan_seller_sk",
     f"""SELECT count(*) FROM {C}.gold.fact_order_line f
         LEFT ANTI JOIN {C}.gold.dim_seller d ON f.seller_sk = d.seller_sk""", 0, "=="),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Justification des seuils
# MAGIC
# MAGIC | Type de contrôle | Seuil retenu | Pourquoi |
# MAGIC |---|---|---|
# MAGIC | Invariants (6) | `== 0` | Ce sont des propriétés que le code garantit. Une seule violation signifie que le code est faux, pas que les données ont bougé. |
# MAGIC | Volumétries | `>= 80 %` de l'observé | Attrape le cas qui compte : une source qui livre un fichier vide ou tronqué. Un seuil sur la valeur exacte serait cassé dès la prochaine livraison. |
# MAGIC | Référentiels | `>= 1` | Le **garde-fou du référentiel vide**. C'est le contrôle le plus rentable du lot : il attrape à lui seul la majorité des incidents réels, et il tient en trois mots. |
# MAGIC | Taux d'anomalie | `<= 2 ×` la valeur observée | On surveille l'ordre de grandeur, pas la valeur. Un seuil à `<= 1 721` déclencherait à la 1 722ᵉ ligne orpheline, c'est-à-dire tous les jours pour rien. |
# MAGIC
# MAGIC Le principe général : **un seuil doit être choisi pour ce qu'il attrape, pas pour
# MAGIC ce qu'il mesure.** Un seuil qui se déclenche une fois par semaine sans qu'on agisse
# MAGIC sera désactivé dans le mois, et le jour où il aurait servi, il sera muet.

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO B et C — le moteur

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C}.ops.dq_metrics (
        run_id       STRING    COMMENT 'Execution de la campagne de controles',
        measured_at  TIMESTAMP COMMENT 'Horodatage commun a toute la campagne',
        layer        STRING    COMMENT 'bronze / silver / gold / ops',
        table_name   STRING    COMMENT 'Table controlee, prefixee du schema',
        check_name   STRING    COMMENT 'Identifiant du controle',
        metric_value DOUBLE    COMMENT 'Valeur mesuree',
        threshold    DOUBLE    COMMENT 'Seuil retenu',
        comparison   STRING    COMMENT 'Operateur : <=, >= ou ==',
        status       STRING    COMMENT 'PASS / WARN / FAIL, deduit de la comparaison'
    )
    COMMENT 'Historique des controles de qualite. Une ligne = un controle a un instant.'
""")


def evaluate(metric_value, threshold, comparison):
    ok = {"<=": metric_value <= threshold,
          ">=": metric_value >= threshold,
          "==": metric_value == threshold}[comparison]
    if ok:
        return "PASS"
    # Un invariant viole est un bug ; un seuil d'observation depasse est un signal.
    return "FAIL" if comparison == "==" and threshold == 0 else "WARN"


def run_checks(checks, run_id, measured_at):
    rows = []
    for layer, table, name, sql, threshold, comparison in checks:
        value = float(spark.sql(sql).first()[0])
        rows.append((run_id, measured_at, layer, table, name,
                     value, float(threshold), comparison,
                     evaluate(value, float(threshold), comparison)))
        print(f"{rows[-1][8]:5s} {table:28s} {name:26s} {value:>12,.0f}".replace(",", " "))

    schema = ("run_id string, measured_at timestamp, layer string, table_name string, "
              "check_name string, metric_value double, threshold double, "
              "comparison string, status string")
    (spark.createDataFrame(rows, schema)
          .write.mode("append").saveAsTable(f"{C}.ops.dq_metrics"))


run_checks(CHECKS, RUN_ID, MEASURED_AT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO D — écarts au contrat d'interface

# COMMAND ----------

DEDUP_ORDERS = f"""
    SELECT * FROM (
        SELECT *, row_number() OVER (PARTITION BY order_line_id
                                     ORDER BY _source_file, _source_file_modification_time) AS rn
        FROM {C}.bronze.orders_raw
    ) WHERE rn = 1
"""

RULES = [
    ("ORDER_LINE_ID_UNIQUE", "orders_csv",
     "order_line_id est une cle unique",
     f"SELECT count(*), count(*) - count(DISTINCT order_line_id) FROM {C}.bronze.orders_raw"),

    ("ORDER_TS_PARSABLE", "orders_csv",
     "order_ts respecte le format yyyy-MM-dd HH:mm:ss",
     f"""SELECT count(*), sum(CASE WHEN to_timestamp(order_ts, 'yyyy-MM-dd HH:mm:ss') IS NULL
                                   THEN 1 ELSE 0 END)
         FROM ({DEDUP_ORDERS})"""),

    ("QUANTITY_POSITIVE", "orders_csv",
     "quantity est un entier strictement positif",
     # try_cast et non cast : le mode ANSI est actif sur serverless, un cast invalide
     # leve au lieu de rendre NULL — et c'est justement le NULL qu'on veut compter.
     f"""SELECT count(*), sum(CASE WHEN try_cast(quantity AS int) IS NULL
                                     OR try_cast(quantity AS int) <= 0 THEN 1 ELSE 0 END)
         FROM ({DEDUP_ORDERS})"""),

    ("UNIT_PRICE_NUMERIC", "orders_csv",
     "unit_price est un decimal a virgule, sans autre caractere",
     f"""SELECT count(*), sum(CASE WHEN unit_price RLIKE '^[0-9]+,[0-9]{{2}}$'
                                   THEN 0 ELSE 1 END)
         FROM ({DEDUP_ORDERS})"""),

    ("CURRENCY_ALWAYS_EUR", "orders_csv",
     "currency vaut toujours EUR",
     f"""SELECT count(*), sum(CASE WHEN currency <> 'EUR' THEN 1 ELSE 0 END)
         FROM ({DEDUP_ORDERS})"""),

    ("EVENT_TS_ISO8601", "events_jsonl",
     "event_ts est une chaine ISO 8601",
     f"""SELECT count(*), sum(CASE WHEN cast(event_ts AS string) RLIKE '^[0-9]{{12,13}}$'
                                   THEN 1 ELSE 0 END)
         FROM {C}.silver.event"""),
]

rows = []
for rule_code, source, text, sql in RULES:
    scope, violations = spark.sql(sql).first()
    violations = int(violations or 0)
    rows.append((rule_code, source, text, int(scope), violations,
                 violations / scope if scope else 0.0,
                 "OK" if violations == 0 else "VIOLATED", MEASURED_AT))

schema = ("rule_code string, source_name string, rule_text string, scope_rows bigint, "
          "violation_rows bigint, violation_rate double, status string, checked_at timestamp")

(spark.createDataFrame(rows, schema)
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{C}.ops.contract_violations"))

spark.sql(f"""COMMENT ON TABLE {C}.ops.contract_violations IS
    'Ecarts entre le contrat d interface declare par les equipes source et ce qu elles
     livrent reellement. Se corrige en parlant a la source, pas en changeant notre code.'""")

display(spark.table(f"{C}.ops.contract_violations").orderBy("rule_code"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO E — bilan des lignes sauvées

# COMMAND ----------

rescued = spark.sql(f"""
    -- Les commandes n'ont rien dans `_rescued_data` : le lecteur CSV a jete les jetons
    -- excedentaires sans les sauver. On compte donc les degats sur leur unique trace,
    -- `shipping_address` amputee de sa virgule, et l'exemple montre l'adresse mutilee
    -- plutot qu'un rescue qui n'existe pas.
    SELECT 'bronze.orders_raw'    AS table_name,
           'EXTRA_COLUMNS'        AS rescue_reason,
           count(*)               AS n_rows,
           min(shipping_address)  AS example_value
    FROM {C}.bronze.orders_raw WHERE shipping_address NOT LIKE '%,%'

    UNION ALL

    -- Le texte d'origine est dans `_corrupt_record` : un enregistrement illisible
    -- n'est pas un ecart au schema, c'est un echec d'analyse. `_rescued_data` est vide.
    SELECT 'bronze.events_raw', 'MALFORMED_JSON', count(*), min(_corrupt_record)
    FROM {C}.bronze.events_raw
    WHERE event_id IS NULL

    UNION ALL

    -- Lignes par ailleurs bien parsees, mais dont un champ est parti au rescue.
    -- Compte 0 si l'inference a resolu event_ts en chaine ; c'est une information.
    -- Compte 0 avec l'inference observee (`event_ts` en string) : c'est une information,
    -- pas un oubli. La ligne est filtree plus bas par `n_rows > 0`.
    SELECT 'bronze.events_raw', 'FIELD_TYPE_CONFLICT', count(*), min(_rescued_data)
    FROM {C}.bronze.events_raw
    WHERE event_id IS NOT NULL AND _rescued_data IS NOT NULL
""").withColumn("summarized_at", F.lit(MEASURED_AT).cast("timestamp"))

(rescued.filter(F.col("n_rows") > 0)
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{C}.ops.dq_rescued_summary"))

display(spark.table(f"{C}.ops.dq_rescued_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO F — documentation
# MAGIC
# MAGIC Un commentaire utile dit ce que la table **n'est pas** et quels pièges elle contient.

# COMMAND ----------

COMMENTS = {
    "silver.order_line": (
        "Lignes de commande typees, dedupliquees et validees. Une ligne = un produit dans "
        "une commande. Le CA ne se somme QUE sur is_revenue = true. 1 721 lignes portent un "
        "customer_id absent du referentiel (is_orphan_customer) : elles sont valides et "
        "comptent dans le CA. Les lignes rejetees sont dans ops.quarantine_order_line."),
    "silver.event": (
        "Evenements applicatifs aplatis et dedupliques sur event_id. Les enregistrements "
        "illisibles sont dans ops.quarantine_event. event_ts est normalise : la source livre "
        "deux formats. Le detail des paniers est dans silver.event_item."),
    "silver.event_item": (
        "Table fille de silver.event : un produit dans un evenement de panier. "
        "N'existe que pour add_to_cart, checkout_start et purchase."),
    "silver.customer_scd2": (
        "Historique des versions client. Une ligne = une version valide sur "
        "[valid_from, valid_to). valid_to NULL = version courante. Ne PAS joindre sans "
        "filtrer is_current ou sans condition temporelle : la table contient 25 570 lignes "
        "pour 25 080 clients."),
    "silver.seller_scd2": (
        "Historique des versions vendeur. Porte plan_code, donc le taux de commission. "
        "Joindre sur seller_id SANS condition temporelle fausse la commission historique "
        "de 45 765,97 EUR. Convention [valid_from, valid_to)."),
    "gold.fact_order_line": (
        "Fait au grain ligne de commande. Le CA ne se somme que sur is_revenue = true. "
        "seller_sk pointe vers la VERSION de vendeur en vigueur a la date de la commande."),
    "gold.dim_customer": (
        "Dimension client en etat courant (SCD1). 35 clients sont marques is_deleted : ils "
        "sont conserves pour l integrite referentielle des faits. L historique des versions "
        "est dans silver.customer_scd2."),
    "gold.dim_seller": (
        "Dimension vendeur historisee (SCD2). Une ligne = une VERSION de vendeur. "
        "Joindre les faits sur seller_sk, jamais sur seller_id seul."),
    "gold.dim_product": (
        "Catalogue produit en etat courant. 545 lignes de fait pointent vers un produit "
        "absent de cette table : utiliser une jointure GAUCHE."),
    "gold.dim_date": "Calendrier du 2025-12-01 au 2026-06-30. day_of_week : 1 = lundi.",
    "gold.ref_commission_plan": (
        "Taux de commission par plan vendeur. ATTENTION : cette table n est PAS historisee. "
        "Un changement de taux reecrirait tout l historique au prochain recalcul du fait."),
    "gold.agg_revenue_monthly": (
        "CA net et commission par mois x categorie de tete x vendeur. Construit uniquement "
        "sur les lignes de CA. Les produits orphelins sont regroupes sous 'UNKNOWN'."),
    "gold.agg_funnel_source": (
        "Entonnoir de conversion par source d acquisition, au grain session. Une session "
        "compte dans une etape si elle contient au moins un evenement de ce type."),
}

for table, comment in COMMENTS.items():
    spark.sql(f"COMMENT ON TABLE {C}.{table} IS '{comment}'")

for schema in ["silver", "gold"]:
    missing = spark.sql(f"""
        SELECT table_name FROM {C}.information_schema.tables
        WHERE table_schema = '{schema}' AND table_type IN ('MANAGED', 'EXTERNAL')
          AND (comment IS NULL OR trim(comment) = '')
    """).collect()
    print(f"{schema:8s} {len(missing)} table(s) sans commentaire : {[r[0] for r in missing]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO G — étiquetage des données personnelles

# COMMAND ----------

for table in ["silver.customer_scd2", "gold.dim_customer"]:
    for column in ["first_name", "last_name", "email", "zip_code"]:
        spark.sql(f"ALTER TABLE {C}.{table} ALTER COLUMN {column} SET TAGS ('pii' = 'true')")

display(spark.sql(f"""
    SELECT schema_name, table_name, column_name, tag_name, tag_value
    FROM {C}.information_schema.column_tags
    ORDER BY schema_name, table_name, column_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Réponses
# MAGIC
# MAGIC **1. Détecter une dérive que ce modèle ne voit pas**
# MAGIC
# MAGIC > `dq_metrics` compare une mesure à une constante. Une dérive, c'est une mesure
# MAGIC > comparée à **son propre passé** — et la table contient déjà ce passé, une ligne
# MAGIC > par exécution. Il suffit de l'interroger :
# MAGIC >
# MAGIC > ```sql
# MAGIC > SELECT table_name, check_name, metric_value,
# MAGIC >        avg(metric_value) OVER (PARTITION BY table_name, check_name
# MAGIC >                                ORDER BY measured_at
# MAGIC >                                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS moyenne_7j
# MAGIC > FROM novamarket.ops.dq_metrics
# MAGIC > ```
# MAGIC >
# MAGIC > Alerte si `metric_value > 2 × moyenne_7j`. C'est la règle que j'annonçais en M3, et
# MAGIC > elle attrape ce qu'un seuil fixe ne verra jamais : un taux d'orphelins qui passe de
# MAGIC > 0,6 % à 0,9 % reste très loin de n'importe quel seuil absolu raisonnable, alors que
# MAGIC > c'est un dérapage de 50 % qui mérite un coup d'œil.
# MAGIC >
# MAGIC > Le prérequis, c'est d'avoir gardé l'historique. Une table de qualité en `overwrite`
# MAGIC > ne peut rien détecter de tout ça — d'où le choix de l'`append` et de la colonne
# MAGIC > `run_id`.
# MAGIC
# MAGIC **2. Avant l'écriture, après, ou les deux ?**
# MAGIC
# MAGIC > Les deux, et ce ne sont pas les mêmes contrôles.
# MAGIC >
# MAGIC > **Avant** (sur le DataFrame, avant `write`) : c'est le seul moment où on peut
# MAGIC > encore *ne pas écrire*. Réservé aux contrôles bloquants — référentiel vide, schéma
# MAGIC > inattendu, volumétrie effondrée. Écrire puis constater qu'il ne fallait pas, ça
# MAGIC > oblige à un `RESTORE` ; ne pas écrire, ça n'oblige à rien.
# MAGIC >
# MAGIC > **Après** (sur la table) : c'est le seul moment où on mesure ce qui est réellement
# MAGIC > là, y compris les effets du moteur — un `MERGE` mal écrit, une partition oubliée.
# MAGIC > C'est aussi ce qui permet de contrôler une table qu'on n'a pas produite.
# MAGIC >
# MAGIC > Ce module fait des contrôles *après*, ce qui est le bon choix pour de
# MAGIC > l'observabilité. Ça ne dispense pas des contrôles bloquants en amont : les `assert`
# MAGIC > du corrigé de M5 sur la jointure temporelle en sont, et ils valent tous les
# MAGIC > tableaux de bord du monde.
# MAGIC
# MAGIC **3. Un `FAIL` doit-il arrêter le pipeline ?**
# MAGIC
# MAGIC > Trois cas, présents dans ce projet :
# MAGIC >
# MAGIC > **Oui, immédiatement** — les invariants. `duplicate_keys > 0` ou `chain_breaks > 0`
# MAGIC > signifie que le code est faux. Continuer, c'est propager une erreur dans le gold et
# MAGIC > dans les décisions qui en découlent. Coût de l'arrêt : un retard. Coût de la
# MAGIC > poursuite : un chiffre faux que personne ne saura dater.
# MAGIC >
# MAGIC > **Non, on alerte** — les taux d'anomalie. 1 900 lignes orphelines au lieu de 1 721,
# MAGIC > c'est une information pour demain matin, pas une raison de priver l'entreprise de
# MAGIC > ses données du jour.
# MAGIC >
# MAGIC > **Oui, mais en amont** — le référentiel vide. Ici, arrêter ne suffit pas : il faut
# MAGIC > arrêter **avant** d'écrire, sinon on a déjà remplacé 8 000 produits par zéro et le
# MAGIC > gold est mort. C'est le cas qui justifie à lui seul de ne pas mettre tous ses
# MAGIC > contrôles en aval.
# MAGIC >
# MAGIC > La règle sous-jacente : on bloque sur ce qui est **irréversible ou contagieux**,
# MAGIC > on alerte sur ce qui est simplement anormal.
# MAGIC
# MAGIC **4. Ce qu'on envoie à l'équipe source**
# MAGIC
# MAGIC > Pas 3 452 lignes. Un message court avec quatre éléments :
# MAGIC >
# MAGIC > 1. **La règle du contrat** qu'ils ont écrite, citée mot pour mot.
# MAGIC > 2. **Le taux**, pas le volume brut : « 1,2 % des lignes de commande sont émises en
# MAGIC >    double » se discute ; « 3 452 doublons » se noie.
# MAGIC > 3. **Trois exemples reproductibles** : `order_line_id`, fichier source, ligne.
# MAGIC >    Un développeur qui peut reproduire corrige ; un développeur à qui on demande de
# MAGIC >    chercher répond qu'il ne reproduit pas.
# MAGIC > 4. **La conséquence métier**, quantifiée. Sans elle, la demande est classée en
# MAGIC >    « confort de l'équipe data ».
# MAGIC >
# MAGIC > Et une chose à ne surtout pas faire : présenter ça comme une liste de reproches.
# MAGIC > Sur les six règles, une est respectée — le dire aussi. Le but est d'obtenir un
# MAGIC > correctif, pas d'avoir raison.
# MAGIC
# MAGIC **5. À quoi sert l'étiquette `pii`**
# MAGIC
# MAGIC > Seule, à rien : c'est une chaîne de caractères dans un catalogue. Elle devient utile
# MAGIC > parce qu'elle est **interrogeable** et donc automatisable :
# MAGIC >
# MAGIC > - **Masquage dynamique** : une fonction de masquage appliquée par requête sur
# MAGIC >   `information_schema.column_tags`, plutôt que colonne par colonne à la main.
# MAGIC > - **Audit RGPD** : « où sont nos données personnelles ? » devient une requête SQL
# MAGIC >   au lieu d'un fichier Excel maintenu par quelqu'un qui est parti.
# MAGIC > - **Contrôle de non-régression** : détecter qu'une nouvelle table gold expose une
# MAGIC >   colonne étiquetée `pii` en amont, et le signaler avant la mise en production.
# MAGIC > - **Purge ciblée** : sur demande d'effacement, savoir quoi anonymiser sans relire
# MAGIC >   tout le modèle.
# MAGIC >
# MAGIC > L'étiquette ne protège pas. Elle rend la protection **systématique au lieu
# MAGIC > d'artisanale**, ce qui est la seule façon qu'elle survive à trois changements
# MAGIC > d'équipe.
