# Databricks notebook source
# MAGIC %md
# MAGIC # Complément M3 — Jointures, manipulations et agrégations
# MAGIC
# MAGIC **Section 3 de l'examen · 22 %** — la section la plus lourde du guide.
# MAGIC
# MAGIC Le projet exerce les jointures gauche et diffusée, l'explosion de tableaux et la
# MAGIC déduplication. Le guide en nomme une dizaine d'autres. Ce notebook les passe toutes
# MAGIC sur les données de NovaMarket, avec un contrôle auto-vérifiant à chaque étape.
# MAGIC
# MAGIC Il n'a pas de grader : chaque cellule vérifie son propre résultat. Ce qui compte
# MAGIC ici, c'est de **savoir prédire** le nombre de lignes avant d'exécuter.

# COMMAND ----------

from pyspark.sql import functions as F, Window as W

CATALOG = "novamarket"

orders = spark.table(f"{CATALOG}.silver.order_line")
products = spark.table(f"{CATALOG}.gold.dim_product")
sellers = spark.table(f"{CATALOG}.gold.dim_seller").filter("is_current")

N_ORDERS = orders.count()
print(f"lignes de commande : {N_ORDERS:,}".replace(",", " "))


def check(label, got, expected=None, rule=None):
    ok = (got == expected) if expected is not None else rule(got)
    print(f"{'OK ' if ok else 'KO '} {label:52s} {got:>10,}".replace(",", " "))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Les six formes de jointure
# MAGIC
# MAGIC **Avant d'exécuter chaque cellule, écris le nombre de lignes que tu attends.**
# MAGIC C'est le seul exercice qui compte : le code, tu l'as déjà écrit vingt fois.

# COMMAND ----------

# --- inner : ne garde que ce qui matche des deux cotes ---------------------
inner = orders.join(products, on="product_id", how="inner")
check("inner join (perd les produits orphelins)", inner.count(),
      rule=lambda n: n < N_ORDERS)

# --- left : garde tout le cote gauche --------------------------------------
left = orders.join(products, on="product_id", how="left")
check("left join (conserve tout)", left.count(), N_ORDERS)

# L'ecart entre les deux, c'est exactement le nombre de lignes orphelines.
check("ecart inner/left = lignes orphelines", N_ORDERS - inner.count(),
      orders.filter("is_orphan_product").count())

# COMMAND ----------

# --- left_anti : ce qui est a gauche et PAS a droite -----------------------
# La facon la plus lisible de compter des orphelins, et la plus rapide :
# pas de colonnes ramenees, donc pas de shuffle inutile.
anti = orders.join(products, on="product_id", how="left_anti")
check("left_anti join = les orphelins, directement", anti.count(),
      orders.filter("is_orphan_product").count())

# --- left_semi : ce qui est a gauche ET a droite, sans ramener les colonnes -
semi = orders.join(products, on="product_id", how="left_semi")
check("left_semi join (filtre, ne joint pas)", semi.count(), inner.count())
print(f"     colonnes du semi : {len(semi.columns)} (identique a la table de gauche)")

# COMMAND ----------

# --- broadcast : force la diffusion de la petite table ---------------------
# Meme resultat que le left, plan d'execution different : pas de shuffle du
# gros cote. A verifier dans .explain().
broadcast = orders.join(F.broadcast(sellers), on="seller_id", how="left")
check("broadcast join (meme resultat, autre plan)", broadcast.count(), N_ORDERS)

broadcast.explain(mode="simple")

# COMMAND ----------

# --- jointure sur cles multiples ------------------------------------------
# Piege classique : joindre sur une seule cle quand la relation en demande deux
# multiplie silencieusement les lignes.
multi = (orders.alias("o")
         .join(products.alias("p"),
               (F.col("o.product_id") == F.col("p.product_id"))
               & (F.col("o.seller_id") == F.col("p.seller_id")),
               "inner"))
check("jointure sur 2 cles (produit ET vendeur)", multi.count(),
      rule=lambda n: n <= inner.count())

# COMMAND ----------

# --- cross join : produit cartesien ---------------------------------------
# Spark le refuse par defaut, et c'est une protection, pas une limitation.
mois = spark.sql("SELECT explode(sequence(1, 12)) AS mois")
pays = orders.select("shipping_country").distinct()

cross = mois.crossJoin(pays)
check("cross join 12 mois x pays", cross.count(), 12 * pays.count())

# Cas d'usage legitime : fabriquer un squelette complet pour ne pas avoir de
# trous dans un rapport (les combinaisons sans donnee doivent apparaitre a zero).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `union`, `unionAll`, `unionByName`
# MAGIC
# MAGIC Le piège le plus coûteux de la section, parce qu'il ne lève aucune erreur.

# COMMAND ----------

fr = orders.filter("shipping_country = 'FR'")
be = orders.filter("shipping_country = 'BE'")

check("union (en Spark, ne deduplique PAS)", fr.union(be).count(), fr.count() + be.count())
check("unionAll (alias de union depuis Spark 2)", fr.unionAll(be).count(),
      fr.count() + be.count())
check("union + distinct (deduplication explicite)",
      fr.union(be).distinct().count(), fr.count() + be.count())

# MAGIC %md
# MAGIC **À retenir** : en SQL ANSI, `UNION` déduplique et `UNION ALL` non. Dans l'API
# MAGIC DataFrame de Spark, `union` et `unionAll` font **la même chose** et ne dédupliquent
# MAGIC ni l'une ni l'autre — `unionAll` n'est qu'un alias historique. Pour dédupliquer,
# MAGIC il faut un `.distinct()` explicite.
# MAGIC
# MAGIC En SQL (`spark.sql("... UNION ...")`), la sémantique ANSI s'applique et `UNION`
# MAGIC déduplique bien. Deux comportements différents pour le même mot selon l'API :
# MAGIC c'est exactement le genre de détail qui départage deux options d'un QCM.

# COMMAND ----------

# --- union par position contre union par nom ------------------------------
a = orders.select("order_line_id", "net_amount")
b = orders.select("net_amount", "order_line_id")     # colonnes inversees !

par_position = a.union(b)          # colle net_amount dans order_line_id...
par_nom = a.unionByName(b)         # aligne sur les noms

print("union() aligne sur la POSITION :")
par_position.show(2, truncate=False)
print("unionByName() aligne sur le NOM :")
par_nom.show(2, truncate=False)

# MAGIC %md
# MAGIC `union()` n'a pas échoué. Il a produit des données fausses, silencieusement, parce
# MAGIC que les deux colonnes étaient compatibles en type. **Utilise `unionByName` par
# MAGIC défaut** ; garde `union` pour les cas où tu contrôles l'ordre des colonnes des deux
# MAGIC côtés.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Manipulations de colonnes et de lignes

# COMMAND ----------

manip = (
    orders
    .withColumn("annee_mois", F.date_format("order_date", "yyyy-MM"))          # ajout
    .withColumnRenamed("net_amount", "ca_net")                                 # renommage
    .drop("shipping_address")                                                  # suppression
    .filter(F.col("ca_net") > 0)                                               # filtre
    # split : decouper une chaine en tableau, puis extraire un element
    .withColumn("_parts", F.split(F.col("order_line_id"), "-"))
    .withColumn("numero_ligne", F.element_at(F.col("_parts"), -1).cast("int"))
    .drop("_parts")
)

manip.select("order_line_id", "numero_ligne", "annee_mois", "ca_net").show(5, truncate=False)

# COMMAND ----------

# --- explode : un tableau -> une ligne par element -------------------------
items = spark.table(f"{CATALOG}.silver.event")

with_items = items.filter(F.col("n_items") > 0)
print(f"evenements avec items : {with_items.count():,}".replace(",", " "))

# MAGIC %md
# MAGIC `explode` supprime les lignes dont le tableau est vide ou nul ; `explode_outer`
# MAGIC les conserve avec un `null`. Le même couple existe en version positionnelle
# MAGIC (`posexplode` / `posexplode_outer`). C'est le choix qu'on a fait en M3 pour
# MAGIC `silver.event_item` — une table fille ne contient que des lignes filles.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Agrégations
# MAGIC
# MAGIC Les quatre que le guide nomme, plus celles qu'on utilise tout le temps.

# COMMAND ----------

display(
    orders.agg(
        F.count("*").alias("lignes"),
        F.count("customer_id").alias("clients_non_nuls"),        # count ignore les nulls
        F.countDistinct("customer_id").alias("clients_distincts"),
        F.approx_count_distinct("customer_id").alias("clients_approx"),
        F.mean("net_amount").alias("moyenne"),
        F.expr("percentile_approx(net_amount, 0.5)").alias("mediane"),
        F.stddev("net_amount").alias("ecart_type"),
        F.min("order_date").alias("debut"),
        F.max("order_date").alias("fin"),
    )
)

# MAGIC %md
# MAGIC **`countDistinct` contre `approx_count_distinct`** : le premier est exact et exige
# MAGIC un *shuffle* complet ; le second utilise HyperLogLog, avec une erreur par défaut
# MAGIC d'environ 5 % et un coût très inférieur. Sur un tableau de bord qui affiche
# MAGIC « 24 700 clients actifs », l'approximation est parfaitement acceptable. Sur un
# MAGIC décompte réglementaire, non.
# MAGIC
# MAGIC **`count("*")` contre `count("colonne")`** : le premier compte les lignes, le second
# MAGIC les valeurs **non nulles**. L'écart entre les deux est un compteur de nulls gratuit.

# COMMAND ----------

# --- summary : le resume statistique en une commande -----------------------
# describe() donne count/mean/stddev/min/max ; summary() ajoute les quartiles
# et accepte les percentiles qu'on lui demande.
display(orders.select("quantity", "unit_price", "net_amount").summary())

# COMMAND ----------

# --- agregation par groupe + fenetre ---------------------------------------
par_mois = (orders.filter("is_revenue")
            .groupBy(F.date_format("order_date", "yyyy-MM").alias("mois"))
            .agg(F.sum("net_amount").alias("ca"),
                 F.countDistinct("order_id").alias("commandes"))
            .withColumn("ca_cumule", F.sum("ca").over(W.orderBy("mois")))
            .withColumn("evolution", F.round(
                F.col("ca") / F.lag("ca").over(W.orderBy("mois")) - 1, 3))
            .orderBy("mois"))

display(par_mois)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ce qu'il faut savoir prédire
# MAGIC
# MAGIC Si tu sais répondre à ces cinq questions sans exécuter, la section 3 est acquise.
# MAGIC
# MAGIC 1. Une jointure interne entre une table de 100 lignes et une de 10 lignes partageant
# MAGIC    5 clés : combien de lignes en sortie ? Et si la table de droite a des doublons
# MAGIC    sur la clé ?
# MAGIC 2. `dfA.union(dfB)` où B a les mêmes colonnes dans un ordre différent : erreur, ou
# MAGIC    données fausses ?
# MAGIC 3. `count("colonne")` sur une colonne à 30 % de nulls : quel écart avec `count("*")` ?
# MAGIC 4. `explode` sur une colonne où 40 % des tableaux sont vides : combien de lignes en
# MAGIC    moins ?
# MAGIC 5. Une jointure gauche où la table de droite a deux lignes pour une clé : la table de
# MAGIC    gauche grossit-elle ?
