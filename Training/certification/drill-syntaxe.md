# Drill de syntaxe — 100 questions

Écrit le 2 septembre 2026. **PySpark et SQL, syntaxe et choix de fonction.**
Pas de concept, pas de cas limite, pas de pondération selon tes faiblesses : la surface
est balayée uniformément.

Le corrigé est en fin de fichier, avec une colonne *thème* pour repérer les familles où
tu perds des points. Réponds à tout avant de le déplier.

Note tes réponses au fil de l'eau. Compte 60 à 75 minutes.

---

## Résultat — 2 septembre 2026

**88 / 101 — 87 %.**

| Thème | Score |
|---|---|
| Imbriqué | **4/8** |
| Optimisation | **3/7** |
| Écriture | **1/4** |
| Lecture | 2/3 |
| Déclaratif | 3/4 |
| *Les 22 autres familles* | *sans faute* |

> **Huit des treize ratés sont des options qui n'existent pas** : `@dlt.materialized_view`,
> `F.json_extract`, `schemaEvolution`, `forceSchema`, `OPTIMIZE … COMPUTE STATS`,
> `option("partition", …)`, `F.col("_file")`, `F.map`. Ce n'est pas un trou de
> connaissance — c'est le réflexe d'élimination qui n'a pas joué.

---

**1.** Renommer une colonne existante.

- **A.** `df.rename("ancien", "nouveau")`
- **B.** `df.withColumn("nouveau", "ancien")`
- **C.** `df.withColumnRenamed("ancien", "nouveau")`
- **D.** `df.alias("ancien", "nouveau")`

Réponse candidat : C  ✅

**2.** Ajouter une colonne constante valant `1`.

- **A.** `df.withColumn("n", 1)`
- **B.** `df.withColumn("n", F.col(1))`
- **C.** `df.addColumn("n", 1)`
- **D.** `df.withColumn("n", F.lit(1))`

Réponse candidat : D  ✅

**3.** Déclarer la source d'un flux Auto Loader lisant des fichiers JSON.

- **A.** `.format("autoloader").option("type", "json")`
- **B.** `.format("cloudFiles").option("cloudFiles.format", "json")`
- **C.** `.format("json").option("cloudFiles", "true")`
- **D.** `.format("stream").option("fileFormat", "json")`

Réponse candidat : B  ✅

**4.** Supprimer une colonne d'un DataFrame.

- **A.** `df.drop("c")`
- **B.** `df.remove("c")`
- **C.** `df.delete("c")`
- **D.** `df.select(-"c")`

Réponse candidat : A  ✅

**5.** Écrire un DataFrame dans une table gouvernée, en ajoutant aux lignes existantes.

- **A.** `df.write.append().saveAsTable("cat.sch.t")`
- **B.** `df.save("cat.sch.t", mode="add")`
- **C.** `df.write.mode("append").saveAsTable("cat.sch.t")`
- **D.** `df.write.insert("cat.sch.t")`

Réponse candidat : C  ✅

**6.** Convertir une chaîne en entier **sans faire échouer la requête** si la conversion est impossible.

- **A.** `F.col("c").cast("int")`
- **B.** `F.int(F.col("c"))`
- **C.** `F.safe_cast(F.col("c"), "int")`
- **D.** `F.col("c").try_cast("int")`

Réponse candidat : D  ✅

**7.** Déplier un tableau en conservant une ligne lorsque le tableau est vide.

- **A.** `F.explode("arr")`
- **B.** `F.flatten("arr")`
- **C.** `F.posexplode("arr")`
- **D.** `F.explode_outer("arr")`

Réponse candidat : D  ✅

**8.** Charger des fichiers CSV avec en-tête depuis un volume, par la commande SQL incrémentale.

- **A.** `COPY INTO t FROM '/Volumes/…' FILEFORMAT = CSV FORMAT_OPTIONS ('header' = 'true')`
- **B.** `COPY INTO t FROM '/Volumes/…' TYPE = CSV WITH HEADER`
- **C.** `LOAD DATA INPATH '/Volumes/…' INTO TABLE t FORMAT CSV`
- **D.** `COPY t FROM '/Volumes/…' USING CSV OPTIONS (header true)`

Réponse candidat : A  ✅

**9.** Forcer la diffusion de la petite table dans une jointure.

- **A.** `dfA.broadcastJoin(dfB, "id")`
- **B.** `dfA.join(dfB, "id", hint="broadcast")`
- **C.** `dfA.join(F.broadcast(dfB), "id")`
- **D.** `F.broadcast(dfA.join(dfB, "id"))`

Réponse candidat : C  ✅

**10.** Calculer une somme par région, en nommant le résultat.

- **A.** `df.groupBy("region").sum("montant").alias("ca")`
- **B.** `df.groupBy("region").agg(F.sum("montant").alias("ca"))`
- **C.** `df.agg("region", F.sum("montant") AS "ca")`
- **D.** `df.groupBy("region").agg({"montant": "sum"}).alias("ca")`

Réponse candidat : B  ✅

**11.** Importer la classe permettant de définir une fenêtre.

- **A.** `from pyspark.sql.window import Window`
- **B.** `from pyspark.sql import Window`
- **C.** `from pyspark.window import Window`
- **D.** `import pyspark.sql.functions.Window`

Réponse candidat : A  ✅

**12.** Numéroter les lignes de chaque groupe, du plus récent au plus ancien.

- **A.** `F.row_number(Window.partitionBy("id").orderBy("d"))`
- **B.** `Window.partitionBy("id").orderBy("d").row_number()`
- **C.** `F.rank().over(Window.partitionBy("id").sort("d"))`
- **D.** `F.row_number().over(Window.partitionBy("id").orderBy(F.col("d").desc()))`

Réponse candidat : D  ✅

**13.** Remplacer toutes les virgules par des points dans une colonne texte.

- **A.** `F.regexp_replace(F.col("p"), ",", ".")`
- **B.** `F.col("p").replace(",", ".")`
- **C.** `F.replace(F.col("p"), ",", ".")`
- **D.** `F.translate("p", ",", ".", 1)`

Réponse candidat : A  ✅

**14.** Rendre la première valeur non nulle parmi trois colonnes.

- **A.** `F.first_non_null("a", "b", "c")`
- **B.** `F.nvl3("a", "b", "c")`
- **C.** `F.coalesce("a", "b", "c")`
- **D.** `F.ifnull("a", "b", "c")`

Réponse candidat : C  ✅

**15.** Appliquer une condition avec valeur par défaut.

- **A.** `F.if(cond, x, y)`
- **B.** `F.case(cond).then(x).else(y)`
- **C.** `F.when(cond, x).otherwise(y)`
- **D.** `F.when(cond, x).else(y)`

Réponse candidat : C  ✅

**16.** Fusionner un delta dans une table cible, sur la clé métier, en SQL.

- **A.** `MERGE cible USING source ON id WHEN MATCH UPDATE`
- **B.** `UPSERT INTO cible FROM source ON id`
- **C.** `MERGE INTO cible SELECT * FROM source ON KEY id`
- **D.** `MERGE INTO cible c USING source s ON c.id = s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *`

Réponse candidat : D  ✅

**17.** Lire une table gouvernée dans un DataFrame.

- **A.** `spark.load("cat.sch.t")`
- **B.** `spark.read.table_name("cat.sch.t")`
- **C.** `spark.catalog("cat.sch.t")`
- **D.** `spark.table("cat.sch.t")`

Réponse candidat : D  ✅

**18.** Déclarer l'emplacement du checkpoint d'un flux en écriture.

- **A.** `.writeStream.option("checkpoint", chemin)`
- **B.** `.writeStream.checkpoint(chemin)`
- **C.** `.writeStream.option("checkpointLocation", chemin)`
- **D.** `.writeStream.option("cloudFiles.checkpoint", chemin)`

Réponse candidat : C  ✅

**19.** Traiter tout ce qui attend puis arrêter le flux.

- **A.** `.trigger(once=True)`
- **B.** `.trigger(processingTime="0 seconds")`
- **C.** `.trigger(continuous="1 second")`
- **D.** `.trigger(availableNow=True)`

Réponse candidat : D  ✅

**20.** Compter les valeurs distinctes de façon approchée.

- **A.** `F.countDistinct("c", approx=True)`
- **B.** `F.count_distinct_approx("c")`
- **C.** `F.approx_count_distinct("c", 0.01)`
- **D.** `F.hll_count("c")`

Réponse candidat : C  ✅

**21.** Empiler deux DataFrames en alignant les colonnes **par leur nom**.

- **A.** `dfA.union(dfB)`
- **B.** `dfA.unionByName(dfB)`
- **C.** `dfA.unionAll(dfB)`
- **D.** `dfA.merge(dfB, on="name")`

Réponse candidat : B  ✅

**22.** Réorganiser physiquement une table existante selon une colonne, à l'ancienne.

- **A.** `OPTIMIZE t CLUSTER BY (jour)`
- **B.** `OPTIMIZE t ZORDER BY (jour)`
- **C.** `REORG TABLE t ON (jour)`
- **D.** `ALTER TABLE t ZORDER (jour)`

Réponse candidat : B  ✅

**23.** Déclarer le rangement recommandé sur une table neuve.

- **A.** `CREATE TABLE t (…) PARTITIONED BY (jour)`
- **B.** `CREATE TABLE t (…) ZORDER BY (jour)`
- **C.** `CREATE TABLE t (…) SORTED BY (jour)`
- **D.** `CREATE TABLE t (…) CLUSTER BY (jour)`

Réponse candidat : D  ✅

**24.** Consulter l'historique des écritures d'une table.

- **A.** `SHOW HISTORY t`
- **B.** `SELECT * FROM t.history`
- **C.** `DESCRIBE HISTORY t`
- **D.** `DESCRIBE DETAIL t`

Réponse candidat : C  ✅

**25.** Revenir à une version antérieure d'une table.

- **A.** `RESTORE TABLE t TO VERSION AS OF 12`
- **B.** `ROLLBACK TABLE t TO VERSION 12`
- **C.** `UNDO TABLE t VERSION 12`
- **D.** `REVERT TABLE t VERSION 12`

Réponse candidat : A  ✅

**26.** Lire une table telle qu'elle était à une version donnée.

- **A.** `SELECT * FROM t VERSION AS OF 12`
- **B.** `SELECT * FROM t AT VERSION 12`
- **C.** `SELECT * FROM t@v12 HISTORY`
- **D.** `SELECT * FROM HISTORY(t, 12)`

Réponse candidat : A  ✅

**27.** Accorder la lecture d'une table à un groupe.

- **A.** `GRANT READ ON t TO groupe`
- **B.** `GRANT SELECT ON TABLE cat.sch.t TO \`analystes\``
- **C.** `ALLOW SELECT ON cat.sch.t FOR \`analystes\``
- **D.** `GRANT ACCESS SELECT cat.sch.t TO \`analystes\``

Réponse candidat : B  ✅

**28.** Créer un volume dans Unity Catalog.

- **A.** `CREATE VOLUME cat.sch.vol`
- **B.** `CREATE EXTERNAL LOCATION vol IN cat.sch`
- **C.** `CREATE STORAGE cat.sch.vol`
- **D.** `CREATE MOUNT cat.sch.vol`

Réponse candidat : A  ✅

**29.** Déclarer une table matérialisée dans un pipeline déclaratif en Python.

- **A.** `@dlt.materialized_view`
- **B.** `@dlt.table`
- **C.** `@dlt.create_table`
- **D.** `@pipeline.table`

Réponse candidat : A  ❌ — juste : **B**

> `@dlt.table` déclare un jeu de données ; qu'il soit matérialisé ou alimenté par incréments dépend de la **lecture** (`dlt.read` ou `dlt.read_stream`), pas du décorateur. `@dlt.materialized_view` n'existe pas.

**30.** Déclarer une attente qui **écarte** la ligne fautive.

- **A.** `@dlt.expect("nom", "cond")`
- **B.** `@dlt.expect_or_fail("nom", "cond")`
- **C.** `@dlt.expect_or_drop("nom", "cond")`
- **D.** `@dlt.drop_if("nom", "cond")`

Réponse candidat : C  ✅

**31.** L'équivalent SQL d'une attente qui fait échouer le lot.

- **A.** `CONSTRAINT c EXPECT (cond) ON VIOLATION FAIL UPDATE`
- **B.** `CONSTRAINT c EXPECT (cond) ON VIOLATION DROP ROW`
- **C.** `CONSTRAINT c CHECK (cond) ON FAIL ABORT`
- **D.** `EXPECT c (cond) OTHERWISE FAIL`

Réponse candidat : A  ✅

**32.** Lire une table du même pipeline en créant la dépendance dans le graphe.

- **A.** `spark.read.table("brut")`
- **B.** `spark.table("LIVE.brut")`
- **C.** `dlt.read_stream("brut")`
- **D.** `dlt.load("brut")`

Réponse candidat : C  ✅

**33.** Déclarer un paramètre attendu par un carnet.

- **A.** `dbutils.params.declare("jour")`
- **B.** `dbutils.widgets.text("jour", "")`
- **C.** `dbutils.notebook.param("jour")`
- **D.** `spark.conf.set("jour", "")`

Réponse candidat : B  ✅

**34.** Lire la valeur d'un paramètre reçu.

- **A.** `dbutils.widgets.read("jour")`
- **B.** `dbutils.widgets.value("jour")`
- **C.** `spark.conf.get("jour")`
- **D.** `dbutils.widgets.get("jour")`

Réponse candidat : D  ✅

**35.** Publier une valeur pour une tâche en aval.

- **A.** `dbutils.jobs.taskValues.set(key="n", value=12)`
- **B.** `dbutils.jobs.setValue("n", 12)`
- **C.** `dbutils.notebook.exit({"n": 12})`
- **D.** `spark.conf.set("task.n", 12)`

Réponse candidat : A  ✅

**36.** Valider un bundle sans rien déployer.

- **A.** `databricks bundle check -t dev`
- **B.** `databricks bundle validate -t dev`
- **C.** `databricks bundle deploy --dry-run -t dev`
- **D.** `databricks bundle verify -t dev`

Réponse candidat : B  ✅

**37.** Supprimer les ressources créées par un bundle.

- **A.** `databricks bundle clean -t dev`
- **B.** `databricks bundle destroy -t dev`
- **C.** `databricks bundle remove -t dev`
- **D.** `databricks bundle delete -t dev`

Réponse candidat : B  ✅

**38.** Dédupliquer sur un sous-ensemble de colonnes.

- **A.** `df.distinct(["a", "b"])`
- **B.** `df.deduplicate("a", "b")`
- **C.** `df.dropDuplicates(["a", "b"])`
- **D.** `df.unique(subset=["a", "b"])`

Réponse candidat : C  ✅

**39.** Remplacer les valeurs absentes d'une colonne numérique par zéro.

- **A.** `df.replaceNull("montant", 0)`
- **B.** `df.na.zero("montant")`
- **C.** `df.withColumn("montant", F.nullif("montant", 0))`
- **D.** `df.fillna(0, subset=["montant"])`

Réponse candidat : D  ✅

**40.** Supprimer les lignes ayant au moins une valeur absente.

- **A.** `df.filter(F.col("*").isNotNull())`
- **B.** `df.removeNulls()`
- **C.** `df.dropna()`
- **D.** `df.na.remove()`

Réponse candidat : C  ✅

**41.** Découper une chaîne sur un séparateur.

- **A.** `F.explode("adresse", ";")`
- **B.** `F.substring("adresse", ";")`
- **C.** `F.split("adresse", ";")`
- **D.** `F.tokenize("adresse", ";")`

Réponse candidat : C  ✅

**42.** Concaténer deux colonnes avec un tiret entre elles.

- **A.** `F.concat_ws("-", "a", "b")`
- **B.** `F.concat("a", "-", "b")`
- **C.** `F.join("-", "a", "b")`
- **D.** `F.merge("a", "b", sep="-")`

Réponse candidat : A  ✅

**43.** Analyser une chaîne en horodatage, en tolérant l'échec.

- **A.** `F.to_timestamp("c", "yyyy-MM-dd HH:mm:ss")`
- **B.** `F.try_to_timestamp("c", F.lit("yyyy-MM-dd HH:mm:ss"))`
- **C.** `F.timestamp("c", "yyyy-MM-dd HH:mm:ss")`
- **D.** `F.col("c").try_cast("timestamp", "yyyy-MM-dd HH:mm:ss")`

Réponse candidat : B  ✅

**44.** Formater une date en chaîne `2026-03`.

- **A.** `F.format_date("jour", "yyyy-MM")`
- **B.** `F.to_char("jour", "yyyy-MM")`
- **C.** `F.date_format("jour", "yyyy-MM")`
- **D.** `F.strftime("jour", "%Y-%m")`

Réponse candidat : C  ✅

**45.** Calculer le nombre de jours entre deux dates.

- **A.** `F.days_between("fin", "debut")`
- **B.** `F.datediff("fin", "debut")`
- **C.** `F.date_sub("fin", "debut")`
- **D.** `F.diff("fin", "debut", "day")`

Réponse candidat : B  ✅

**46.** Extraire un champ d'une colonne JSON stockée en texte, sans schéma.

- **A.** `F.from_json("payload", "$.client")`
- **B.** `F.json_extract("payload", "client")`
- **C.** `F.col("payload")["client"]`
- **D.** `F.get_json_object("payload", "$.client")`

Réponse candidat : B  ❌ — juste : **D**

> Sans schéma : `F.get_json_object(col, "$.chemin")`. Avec schéma : `F.from_json(col, schema)`. `F.json_extract` n'existe pas.

**47.** Convertir une colonne texte JSON en structure typée.

- **A.** `F.to_json(F.col("payload"), schema)`
- **B.** `F.parse_json(F.col("payload"), schema)`
- **C.** `F.col("payload").cast(schema)`
- **D.** `F.from_json(F.col("payload"), schema)`

Réponse candidat : D  ✅

**48.** Compter les éléments d'une colonne tableau.

- **A.** `F.length("arr")`
- **B.** `F.count("arr")`
- **C.** `F.size("arr")`
- **D.** `F.cardinality("arr", strict=True)`

Réponse candidat : C  ✅

**49.** Tester la présence d'une valeur dans un tableau.

- **A.** `F.array_contains("motifs", "INVALID")`
- **B.** `F.contains("motifs", "INVALID")`
- **C.** `F.col("motifs").has("INVALID")`
- **D.** `F.in_array("INVALID", "motifs")`

Réponse candidat : B  ❌ — juste : **A**

> Sur un **tableau**, c'est `F.array_contains`. `F.contains` existe mais porte sur des **chaînes**.

**50.** Retirer les valeurs nulles d'une colonne tableau.

- **A.** `F.array_remove("arr", None)`
- **B.** `F.array_compact("arr")`
- **C.** `F.dropna("arr")`
- **D.** `F.array_clean("arr")`

Réponse candidat : B  ✅

**51.** Jointure gauche entre deux DataFrames sur une clé commune.

- **A.** `dfA.leftJoin(dfB, "id")`
- **B.** `dfA.join(dfB, "id", "left")`
- **C.** `dfA.join(dfB, on="id", type="left")`
- **D.** `dfA.merge(dfB, how="left", on="id")`

Réponse candidat : B  ✅

**52.** Jointure sur deux clés à la fois.

- **A.** `dfA.join(dfB, ["a", "b"])`
- **B.** `dfA.join(dfB, "a" and "b")`
- **C.** `dfA.join(dfB, on="a,b")`
- **D.** `dfA.join(dfB, keys=("a", "b"))`

Réponse candidat : A  ✅

**53.** Ne garder que les lignes de gauche **sans** correspondance à droite.

- **A.** `dfA.join(dfB, "id", "left_anti")`
- **B.** `dfA.join(dfB, "id", "left_semi")`
- **C.** `dfA.join(dfB, "id", "outer").filter(…)`
- **D.** `dfA.subtract(dfB)`

Réponse candidat : A  ✅

**54.** Compter les lignes vérifiant une condition, dans une agrégation.

- **A.** `F.count(F.col("m") > 1000)`
- **B.** `F.count_if(F.col("m") > 1000)`
- **C.** `F.sum(F.col("m") > 1000)`
- **D.** `F.countWhere("m > 1000")`

Réponse candidat : B  ✅

**55.** Décaler d'une ligne en arrière dans une fenêtre.

- **A.** `F.lag("m", 1).over(w)`
- **B.** `F.previous("m", 1).over(w)`
- **C.** `F.shift("m", -1).over(w)`
- **D.** `F.lead("m", -1).over(w)`

Réponse candidat : A  ✅

**56.** Définir un cadre de fenêtre allant du début du groupe jusqu'à la ligne courante.

- **A.** `w.rowsBetween(Window.unboundedPreceding, 0)`
- **B.** `w.rangeBetween(0, Window.unboundedFollowing)`
- **C.** `w.between("start", "current")`
- **D.** `w.frame(Window.unboundedPreceding, Window.currentRow)`

Réponse candidat : A  ✅

**57.** Répartir les lignes d'un groupe en quatre paquets d'effectif voisin.

- **A.** `F.quartile(4).over(w)`
- **B.** `F.bucket(4).over(w)`
- **C.** `F.ntile(4).over(w)`
- **D.** `F.percent_rank(4).over(w)`

Réponse candidat : C  ✅

**58.** Lire un CSV avec en-tête et inférence de type.

- **A.** `spark.read.csv(chemin, header=True, infer=True)`
- **B.** `spark.read.format("csv").option("firstRowIsHeader", "true").load(chemin)`
- **C.** `spark.read.csv(chemin, options={"header": True})`
- **D.** `spark.read.option("header", "true").option("inferSchema", "true").csv(chemin)`

Réponse candidat : D  ✅

**59.** Autoriser l'ajout de nouvelles colonnes lors d'une écriture Delta.

- **A.** `.option("overwriteSchema", "true")`
- **B.** `.option("mergeSchema", "true")`
- **C.** `.option("schemaEvolution", "add")`
- **D.** `.option("evolveSchema", "true")`

Réponse candidat : C  ❌ — juste : **B**

> `mergeSchema` ajoute les colonnes nouvelles à l'écriture. `schemaEvolution` n'est pas une option d'écriture Delta.

**60.** Remplacer entièrement le schéma d'une table lors d'un écrasement.

- **A.** `.mode("overwrite").option("mergeSchema", "true")`
- **B.** `.mode("replace").option("schema", "new")`
- **C.** `.mode("overwrite").option("forceSchema", "true")`
- **D.** `.mode("overwrite").option("overwriteSchema", "true")`

Réponse candidat : C  ❌ — juste : **D**

> `overwriteSchema` remplace le schéma **entier**. `forceSchema` n'existe pas. À ne pas confondre avec `mergeSchema`, qui ajoute.

**61.** Passer un paramètre nommé à une requête SQL depuis Python.

- **A.** `spark.sql("SELECT * FROM t WHERE j = :j", args={"j": jour})`
- **B.** `spark.sql(f"SELECT * FROM t WHERE j = '{jour}'")`
- **C.** `spark.sql("SELECT * FROM t WHERE j = ?", jour)`
- **D.** `spark.sql("SELECT * FROM t WHERE j = %s" % jour)`

Réponse candidat : A  ✅

**62.** Écrire une expression SQL à l'intérieur d'un `withColumn`.

- **A.** `F.sql("montant * 1.2")`
- **B.** `F.eval("montant * 1.2")`
- **C.** `F.raw("montant * 1.2")`
- **D.** `F.expr("montant * 1.2")`

Réponse candidat : D  ✅

**63.** Sélectionner plusieurs colonnes en écrivant du SQL.

- **A.** `df.sql("id, montant * 1.2 AS ttc")`
- **B.** `df.select("id", "montant * 1.2 AS ttc")`
- **C.** `df.selectExpr("id", "montant * 1.2 AS ttc")`
- **D.** `df.query("SELECT id, montant * 1.2 AS ttc")`

Réponse candidat : C  ✅

**64.** Créer une table à partir du résultat d'une requête, en SQL.

- **A.** `CREATE TABLE t FROM SELECT …`
- **B.** `CREATE TABLE t POPULATE SELECT …`
- **C.** `INSERT TABLE t SELECT …`
- **D.** `CREATE TABLE t AS SELECT …`

Réponse candidat : D  ✅

**65.** Créer une vue recalculée à chaque lecture.

- **A.** `CREATE MATERIALIZED VIEW v AS SELECT …`
- **B.** `CREATE STREAMING TABLE v AS SELECT …`
- **C.** `CREATE TEMP TABLE v AS SELECT …`
- **D.** `CREATE OR REPLACE VIEW v AS SELECT …`

Réponse candidat : D  ✅

**66.** Créer un objet dont le résultat est stocké et rafraîchi par la plateforme.

- **A.** `CREATE VIEW`
- **B.** `CREATE MATERIALIZED VIEW`
- **C.** `CREATE TABLE AS SELECT`
- **D.** `CREATE TEMPORARY VIEW`

Réponse candidat : B  ✅

**67.** Déclarer une table alimentée par incréments depuis une source de flux, en SQL.

- **A.** `CREATE STREAM TABLE t AS SELECT * FROM source`
- **B.** `CREATE TABLE t STREAMING AS SELECT * FROM source`
- **C.** `CREATE OR REFRESH STREAMING TABLE t AS SELECT * FROM STREAM(source)`
- **D.** `CREATE INCREMENTAL TABLE t FROM source`

Réponse candidat : C  ✅

**68.** Créer une vue de session qui disparaît avec elle.

- **A.** `df.createView("v", temp=True)`
- **B.** `df.createOrReplaceTempView("v")`
- **C.** `df.registerTable("v")`
- **D.** `spark.createTempTable("v", df)`

Réponse candidat : B  ✅

**69.** Purger les fichiers obsolètes d'une table en conservant sept jours.

- **A.** `VACUUM t KEEP 7 DAYS`
- **B.** `CLEAN TABLE t RETAIN 7 DAYS`
- **C.** `VACUUM t RETAIN 168 HOURS`
- **D.** `PURGE t OLDER THAN 168 HOURS`

Réponse candidat : C  ✅

**70.** Recalculer les statistiques d'une table après un chargement massif.

- **A.** `REFRESH TABLE t`
- **B.** `UPDATE STATISTICS t`
- **C.** `OPTIMIZE t COMPUTE STATS`
- **D.** `ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS`

Réponse candidat : C  ❌ — juste : **D**

> `ANALYZE TABLE … COMPUTE STATISTICS`. `OPTIMIZE` **compacte** les fichiers, il ne calcule aucune statistique.

**71.** Consulter le nombre de fichiers et la taille physique d'une table.

- **A.** `DESCRIBE HISTORY t`
- **B.** `SHOW TABLE STATS t`
- **C.** `DESCRIBE EXTENDED t`
- **D.** `DESCRIBE DETAIL t`

Réponse candidat : C  ❌ — juste : **D**

> `DESCRIBE DETAIL` donne l'état **physique** : `numFiles`, `sizeInBytes`, colonnes de rangement. `DESCRIBE EXTENDED` donne les métadonnées **logiques** : propriétaire, emplacement, `MANAGED` ou `EXTERNAL`.

**72.** Voir le plan qu'une requête va exécuter, avec le détail des opérateurs.

- **A.** `DESCRIBE PLAN SELECT …`
- **B.** `EXPLAIN FORMATTED SELECT …`
- **C.** `SHOW PLAN FOR SELECT …`
- **D.** `ANALYZE SELECT …`

Réponse candidat : B  ✅

**73.** Modifier un paramètre de session Spark depuis un carnet.

- **A.** `spark.set("spark.sql.shuffle.partitions", 400)`
- **B.** `spark.conf.set("spark.sql.shuffle.partitions", 400)`
- **C.** `spark.config("spark.sql.shuffle.partitions", 400)`
- **D.** `sc.setConf("spark.sql.shuffle.partitions", 400)`

Réponse candidat : B  ✅

**74.** Copier une table instantanément, sans recopier les fichiers.

- **A.** `CREATE TABLE c SHALLOW CLONE t`
- **B.** `CREATE TABLE c DEEP CLONE t`
- **C.** `CREATE TABLE c LIKE t`
- **D.** `CREATE TABLE c AS SELECT * FROM t`

Réponse candidat : A  ✅

**75.** Copier une table en la rendant réellement indépendante.

- **A.** `CREATE TABLE c SHALLOW CLONE t`
- **B.** `CREATE TABLE c COPY OF t`
- **C.** `CREATE TABLE c FULL CLONE t`
- **D.** `CREATE TABLE c DEEP CLONE t`

Réponse candidat : D  ✅

**76.** Poser un masque sur une colonne.

- **A.** `ALTER TABLE t MASK COLUMN email WITH f`
- **B.** `ALTER TABLE t ALTER COLUMN email SET MASK f`
- **C.** `ALTER COLUMN t.email APPLY MASK f`
- **D.** `GRANT MASK f ON t.email`

Réponse candidat : B  ✅

**77.** Poser un filtre de lignes sur une table.

- **A.** `ALTER TABLE t ADD FILTER f (region)`
- **B.** `CREATE ROW POLICY f ON t (region)`
- **C.** `ALTER TABLE t SET POLICY f FOR ROWS (region)`
- **D.** `ALTER TABLE t SET ROW FILTER f ON (region)`

Réponse candidat : D  ✅

**78.** Tester l'appartenance du lecteur à un groupe, dans une fonction de masquage.

- **A.** `is_member('rh')`
- **B.** `is_account_group_member('rh')`
- **C.** `current_group() = 'rh'`
- **D.** `has_group('rh')`

Réponse candidat : B  ✅

**79.** Obtenir l'identité de l'utilisateur qui exécute la requête.

- **A.** `session_id()`
- **B.** `whoami()`
- **C.** `current_user()`
- **D.** `user_identity()`

Réponse candidat : C  ✅

**80.** Étiqueter une table pour la gouvernance par attributs.

- **A.** `ALTER TABLE t SET TAGS ('pii' = 'true')`
- **B.** `ALTER TABLE t ADD LABEL 'pii'`
- **C.** `TAG TABLE t WITH 'pii'`
- **D.** `COMMENT ON TABLE t IS 'pii'`

Réponse candidat : A  ✅

**81.** Afficher la structure d'un DataFrame, types compris.

- **A.** `df.printSchema()`
- **B.** `df.schema()`
- **C.** `df.describe()`
- **D.** `df.columns()`

Réponse candidat : A  ✅

**82.** Obtenir count, moyenne, écart-type, min, max **et les quartiles**.

- **A.** `df.describe()`
- **B.** `df.stats()`
- **C.** `df.profile()`
- **D.** `df.summary()`

Réponse candidat : D  ✅

**83.** Augmenter le nombre de partitions d'un DataFrame, avec redistribution.

- **A.** `df.repartition(200)`
- **B.** `df.coalesce(200)`
- **C.** `df.partitionBy(200)`
- **D.** `df.rebalance(200)`

Réponse candidat : B  ❌ — juste : **A**

> `repartition(n)` : **brassage complet**, peut augmenter **ou** diminuer. Tu l'as inversé avec la Q84.

**84.** Réduire le nombre de partitions **sans** redistribution complète.

- **A.** `df.coalesce(10)`
- **B.** `df.repartition(10)`
- **C.** `df.shrink(10)`
- **D.** `df.reduce(10)`

Réponse candidat : B  ❌ — juste : **A**

> `coalesce(n)` : fusionne les partitions existantes **sans brassage complet**, et ne peut que **diminuer**. Tu l'as inversé avec la Q83.

**85.** Partitionner physiquement les fichiers à l'écriture.

- **A.** `df.write.option("partition", "jour").saveAsTable("t")`
- **B.** `df.write.partitionBy("jour").saveAsTable("t")`
- **C.** `df.partitionBy("jour").write.saveAsTable("t")`
- **D.** `df.write.clusterBy("jour").saveAsTable("t")`

Réponse candidat : A  ❌ — juste : **B**

> `partitionBy` est une **méthode du writer**, pas une option. `df.write.partitionBy(...).saveAsTable(...)`.

**86.** Récupérer le nom du fichier source de chaque ligne lue.

- **A.** `F.source_file()`
- **B.** `F.file_path()`
- **C.** `F.col("_file")`
- **D.** `F.input_file_name()`

Réponse candidat : C  ❌ — juste : **D**

> `F.input_file_name()`, ou la colonne de métadonnées `_metadata.file_path`. `F.col("_file")` n'existe pas.

**87.** Déclarer où Auto Loader mémorise le schéma inféré.

- **A.** `.option("cloudFiles.schemaPath", chemin)`
- **B.** `.option("cloudFiles.schemaLocation", chemin)`
- **C.** `.option("schemaRegistry", chemin)`
- **D.** `.option("cloudFiles.inferSchemaAt", chemin)`

Réponse candidat : B  ✅

**88.** Activer la colonne de sauvetage d'Auto Loader.

- **A.** `.option("cloudFiles.rescue", "true")`
- **B.** `.option("badRecordsPath", "_rescued_data")`
- **C.** `.option("mode", "RESCUE")`
- **D.** `.option("rescuedDataColumn", "_rescued_data")`

Réponse candidat : D  ✅

**89.** Choisir le mode d'évolution de schéma d'Auto Loader.

- **A.** `.option("cloudFiles.evolution", "add")`
- **B.** `.option("cloudFiles.schemaEvolutionMode", "addNewColumns")`
- **C.** `.option("schemaEvolution", "addNewColumns")`
- **D.** `.option("cloudFiles.schemaMode", "evolve")`

Réponse candidat : B  ✅

**90.** Forcer `COPY INTO` à recharger des fichiers déjà traités.

- **A.** `FORMAT_OPTIONS ('reload' = 'true')`
- **B.** `COPY_OPTIONS ('overwrite' = 'true')`
- **C.** `COPY_OPTIONS ('force' = 'true')`
- **D.** `WITH RELOAD ALL`

Réponse candidat : C  ✅

**91.** Écrire dans une table de flux depuis un DataFrame en streaming.

- **A.** `df.writeStream.toTable("cat.sch.t")`
- **B.** `df.writeStream.saveAsTable("cat.sch.t")`
- **C.** `df.write.stream("cat.sch.t")`
- **D.** `df.writeStream.into("cat.sch.t")`

Réponse candidat : A  ✅

**92.** Rassembler les valeurs distinctes d'un groupe dans un tableau.

- **A.** `F.collect_set("canal")`
- **B.** `F.collect_list("canal")`
- **C.** `F.array_distinct("canal")`
- **D.** `F.group_array("canal")`

Réponse candidat : A  ✅

**93.** Calculer une médiane approchée.

- **A.** `F.median("m", approx=True)`
- **B.** `F.approx_percentile("m", 50)`
- **C.** `F.percentile_approx("m", 0.5, 10000)`
- **D.** `F.quantile("m", 0.5)`

Réponse candidat : C  ✅

**94.** Filtrer des groupes après agrégation, en PySpark.

- **A.** `.groupBy("r").agg(F.sum("m").alias("ca")).filter(F.col("ca") > 1000)`
- **B.** `.groupBy("r").agg(F.sum("m").alias("ca")).having("ca > 1000")`
- **C.** `.groupBy("r").having(F.sum("m") > 1000)`
- **D.** `.filter(F.sum("m") > 1000).groupBy("r")`

Réponse candidat : A  ✅

**95.** Transposer des valeurs en colonnes, en fournissant la liste.

- **A.** `df.pivot("mois", MOIS).groupBy("region").sum("montant")`
- **B.** `df.groupBy("region").transpose("mois", MOIS)`
- **C.** `df.groupBy("region").pivot("mois", MOIS).sum("montant")`
- **D.** `df.crosstab("region", "mois")`

Réponse candidat : C  ✅

**96.** Créer une structure imbriquée à partir de plusieurs colonnes.

- **A.** `F.nest("a", "b")`
- **B.** `F.map("a", "b")`
- **C.** `F.struct("a", "b")`
- **D.** `F.record("a", "b")`

Réponse candidat : B  ❌ — juste : **C**

> `F.struct` construit une **structure**. `F.create_map` construit une carte clé-valeur — et `F.map` tout court n'existe pas.

**97.** Déplier un tableau en conservant la position de chaque élément.

- **A.** `F.explode("arr")`
- **B.** `F.explode_outer("arr")`
- **C.** `F.enumerate("arr")`
- **D.** `F.posexplode("arr")`

Réponse candidat : B  ❌ — juste : **D**

> `posexplode` ajoute la **position** de chaque élément. `explode_outer` fait autre chose : il conserve les tableaux **vides**.

**98.** Rendre un rang **sans trou** en présence d'ex æquo.

- **A.** `F.dense_rank()`
- **B.** `F.rank()`
- **C.** `F.row_number()`
- **D.** `F.percent_rank()`

Réponse candidat : A  ✅

**99.** Créer un catalogue s'il n'existe pas déjà.

- **A.** `CREATE DATABASE IF NOT EXISTS ventes`
- **B.** `CREATE CATALOG IF NOT EXISTS ventes`
- **C.** `CREATE SCHEMA IF NOT EXISTS ventes CATALOG`
- **D.** `NEW CATALOG ventes`

Réponse candidat : B  ✅

**100.** Désactiver totalement la diffusion automatique dans les jointures.

- **A.** `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 0)`
- **B.** `spark.conf.set("spark.sql.broadcast.enabled", "false")`
- **C.** `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)`
- **D.** `spark.conf.set("spark.sql.join.broadcast", "off")`

Réponse candidat : C  ✅

**101.** Trier un DataFrame par montant décroissant.

- **A.** `df.orderBy(F.col("montant").desc())`
- **B.** `df.sort("montant", ascending=False, reverse=True)`
- **C.** `df.orderBy("montant DESC")`
- **D.** `df.sortBy(F.desc("montant"))`

Réponse candidat : A  ✅

---

<details>
<summary><b>Corrigé — ne déplier qu'après avoir répondu à tout</b></summary>

| # | Rép. | Thème | Note |
|---|---|---|---|
| 1 | C | DataFrame | `withColumnRenamed(ancien, nouveau)` |
| 2 | D | DataFrame | Une constante doit être enveloppée dans `F.lit` |
| 3 | B | Auto Loader | Le format est `cloudFiles` ; le format réel va dans `cloudFiles.format` |
| 4 | A | DataFrame | — |
| 5 | C | Écriture | `mode("append")` puis `saveAsTable` |
| 6 | D | Types | `try_cast` est une méthode de Column |
| 7 | D | Imbriqué | `explode_outer` conserve les tableaux vides ou nuls |
| 8 | A | `COPY INTO` | `FILEFORMAT` puis `FORMAT_OPTIONS` |
| 9 | C | Jointure | `F.broadcast` s'applique au DataFrame à diffuser |
| 10 | B | Agrégation | `.agg(...)` avec `.alias(...)` : toujours nommer |
| 11 | A | Fenêtre | `Window` vient de `pyspark.sql.window` |
| 12 | D | Fenêtre | `F.row_number().over(w)` |
| 13 | A | Chaînes | `F.regexp_replace(colonne, motif, remplacement)` |
| 14 | C | Nulls | `F.coalesce` rend la première valeur non nulle |
| 15 | C | Conditions | `when(...).otherwise(...)` |
| 16 | D | `MERGE` | La forme complète, avec `UPDATE SET *` et `INSERT *` |
| 17 | D | Lecture | `spark.table` |
| 18 | C | Streaming | `checkpointLocation` |
| 19 | D | Streaming | `availableNow` : traite le retard puis s'arrête |
| 20 | C | Agrégation | Second argument : erreur relative admise |
| 21 | B | Union | `unionByName` aligne par nom ; `union` par position |
| 22 | B | Rangement | `OPTIMIZE … ZORDER BY` |
| 23 | D | Rangement | `CLUSTER BY` à la création : le liquid clustering |
| 24 | C | Delta | `DESCRIBE HISTORY` |
| 25 | A | Delta | La seule commande de retour arrière qui existe |
| 26 | A | Delta | Aussi `TIMESTAMP AS OF` |
| 27 | B | Gouvernance | `GRANT SELECT ON TABLE … TO` |
| 28 | A | Unity Catalog | — |
| 29 | B | Déclaratif | `@dlt.table` |
| 30 | C | Déclaratif | `expect` compte · `expect_or_drop` écarte · `expect_or_fail` arrête |
| 31 | A | Déclaratif | `ON VIOLATION FAIL UPDATE` |
| 32 | C | Déclaratif | C'est cette lecture qui construit le graphe |
| 33 | B | Jobs | Aussi `dropdown`, `combobox`, `multiselect` |
| 34 | D | Jobs | Rend **toujours** une chaîne |
| 35 | A | Jobs | La lecture accepte `debugValue` |
| 36 | B | Bundles | Résout, vérifie, affiche — ne modifie rien |
| 37 | B | Bundles | `clean` et `remove` n'existent pas |
| 38 | C | DataFrame | `distinct()` porte sur toutes les colonnes |
| 39 | D | Nulls | Aussi `df.na.fill(0, ["montant"])` |
| 40 | C | Nulls | Aussi `df.na.drop()` |
| 41 | C | Chaînes | Rend un tableau |
| 42 | A | Chaînes | `concat_ws` : le séparateur en premier |
| 43 | B | Types | Le motif s'enveloppe dans `F.lit` |
| 44 | C | Dates | Motif Java |
| 45 | B | Dates | Fin d'abord, début ensuite |
| 46 | D | Imbriqué | Chemin JSON, sans schéma |
| 47 | D | Imbriqué | `from_json` avec un schéma |
| 48 | C | Imbriqué | `F.size` sur un tableau |
| 49 | A | Imbriqué | — |
| 50 | B | Imbriqué | `array_compact` retire les nulls |
| 51 | B | Jointure | `join(autre, cle, type)` |
| 52 | A | Jointure | Une liste de colonnes |
| 53 | A | Jointure | `left_anti` : ce qui n'a pas de correspondance |
| 54 | B | Agrégation | Plus lisible qu'un `when` imbriqué |
| 55 | A | Fenêtre | `lag` regarde en arrière, `lead` en avant |
| 56 | A | Fenêtre | Le cadre du cumul |
| 57 | C | Fenêtre | `ntile(n)` |
| 58 | D | Lecture | Deux options distinctes |
| 59 | B | Écriture | `mergeSchema` ajoute des colonnes |
| 60 | D | Écriture | `overwriteSchema` remplace le schéma entier |
| 61 | A | SQL | Paramètre nommé : jamais de concaténation |
| 62 | D | SQL | `F.expr` évalue une expression SQL |
| 63 | C | SQL | `selectExpr` accepte du SQL |
| 64 | D | SQL | `CREATE TABLE … AS SELECT` |
| 65 | D | Publication | Recalculée à chaque lecture |
| 66 | B | Publication | Stockée et rafraîchie par la plateforme |
| 67 | C | Publication | `STREAM(...)` marque la source comme incrémentale |
| 68 | B | Publication | Disparaît avec la session |
| 69 | C | Delta | La rétention s'exprime en heures |
| 70 | D | Optimisation | Après tout chargement massif |
| 71 | D | Optimisation | `numFiles`, `sizeInBytes`, colonnes de rangement |
| 72 | B | Optimisation | Y chercher `number of files read` |
| 73 | B | Optimisation | `spark.conf.set` |
| 74 | A | Delta | Trois secondes quelle que soit la taille |
| 75 | D | Delta | La seule qui recopie les fichiers |
| 76 | B | Gouvernance | `ALTER COLUMN … SET MASK` |
| 77 | D | Gouvernance | `SET ROW FILTER f ON (colonne)` |
| 78 | B | Gouvernance | La brique de toutes les règles |
| 79 | C | Gouvernance | Aussi `session_user()` |
| 80 | A | Gouvernance | Étiquette exploitable par les politiques ABAC |
| 81 | A | DataFrame | `printSchema` affiche, `schema` est un attribut |
| 82 | D | Agrégation | `summary()` ajoute les quartiles |
| 83 | A | Optimisation | `repartition` redistribue |
| 84 | A | Optimisation | `coalesce` fusionne sans brassage complet |
| 85 | B | Écriture | `partitionBy` s'écrit sur le writer |
| 86 | D | Lecture | Aussi la colonne `_metadata` |
| 87 | B | Auto Loader | `cloudFiles.schemaLocation` |
| 88 | D | Auto Loader | — |
| 89 | B | Auto Loader | Valeurs : `addNewColumns`, `rescue`, `failOnNewColumns`, `none` |
| 90 | C | `COPY INTO` | Ignore l'historique des fichiers chargés |
| 91 | A | Streaming | `toTable` en streaming, `saveAsTable` en batch |
| 92 | A | Agrégation | `collect_set` déduplique, `collect_list` non |
| 93 | C | Agrégation | Troisième argument : la précision |
| 94 | A | Agrégation | Un agrégat nommé se filtre comme une colonne — remplace `HAVING` |
| 95 | C | Agrégation | La liste est le contrat de la table produite |
| 96 | C | Imbriqué | — |
| 97 | D | Imbriqué | `posexplode` ajoute la position |
| 98 | A | Fenêtre | `rank` 1,1,3 · `dense_rank` 1,1,2 · `row_number` 1,2,3 |
| 99 | B | Unity Catalog | `CATALOG`, pas `DATABASE` |
| 100 | C | Optimisation | `-1` désactive ; `0` n'est pas la valeur prévue |
| 101 | A | DataFrame | Aussi `df.orderBy(F.desc("montant"))` |

### Comment lire ton résultat

Compte tes ratés **par thème**, pas globalement. Trois erreurs dans la même famille
valent un coup d'œil ; trois erreurs éparpillées sur cent questions ne veulent rien dire.

</details>
