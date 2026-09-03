# 1 · Exploration — avant de coder

**La règle** : on ne déclare jamais un schéma avant d'avoir regardé le fichier brut. Cinq
minutes ici évitent une journée de reprise plus tard.

---

## Regarder les octets avant de parler de colonnes

Un fichier « CSV » n'est qu'un fichier texte. Avant de demander à Spark de l'interpréter,
lis-le **sans interprétation** :

```python
brut = spark.read.text("/Volumes/cat/sch/vol/arrivees/ventes.csv")
brut.show(5, truncate=False)      # les 5 premières lignes, telles quelles
brut.count()                       # nombre de lignes physiques, en-tête comprise
```

C'est ce qui révèle, en une seconde : le vrai séparateur, la présence d'un en-tête, des
guillemets, un pied de page, une ligne vide, un `BOM` en tête.

Pour les cas tordus — accents cassés, espaces invisibles :

```python
from pyspark.sql import functions as F
brut.select(F.hex(F.encode("value", "UTF-8"))).show(3, truncate=False)
```

---

## CSV

### Lecture exploratoire

```python
df = (spark.read
        .option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "true")     # exploration seulement
        .option("encoding", "UTF-8")
        .csv("/Volumes/cat/sch/vol/arrivees/"))

df.printSchema()
df.show(10, truncate=False)
print(df.count(), len(df.columns))
```

> **`inferSchema` est un outil d'exploration, pas de production.** Il coûte une passe
> complète sur les données et le schéma qu'il déduit change avec le contenu du jour. En
> production, on déclare.

### Les options qui règlent 90 % des cas

| Option | À quoi elle sert |
|---|---|
| `header` | la première ligne est un en-tête |
| `sep` | le séparateur — `;` en Europe |
| `encoding` | `UTF-8`, `windows-1252`, `ISO-8859-1` |
| `quote` · `escape` | quand un champ contient le séparateur |
| `multiLine` | un enregistrement s'étale sur plusieurs lignes |
| `nullValue` | la chaîne qui signifie « absent » — `""`, `NULL`, `\N` |
| `dateFormat` · `timestampFormat` | motif Java |
| `mode` | `PERMISSIVE` (défaut) · `DROPMALFORMED` · `FAILFAST` |

### Diagnostiquer la qualité en trois requêtes

```python
n = df.count()

# complétude : l'écart entre les deux comptages EST le nombre d'absences
df.select([
    (F.count(c) / F.lit(n)).alias(c) for c in df.columns
]).show(truncate=False)

# unicité de la clé
df.groupBy("order_id").count().filter("count > 1").count()

# domaine d'une colonne : que contient-elle réellement ?
df.groupBy("status").count().orderBy(F.desc("count")).show(50, truncate=False)
```

### Le piège du CSV, mesuré

Un champ contenant le séparateur non échappé produit **plus de jetons que de colonnes**.
Spark **tronque** les jetons en trop : le compte de lignes reste juste, le contenu est
mutilé, et **rien ne le signale**. Ni la colonne de sauvetage ni celle d'enregistrement
corrompu ne les récupèrent.

Pour le détecter :

```python
# compter les champs réellement présents par ligne, en lisant en texte brut
(brut.withColumn("champs", F.size(F.split("value", ";")))
     .groupBy("champs").count().show())
```

Si tu vois autre chose que le nombre attendu, tu tiens ton problème.

---

## JSON

### Lecture exploratoire

```python
df = (spark.read
        .option("multiLine", "false")      # true si un objet s'étale sur plusieurs lignes
        .json("/Volumes/cat/sch/vol/arrivees/events/"))

df.printSchema()          # LA commande à passer en premier sur du JSON
df.show(3, truncate=False)
```

`printSchema()` te donne la structure imbriquée complète : `struct`, `array`, types
déduits. C'est ce qui décide de tout ce qui suit.

### Deviner un schéma, puis le figer

```python
# extraire le schéma d'un échantillon, pour le recopier dans le code
echantillon = spark.read.json(chemin).schema.json()
print(echantillon)

# en SQL, sur une colonne texte contenant du JSON
spark.sql("SELECT schema_of_json('{\"a\":1,\"b\":[\"x\"]}')").show(truncate=False)
```

Ensuite on **déclare** ce schéma en dur, et l'on cesse de le deviner à chaque exécution.

### Explorer l'imbriqué

```python
# aplatir un niveau
df.select("id", "client.*").printSchema()

# un tableau : combien d'éléments, et des vides ?
df.select(F.size("lignes").alias("n")).groupBy("n").count().show()

# déplier — attention, ça change le grain
df.select("id", F.explode("lignes").alias("ligne")).select("id", "ligne.*")
```

> `explode` **supprime** les lignes dont le tableau est vide ou nul.
> `explode_outer` les conserve avec `NULL`. `posexplode` ajoute la position.

### Les deux colonnes de récupération

```python
df = (spark.read
        .schema(schema_declare)
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(chemin))

df.filter(F.col("_corrupt_record").isNotNull()).show(5, truncate=False)
```

| Colonne | Ce qu'elle attrape |
|---|---|
| `_corrupt_record` | **Échec d'analyse** : la ligne JSON est syntaxiquement invalide |
| `_rescued_data` | **Écart au schéma** : type ou colonne inattendus *(Auto Loader)* |

Deux mécanismes distincts. Une ligne JSON cassée ne va **jamais** dans `_rescued_data`.

---

## Le réflexe, en quatre gestes

1. **Lire en texte brut** et regarder cinq lignes.
2. **`printSchema()`** — sur JSON, c'est la première commande.
3. **Compter** : lignes, valeurs renseignées par colonne, clés en double.
4. **Regarder les domaines** : `groupBy` sur les colonnes catégorielles.

Ensuite seulement, écrire le schéma en dur et passer à l'ingestion.
