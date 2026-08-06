# 🧰 Outillage — M1

*À lire avant d'ouvrir les notebooks. Cette fiche dit **avec quoi** tu vas travailler, pas
**comment** — les choix restent l'exercice.*

Prérequis transversal : `docs/07-python-pour-le-parcours.md`.

---

## Ce que tu vas faire

Amener trois sources de fichiers dans des tables Delta, sans rien perdre en route : un CSV
mal formé, du JSON imbriqué et compressé, et trois référentiels livrés en instantané
complet. Puis prouver que rejouer l'ingestion n'ajoute rien, constater ce que le lecteur a
quand même abîmé, et le réparer.

Trois notebooks, dans cet ordre : `M1_bronze_orders`, `M1_bronze_events`, `M1_bronze_ref`.

---

## 1. Explorer les fichiers (Python ordinaire)

Avant de coder quoi que ce soit. Tu cherches : le séparateur, l'encodage, le format des
nombres, et ce qui casse.

| Outil | Ce qu'il fait |
|---|---|
| `open(chemin, "r", encoding=...)` | ouvrir un texte — l'encodage n'est pas devinable |
| `gzip.open(chemin, "rt", encoding=...)` | idem pour un `.gz`, sans décompresser sur disque |
| `os.listdir(chemin)` + `sorted()` | lister les fichiers du volume |
| `json.loads(ligne)` | JSON → dictionnaire Python |
| `json.dumps(obj, indent=2, ensure_ascii=False)` | l'afficher lisiblement |
| `try` / `except json.JSONDecodeError` | compter les lignes illisibles sans planter |
| `collections.Counter` | « combien de fois chaque… » |
| `type(v).__name__` | savoir si un champ change de type d'une ligne à l'autre |
| `repr(texte)` | **voir les caractères invisibles** |
| `ligne.count(";")` | mesurer combien de champs porte chaque ligne |

> Le CSV des commandes contient au moins quatre défauts distincts. Trois se voient au
> `repr`, le quatrième se voit en comptant les séparateurs.

## 2. Le flux d'ingestion (PySpark)

| Outil | Ce qu'il fait |
|---|---|
| `spark.readStream.format("cloudFiles")` | Auto Loader — le format se déclare **dedans**, pas ici |
| `.option("cloudFiles.format", ...)` | `csv`, `json`… |
| `.option("cloudFiles.schemaLocation", ...)` | où Auto Loader **range le schéma qu'il infère** — tu n'y écris rien |
| `.option("cloudFiles.inferColumnTypes", ...)` | inférer les **types**, ou tout garder en `STRING` |
| `.option("cloudFiles.schemaEvolutionMode", ...)` | ce qui arrive quand une colonne apparaît — décisif en M8 |
| `.option("rescuedDataColumn", ...)` | la colonne de sauvetage — **sans préfixe**, c'est une option du lecteur |
| `sep` · `header` · `encoding` · `multiLine` · `quote` | options du lecteur CSV |
| `.load(chemin)` | déclenche la lecture |

**Écriture** : `.writeStream`, `.outputMode("append")`, `.option("checkpointLocation", ...)`,
`.trigger(availableNow=True)`, `.toTable(nom)`, puis `.awaitTermination()`.

Pour les référentiels, en instantané complet, tu n'as **pas** besoin de streaming :
`spark.read` et `.write.mode("overwrite")` suffisent. Comprendre pourquoi fait partie de
l'exercice.

### Les cinq pièges d'API de cette famille

1. `.format()` appelé deux fois : **le dernier gagne**, en silence.
2. Une option `cloudFiles.*` inconnue ou mal préfixée est **ignorée sans erreur**.
3. `.schema(...)` explicite désactive l'inférence *et* l'évolution de schéma.
4. Sans `.trigger(availableNow=True)`, le flux ne rend jamais la main. Sur serverless,
   `processingTime` n'existe pas.
5. Sans `.awaitTermination()`, la cellule suivante compte l'état d'avant.

## 3. Les colonnes techniques

| Outil | Ce qu'il fait |
|---|---|
| `F.col("_metadata.file_name")` | colonne **cachée** de toute source fichier |
| `F.col("_metadata.file_modification_time")` | idem, en `timestamp` |
| `F.current_timestamp()` · `F.lit(valeur)` | horodater, poser une constante |
| `.withColumn(nom, expr)` | ajouter une colonne |

`_metadata` n'apparaît pas dans `printSchema()`. Elle est pourtant sélectionnable.

## 4. La passe de réparation (étape 6)

| Outil | Ce qu'il fait |
|---|---|
| `F.split(colonne, ";")` | découpe une chaîne en tableau |
| `F.size(tableau)` | combien d'éléments |
| `tableau[1]` | un élément — **base 0** |
| `F.slice(tableau, debut, longueur)` | une portion — **base 1** |
| `F.array_join(tableau, ";")` | recolle en chaîne |
| `.filter(...)` · `.select(...)` · `.alias(...)` | garder, projeter, nommer |

Deux conventions d'indice opposées dans la même expression : c'est là que ça se joue.

Ce second flux a **ses propres** `CHECKPOINT_REPAIR` et `SCHEMA_LOCATION_REPAIR`, déjà
définis en tête de notebook. Les partager avec le flux principal casserait les deux.

## 5. Contrôler et journaliser

| Outil | Ce qu'il fait |
|---|---|
| `spark.table(nom)` · `.count()` · `.dtypes` · `.printSchema()` | vérifier |
| `.select(col).distinct().count()` | compter les valeurs distinctes |
| `.groupBy(...).agg(F.count("*"))` | ventiler par fichier |
| `spark.createDataFrame(lignes, schema)` | fabriquer la ligne de journal |
| `.write.mode("append").saveAsTable(...)` | l'ajouter à `ops.pipeline_runs` |
| `dbutils.fs.ls(chemin)` · `dbutils.fs.rm(chemin, True)` | inspecter, réinitialiser |

---

## Le vocabulaire à retenir de ce module

**Checkpoint** (où j'en suis) · **schemaLocation** (quelle forme ont les données) ·
**colonne de sauvetage** · **`_metadata`** · **`availableNow`** · **idempotence**.

Ces six mots sont des objectifs d'examen, section 2 — 21 % de l'épreuve.
