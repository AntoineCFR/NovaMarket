# 🧰 Outillage — M2

*À lire avant d'ouvrir le notebook. Cette fiche dit **avec quoi**, pas **comment**.*

---

## Ce que tu vas faire

Ingérer une base applicative (clients, vendeurs) qui, contrairement aux fichiers, **ne
livre pas de nouveautés dans un répertoire** : il faut aller lui demander ce qui a changé
depuis la dernière fois. Tu vas donc gérer toi-même ce qu'Auto Loader faisait pour toi :
un curseur de progression.

Trois extractions successives, dont deux incrémentales.

---

## 1. Paramétrer le notebook

| Outil | Ce qu'il fait |
|---|---|
| `dbutils.widgets.dropdown(nom, defaut, [choix], libelle)` | crée un sélecteur en tête de notebook |
| `dbutils.widgets.text(...)` · `.get(nom)` | idem en saisie libre · relire la valeur |

Le module offre deux voies d'accès à la source (Lakebase ou fichiers d'extraction). Le
widget choisit laquelle. Les deux sont notées de la même façon.

## 2. Lire la source

| Outil | Ce qu'il fait |
|---|---|
| `spark.read.format("csv").option(...).load(...)` | lecture **batch**, pas de streaming ici |
| `spark.read.format("postgresql")` ou JDBC | l'autre voie, si Lakebase est disponible |
| `.createOrReplaceTempView(nom)` | exposer un DataFrame au SQL du notebook |
| `spark.sql("...")` | écrire la logique en SQL quand c'est plus lisible |

Pourquoi pas Auto Loader ? Parce qu'il n'y a **pas de fichiers qui arrivent**. C'est la
première question du module.

## 3. Le curseur (*watermark*)

C'est le cœur du module.

| Outil | Ce qu'il fait |
|---|---|
| `.agg(F.max("colonne"))` | trouver la valeur la plus haute observée |
| `.first()` · `[0]` | extraire un scalaire d'un DataFrame à une ligne |
| `.filter(F.col("updated_at") >= curseur)` | ne prendre que le nouveau |
| `F.lit(valeur)` | injecter le curseur dans une expression |
| ~~`.cache()` / `.unpersist()`~~ | **indisponible sur serverless** (`NOT_SUPPORTED_WITH_SERVERLESS`). Concept a connaitre pour l'examen, impraticable ici — voir `docs/01` |

> **Le piège, et c'est un objectif d'examen** : `>` ou `>=` ? Tu as raté cette question au
> diagnostic (section 2, question 9). Réponds-y ici en connaissance de cause, et vérifie
> ce que chacun coûte.

Le curseur se **range quelque part** entre deux exécutions : une table, un fichier, une
propriété de table. Ce choix t'appartient.

## 4. Typer à l'arrivée

| Outil | Ce qu'il fait |
|---|---|
| `.cast("int" / "timestamp" / "decimal(10,2)" / "boolean")` | convertir |
| `.withColumn(nom, expr)` · `.select(...)` · `.alias(...)` | projeter la forme voulue |
| `F.current_timestamp()` | horodater l'extraction |

`.cast` échoue **en silence** : une valeur non convertible devient `NULL`. Compte tes
`NULL` après chaque cast, systématiquement.

## 5. Écrire et journaliser

| Outil | Ce qu'il fait |
|---|---|
| `.write.mode("append" \| "overwrite")` | ajouter ou remplacer |
| `.option("overwriteSchema", "true")` | quand la forme de la table change |
| `.saveAsTable(nom)` | enregistrer dans Unity Catalog |
| `spark.createDataFrame(...)` + append | la ligne dans `ops.pipeline_runs` |

---

## Les questions auxquelles l'outillage ne répond pas

Ce sont elles, le module :

- Que devient une ligne **supprimée** à la source ? Quel outil de cette fiche la détecte ?
  (Réponse : aucun.)
- Si l'extraction plante à mi-chemin, où en est ton curseur ?
- Une ligne modifiée deux fois entre deux extractions : combien de versions vois-tu ?

## Le vocabulaire à retenir

**Extraction incrémentale** · **watermark** · **borne stricte ou inclusive** ·
**suppression physique contre suppression douce** · **idempotence sans checkpoint**.

Section 2 de l'examen — 21 %.
