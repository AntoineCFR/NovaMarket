# Python pour le parcours — l'inventaire

Ce document ne t'apprend pas Python. Il liste **les bibliothèques et les fonctions que ce
parcours utilise réellement**, et rien d'autre. Tu connais les concepts généraux : boucles,
conditions, fonctions, dictionnaires. Ce qui suit est le vocabulaire de bibliothèque.

Il y a **deux Python** dans ce dépôt, et les confondre est la première source de blocage.

| | Où | Ce que c'est |
|---|---|---|
| **Python ordinaire** | Cellules d'**exploration** uniquement | Tu lis un fichier ligne par ligne, tu comptes, tu regardes |
| **API DataFrame PySpark** | Tout le reste | Tu **décris** une transformation, Spark l'exécute |

Le second n'est pas de la programmation : c'est un vocabulaire déclaratif d'une trentaine
de mots, plus proche du SQL que du Python. Aucune boucle, aucun `if`, aucune variable
intermédiaire. Le premier n'apparaît que pour regarder un fichier **avant** de le confier
à Spark — et aucun grader ne le vérifie.

---

## Partie 1 — Python ordinaire, pour explorer

### Lire un fichier texte

```python
with open(chemin, "r", encoding="cp1252") as fh:
    for ligne in fh:
        ...
```

`with` ferme le fichier tout seul à la sortie du bloc. Itérer sur `fh` donne les lignes
une à une, sans tout charger en mémoire — indispensable sur un fichier de 60 000 lignes.

`encoding` n'est **pas** optionnel ici : nos CSV sont en `cp1252`, pas en UTF-8. Sans lui,
Python devine mal et tes accents deviennent illisibles.

### Lire un fichier compressé

```python
import gzip
with gzip.open(chemin, "rt", encoding="utf-8") as fh:
```

Même usage, `"rt"` au lieu de `"r"` (*read text* : décompresse et décode). Rien à
décompresser à la main.

### Lire du JSON

```python
import json
obj = json.loads(ligne)        # texte  -> objet Python
texte = json.dumps(obj, indent=2, ensure_ascii=False)   # objet -> texte lisible
```

**Un JSON désérialisé est un dictionnaire.** Tu y accèdes comme à un dictionnaire, et
l'imbrication s'enchaîne :

```python
obj["event_type"]              # une valeur
obj["user"]["country"]         # objet dans objet
obj["items"][0]["qty"]         # tableau dans objet -> une liste Python
```

`json.dumps(..., indent=2)` sert uniquement à **afficher** lisiblement. `ensure_ascii=False`
garde les accents au lieu de les échapper en `é`.

### Ne pas s'arrêter à la première erreur

```python
try:
    obj = json.loads(ligne)
except json.JSONDecodeError:
    ko += 1
```

Sans `try`, une seule ligne malformée fait planter la boucle et tu ne sais rien des
autres. Le type d'exception attrapé importe : attraper `Exception` masquerait aussi tes
propres fautes de frappe.

### Compter des occurrences

```python
from collections import Counter
c = Counter()
c[quelque_chose] += 1
print(c)                       # Counter({'str': 4941, 'int': 59})
```

Un dictionnaire qui vaut 0 par défaut. Sert partout où la question est « combien de fois
chaque… ». `Counter(liste)` compte directement le contenu d'une liste.

### Connaître le type d'une valeur

```python
type(valeur).__name__          # 'str', 'int', 'float', 'list', 'dict', 'NoneType'
```

Indispensable quand un champ JSON n'a pas toujours le même type — ce qui arrive dans ce
dépôt, et pas par accident.

### Lister un répertoire

```python
import os
fichiers = sorted(os.listdir(chemin))
```

`sorted()` pour un ordre reproductible. Sans lui, l'ordre dépend du système de fichiers.

### Regarder les caractères invisibles

```python
print(repr(ligne[:200]))
```

`repr()` montre `\t`, `\r\n`, `\xa0` (espace insécable) là où `print` les rend invisibles.
**Le réflexe à prendre** : quand une valeur « propre » ne se convertit pas, regarde son
`repr`.

---

## Partie 2 — L'API DataFrame PySpark

### Le principe

```python
from pyspark.sql import functions as F
```

Tout part de là. `F` contient les fonctions qui s'appliquent **aux colonnes**. Tu écris
une expression, Spark la traduit en plan d'exécution.

La règle qui évite 90 % des erreurs de débutant : **on ne manipule jamais les valeurs, on
décrit des colonnes.** Il n'y a pas de boucle sur les lignes.

```python
# NON — pensée ligne à ligne
for ligne in df: ...

# OUI — pensée colonne
df.withColumn("total", F.col("prix") * F.col("quantite"))
```

### Les six verbes qui font tout

| Verbe | Ce qu'il fait | Équivalent SQL |
|---|---|---|
| `.select(...)` | choisit et calcule des colonnes | `SELECT` |
| `.filter(...)` | garde des lignes | `WHERE` |
| `.withColumn(nom, expr)` | ajoute ou remplace **une** colonne | — |
| `.join(autre, on=, how=)` | rapproche deux tables | `JOIN` |
| `.groupBy(...).agg(...)` | agrège | `GROUP BY` |
| `.orderBy(...)` | trie | `ORDER BY` |

`.withColumn` en chaîne, ou un seul `.select` ? Les deux marchent. Le `select` final est
plus lisible quand tu déclares la forme exacte de ta table.

### Désigner une colonne

```python
F.col("nom")          # toujours valide
"nom"                 # accepté par beaucoup de fonctions
df["nom"], df.nom     # lié à CE DataFrame — source d'erreurs en jointure
```

**Préfère `F.col`.** `df.nom` attache l'expression à un DataFrame précis ; utilisée sur un
autre, elle produit une erreur d'analyse difficile à lire.

### Renommer et typer

```python
.alias("nouveau_nom")             # renomme une expression
.cast("int")                      # convertit — LEVE si impossible (mode ANSI actif ici)
```

⚠️ **Attention à ce que dit la documentation générique de Spark.** Elle décrit un `cast`
silencieux, qui rendrait `NULL` sur `"abc".cast("int")`. **Ce n'est pas le comportement de
ton environnement** : le mode ANSI est actif sur le serverless Databricks, et un `cast`
impossible lève `CAST_INVALID_INPUT`. C'est aussi le défaut de Databricks SQL, donc celui
que tu rencontreras en poste.

Pour convertir de la donnée sale — c'est-à-dire toute donnée venant de bronze — utilise
les variantes `try_*`, qui rendent `NULL` au lieu de lever :

```python
c.try_cast("int")                                    # au lieu de c.cast("int")
F.try_to_timestamp(c, F.lit("yyyy-MM-dd HH:mm:ss"))  # au lieu de F.to_timestamp(...)
```

Ça ne change rien à la raison pour laquelle bronze garde tout en `string` : convertir,
c'est interpréter, et bronze n'interprète pas. Voir `docs/01-contraintes-free-edition.md`.

### Conditions

```python
F.when(condition, valeur).when(autre, valeur2).otherwise(defaut)
F.coalesce(a, b)                  # la première valeur non nulle
```

`when` sans `otherwise` renvoie `NULL` dans les cas non couverts. C'est rarement ce qu'on
veut, et ça ne prévient pas — **sauf quand c'est exactement ce qu'on veut**, voir juste
en dessous.

### Construire large, puis retirer

Le réflexe impératif, celui que tu as en Python ordinaire :

```python
motifs = []
if ts_invalide:  motifs.append("INVALID_TIMESTAMP")
if qty_invalide: motifs.append("INVALID_QUANTITY")
```

**Il n'a pas d'équivalent dans l'API DataFrame.** Une expression décrit un calcul appliqué
à *toutes* les lignes à la fois : il n'existe pas de « pour cette ligne-ci, ajouter un
élément ». Le nombre d'éléments devrait être connu à l'écriture, or il dépend de la donnée.

Le motif déclaratif renverse la construction :

```python
F.array_compact(F.array(
    F.when(cond_a, F.lit("MOTIF_A")),     # -> NULL si cond_a est fausse
    F.when(cond_b, F.lit("MOTIF_B")),
    F.when(cond_c, F.lit("MOTIF_C")),
))
```

Quatre positions pour toutes les lignes, dont certaines valent `NULL`, puis on compacte.
**Forme fixe à l'écriture, longueur variable à l'exécution.** C'est là que le `when` sans
`otherwise` devient un outil et non un oubli : le `NULL` est le trou qu'on retirera.

`array_compact` est un raccourci ; la forme générale dit mieux ce qui se passe :

```python
F.filter(F.array(...), lambda x: x.isNotNull())
```

Tu retrouveras ce renversement partout dès qu'une sortie a une taille variable. Si tu te
surprends à chercher comment « ajouter conditionnellement », c'est le signal.

### Tester une valeur

```python
F.col("x").isNull()  /  .isNotNull()
F.col("x").isin("A", "B")
F.col("x").rlike("^[0-9]+$")      # expression régulière
F.col("x").contains(",")
F.col("x").startswith("prefixe")
```

### Chaînes de caractères

```python
F.trim(c) · F.upper(c) · F.lower(c) · F.length(c)
F.regexp_replace(c, motif, remplacement)
F.split(c, ";")                   # -> tableau
F.split(c, ";", 14)               # au plus 14 morceaux, le dernier absorbe le reste
```

### Tableaux

```python
F.size(c)                         # longueur
F.slice(c, debut, longueur)       # ATTENTION : debut est en base 1, pas 0
F.array_join(c, ";")              # recolle en chaîne
F.posexplode(c)                   # une ligne par élément, avec son indice
```

`slice` en base 1 et l'indexation `c[0]` en base 0 dans le même code : c'est le piège
classique de cette famille.

### Dates et horodatages

```python
F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss")     # motif JAVA, pas Python
F.current_timestamp()
```

Le format n'est **pas** celui de `strftime`. `yyyy` et non `%Y`, `HH` et non `%H`,
`mm` = minutes mais `MM` = mois.

### Constantes et métadonnées

```python
F.lit("valeur")                   # une valeur fixe en colonne
F.col("_metadata.file_name")      # colonne cachée des sources fichier
```

`F.lit` est obligatoire dès qu'une fonction attend une colonne et que tu veux lui donner
une valeur en dur.

### Fenêtres — pour dédupliquer et historiser

```python
from pyspark.sql import Window
w = Window.partitionBy("cle").orderBy(F.col("ts").desc())
df.withColumn("rang", F.row_number().over(w)).filter("rang = 1")
```

Le motif « garder la version la plus récente par clé ». Tu le rencontreras en M3 puis en
M4 ; c'est le seul concept vraiment nouveau de la partie PySpark.

### Le dépaquetage — `*` devant une liste

Du Python pur, mais tu ne le croiseras nulle part ailleurs qu'ici, alors autant le poser.

Dans un **appel**, `*` déballe une séquence en arguments séparés — et `**` fait le même
travail sur un dictionnaire, vers des arguments nommés :

```python
args = ["a", "b", "c"]
f(*args)                      # equivaut a  f("a", "b", "c")

opts = {"header": "true", "sep": ";"}
f(**opts)                     # equivaut a  f(header="true", sep=";")
```

Dans une **définition**, le même symbole fait l'inverse : il collecte les arguments reçus
dans un tuple (`def f(*args):`). Même signe, deux sens opposés selon le contexte.

À quoi ça sert ici : construire une liste de colonnes par compréhension, puis la passer à
une méthode qui attend des arguments séparés.

```python
metier = [c for c in df.columns if not c.startswith("_")]
df.select(*metier)
```

`select` est tolérant — il détecte le cas « un seul argument, et c'est une liste » et
l'aplatit lui-même, donc `df.select(metier)` marche aussi. **Mais cette tolérance ne joue
plus dès que tu mélanges** des arguments écrits à la main et une liste construite :

```python
w = Window.partitionBy("cle").orderBy(
    F.col("_ingested_at").desc(),
    *[F.col(c).asc_nulls_last() for c in metier],   # l'etoile est obligatoire ici
)
```

Trois arguments au lieu d'un : le cas spécial ne se déclenche pas, et sans l'étoile Spark
recevrait un objet liste au milieu de ses colonnes. Prendre l'habitude du `*` évite d'avoir
à retenir quelles méthodes sont tolérantes.

### Écrire

```python
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(nom)
```

En streaming, c'est `writeStream` et `.toTable(nom)` — jamais `.option("path", nom)`, qui
attend un **chemin** et pas un nom de table.

---

## Ce qui n'est pas dans ce parcours

Pour te rassurer sur le périmètre : **pas de classes**, pas de décorateurs (sauf `@dlt.table`
en M7, fourni tel quel), pas de UDF, pas de `pandas`, pas de compréhensions de listes
complexes. Si tu te retrouves à écrire une boucle sur des lignes de données, c'est le
signal que tu as quitté l'API DataFrame — et presque toujours qu'il existe une fonction
`F.*` qui fait le travail.
