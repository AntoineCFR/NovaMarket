# 🧰 Outillage — M4

*Cette fiche dit **avec quoi**, pas **comment**.*

---

## Ce que tu vas faire

Les clients et vendeurs changent : un vendeur passe de `BASIC` à `PREMIUM`, un client
déménage. Tes tables actuelles n'en gardent aucune trace — elles ne connaissent que
l'état d'aujourd'hui.

Tu vas construire des tables qui répondent à « **quel était son plan le 12 mars ?** ».
C'est ce qu'on appelle une historisation SCD2, et c'est la structure la plus utile — et
la plus casse-gueule — de tout le parcours.

---

## 1. Empiler les versions observées

Tes trois extractions de M2 sont trois photos successives. Il faut d'abord les remettre
bout à bout, dans l'ordre.

| Outil | Ce qu'il fait |
|---|---|
| `.unionByName(autre)` | empile deux DataFrames **par nom de colonne** |
| `.union(autre)` | empile **par position** — piège majeur, voir plus bas |
| `F.lit(numero)` | marquer de quelle extraction vient chaque ligne |
| `.orderBy(F.col(...).asc())` | ordonner les versions |

> `union` aligne par **position**, pas par nom. Si deux DataFrames ont les mêmes colonnes
> dans un ordre différent, le résultat est **faux et silencieux**. Tu as raté cette
> question au diagnostic (section 3, questions 1 et 2). Prends `unionByName` par défaut.

## 2. Détecter un vrai changement

Une extraction renvoie souvent des lignes identiques à la précédente. Les historiser
créerait des versions fantômes.

| Outil | Ce qu'il fait |
|---|---|
| `F.concat_ws(separateur, *colonnes)` | concatène plusieurs colonnes en une chaîne |
| `F.sha2(colonne, 256)` | empreinte d'une chaîne |
| `F.coalesce(c, F.lit("∅"))` | remplacer les `NULL` **avant** de concaténer |

Deux précautions que le corrigé prend et qui valent d'être comprises :

- un `NULL` dans `concat_ws` disparaît au lieu de compter — d'où le `coalesce` avec un
  jeton explicite ;
- le séparateur doit être un caractère qui n'apparaît **jamais** dans les données, sinon
  `("ab", "c")` et `("a", "bc")` produisent la même empreinte.

## 3. Comparer une ligne à la précédente

| Outil | Ce qu'il fait |
|---|---|
| `Window.partitionBy(cle).orderBy(...)` | une fenêtre par entité, ordonnée dans le temps |
| `F.lag(colonne).over(w)` | la valeur de la ligne **précédente** |
| `F.lead(colonne).over(w)` | celle de la ligne **suivante** |
| `F.row_number().over(w)` | numéroter les versions |

`lag` sert à repérer les changements. `lead` sert à fermer un intervalle : la date de fin
d'une version est la date de début de la suivante.

## 4. Les intervalles de validité

Convention imposée : `[valid_from, valid_to)` — début inclus, fin **exclue**. Elle évite
les chevauchements d'une seconde et les trous.

| Outil | Ce qu'il fait |
|---|---|
| `F.when(...).otherwise(...)` | poser `is_current` |
| `F.lit(None).cast("timestamp")` | la fin ouverte de la version courante |
| `.cast("timestamp")` | typer les bornes |

## 5. Écrire par `MERGE`

`MERGE` est du SQL, pas de l'API DataFrame.

| Outil | Ce qu'il fait |
|---|---|
| `.createOrReplaceTempView(nom)` | exposer un DataFrame au SQL du notebook |
| `spark.sql("MERGE INTO ... USING ... ON ... WHEN MATCHED ... WHEN NOT MATCHED ...")` | fusionner |

Un SCD2 ne se fait **pas** en un seul `MERGE` : fermer l'ancienne version et insérer la
nouvelle sont deux effets sur deux lignes différentes, et `MERGE` n'agit qu'une fois par
ligne source appariée. Le motif classique passe par une clé de fusion nulle pour la
branche d'insertion — c'est l'exercice.

## 6. Change Data Feed

| Outil | Ce qu'il fait |
|---|---|
| `TBLPROPERTIES (delta.enableChangeDataFeed = true)` | activer, à la création ou par `ALTER` |
| `table_changes('table', version_debut)` | lire les changements, en SQL |
| `DESCRIBE HISTORY table` | les versions et les opérations |

Le CDF n'enregistre **que ce qui suit son activation**. Activé après coup, il ne
reconstruit pas le passé.

## 7. Vérifier

| Outil | Ce qu'il fait |
|---|---|
| `.exceptAll(autre)` | les lignes de l'un absentes de l'autre — comparaison exacte |
| `.isEmpty()` | vrai si le DataFrame est vide |
| `.groupBy(cle).agg(F.sum(...))` | compter les versions courantes par entité |

> **Le contrôle qui vaut tous les autres** : reconstruis l'historisation *complète* à
> partir de zéro, et compare-la à celle obtenue par `MERGE` incrémental avec `exceptAll`.
> Les deux doivent être identiques. Si elles divergent, ton `MERGE` a une faille que
> personne ne verra avant des mois.

---

## Les pièges d'API de ce module

1. `union` contre `unionByName`.
2. `NULL` avalé par `concat_ws`.
3. Une fenêtre sans `orderBy` déterministe : le résultat change d'une exécution à l'autre.
4. `MERGE` qui échoue quand plusieurs lignes source correspondent à la même ligne cible.

## Le vocabulaire à retenir

**SCD2** · **`valid_from` / `valid_to`** · **version courante** · **empreinte de
changement** · **`MERGE`** · **Change Data Feed** · **time travel**.

Sections 2 et 3 de l'examen.
