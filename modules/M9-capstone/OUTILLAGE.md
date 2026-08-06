# 🧰 Outillage — M9

*Cette fiche dit **avec quoi**, pas **comment**.*

---

## Ce que tu vas faire

La vague **W4** contient un incident de production. Aucun de tes contrôles techniques ne
le détectera : les valeurs sont parfaitement valides, les types corrects, les comptages
plausibles. Seul le **métier** ne tient pas.

Tu vas le trouver, en mesurer l'ampleur, décider quoi faire, et réparer — dans un ordre
qui compte.

Pas de nouvelle API dans ce module. Tout ce dont tu as besoin, tu l'as déjà écrit. Ce qui
est nouveau, c'est la démarche.

---

## 1. Détecter par comparaison, pas par règle

Un contrôle ne trouve que ce qu'il cherche. Ici, cherche des **ruptures**.

| Outil | Ce qu'il fait |
|---|---|
| `.groupBy(dimension).agg(F.sum(...), F.avg(...), F.count(...))` | ventiler un agrégat |
| `Window.partitionBy(...).orderBy(...)` + `F.lag(...)` | comparer une période à la précédente |
| `F.avg` · `F.stddev` · `F.percentile_approx` | situer une valeur dans une distribution |
| `.orderBy(F.col(...).desc())` | faire remonter les extrêmes |

Les bonnes dimensions de ventilation : le jour, le vendeur, le produit, le pays. L'anomalie
se voit dans le rapport entre deux, pas dans une valeur absolue.

## 2. Qualifier

| Outil | Ce qu'il fait |
|---|---|
| `.filter(...)` | isoler la population suspecte |
| `.count()` · `F.sum(...)` | combien de lignes, combien d'euros |
| `.join(referentiel, ...)` | recouper avec ce qu'on sait par ailleurs |
| `F.round(a / b, 2)` | un ratio parle mieux qu'un écart |

Avant de réparer : **combien de lignes, quel montant, quels vendeurs, quel jour**. Une
réparation dont on ne sait pas mesurer l'effet n'est pas une réparation.

## 3. Marquer plutôt que supprimer

| Outil | Ce qu'il fait |
|---|---|
| `F.when(condition, F.lit("MOTIF")).otherwise(...)` | poser un drapeau |
| `F.array(...)` · `F.array_compact(...)` | cumuler plusieurs motifs |
| `.write.mode("append").saveAsTable(...)` | alimenter la quarantaine |

Supprimer les lignes suspectes fait disparaître la preuve. Le motif du parcours reste le
même depuis M3 : **on écarte, on ne jette pas**.

## 4. Revenir en arrière

| Outil | Ce qu'il fait |
|---|---|
| `DESCRIBE HISTORY table` | les versions, leur opération, leur horodatage |
| `SELECT * FROM table VERSION AS OF n` | lire une version passée |
| `SELECT * FROM table TIMESTAMP AS OF '...'` | idem par date |
| `RESTORE TABLE table TO VERSION AS OF n` | y revenir |

> `RESTORE` remet la **table** dans son état passé. Il ne touche **pas** au checkpoint
> d'Auto Loader, qui continue de croire avoir lu les fichiers. Restaurer une table bronze
> sans réinitialiser le flux te laisse un trou permanent. C'est le piège du module.

## 5. Rejouer

| Outil | Ce qu'il fait |
|---|---|
| `dbutils.fs.rm(chemin, True)` | réinitialiser un checkpoint |
| `databricks fs cp -r ...` | reposer des fichiers |
| `MERGE INTO` | réappliquer sans dupliquer |

---

## L'ordre compte

C'est la leçon centrale, et elle ne se trouve dans aucune API.

Deux réparations sont nécessaires, et **l'une conditionne l'autre**. Faite dans le mauvais
ordre, la seconde n'attrape qu'une fraction des lignes — et le résultat *ressemble* à un
succès : le grader ne t'aidera pas, les comptages seront plausibles, et 90 % du problème
sera toujours là.

Avant de lancer quoi que ce soit, écris la séquence. Puis justifie l'ordre.

## Les questions auxquelles l'outillage ne répond pas

- Quel contrôle **aurais-tu dû** avoir en place pour que cet incident se voie tout seul ?
  Ajoute-le à `ops.dq_metrics`.
- Faut-il corriger les données, ou les marquer et laisser le métier trancher ?
- L'incident a-t-il contaminé la couche gold ? Les agrégats déjà consultés ?

## Le vocabulaire à retenir

**Time travel** · **`RESTORE`** · **checkpoint désynchronisé** · **quarantaine
métier** · **rejeu idempotent** · **ordre de réparation**.

Sections 1, 3 et 6 de l'examen.
