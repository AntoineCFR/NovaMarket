# Les graders

Un notebook de validation par module. Il ne lit jamais ton code, seulement l'état
d'Unity Catalog — libre à toi d'obtenir le résultat comme tu veux.

---

## À importer une seule fois, avant le premier grader

Chaque grader commence par `%run ./_grader_lib`, qui charge la classe `Grader`. Ce chemin
est **relatif au dossier du grader** : `_grader_lib` doit être un notebook frère, dans le
même répertoire du workspace.

Si tu ne l'as pas importé, tu obtiens :

```
Notebook not found: Users/<toi>/Training/M0_Setup/_grader_lib
```

Le plus simple est d'importer **tout le dossier d'un coup**, dans un répertoire dédié :

```bash
databricks workspace import-dir ./graders /Users/<ton-email>/Training/graders
```

Par l'interface, à défaut : sur le dossier cible, menu `⋮` → **Import** → *File* →
sélectionne les `.py` voulus **plus `_grader_lib.py`** → format *Source*. Databricks les
convertit en notebooks (le `.py` disparaît du nom, `_grader_lib.py` devient
`_grader_lib` : c'est bien ce qu'attend le `%run`).

Le sous-dossier `expected/` n'a **pas** besoin d'être importé : il sert à calibrer les
graders depuis ton poste, pas à les exécuter.

---

## Exécution

Chaque grader expose un widget `catalog`, par défaut `novamarket`. Si tu as nommé ton
catalog autrement, change-le en tête de notebook — et garde la même valeur partout.

Un module est validé quand son grader passe au vert.

---

## Ce que les graders mesurent, et ce qu'ils ne mesurent pas

Les graders **M0 à M9 dépendent de l'état des vagues**. Leurs comptages décrivent le
système *à ce moment du parcours*. Relancer le grader de M1 après avoir ingéré W4
échouera : ce n'est pas une régression, c'est la vague qui a changé les chiffres.

Les graders **M10 à M13 ne comptent aucune ligne** — uniquement des comportements et des
ratios. Ils restent valables quelle que soit la vague ingérée.

Trois vérifications sont posées en `soft()` : elles émettent un avertissement au lieu
d'échouer, parce que la fonctionnalité peut ne pas être disponible sur ton workspace
(politiques ABAC, Spark UI en serverless, déclencheur d'arrivée de fichier sur un volume).
Un `WARN` sur ces points n'est pas un échec de ta part — c'est une contrainte de
plateforme à vérifier sur docs.databricks.com.
