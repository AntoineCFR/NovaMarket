# M12 — Performance, monitoring et optimisation

**Section 6 de l'examen · 10 % · ~5 questions** — plus une partie de la section 3
(paramètres de tuning) et de la section 1 (services de compute).

**Durée estimée** : 3 h 30 · **Prérequis** : M5 validé

> 🧰 **Commence par `OUTILLAGE.md`** — les bibliothèques et fonctions dont tu auras besoin,
> sans les réponses. Et `docs/07-python-pour-le-parcours.md` si c'est ton premier passage.

---

## Objectif

Jusqu'ici, tes pipelines sont **justes**. On ne s'est jamais demandé s'ils étaient
**rapides**, ni comment on le saurait.

Ce module t'apprend à lire ce que le moteur raconte de lui-même, à nommer un goulot
d'étranglement plutôt qu'à le contourner au hasard, et à mesurer avant de conclure.

> La règle du module : **aucune affirmation de performance sans mesure**. « Le broadcast
> est plus rapide » ne vaut rien ; « 4,2 s contre 11,8 s sur cette requête, ce volume et
> ce compute » vaut quelque chose.

---

## Ce que tu dois produire

| Objet | Rôle |
|---|---|
| `ops.skew_demo` | Un jeu déséquilibré, pour voir un vrai *skew* |
| `ops.perf_baseline` / `ops.perf_clustered` | Deux copies identiques, une seule groupée |
| `ops.perf_measurements` | Le journal des mesures — c'est le livrable |
| `ops.job_perf_trend` | Les tendances de durée par tâche |

---

## Partie 1 — Fabriquer un déséquilibre

Les données du projet sont trop bien réparties pour montrer quoi que ce soit : 600
vendeurs, ~470 lignes chacun. Construis `ops.skew_demo` en amplifiant **un seul** vendeur
jusqu'à ce qu'il pèse environ 40 % des lignes.

C'est une table de démonstration, à part : elle ne touche à rien du projet et ne
recalibre aucun grader existant.

---

## Partie 2 — Lire le Spark UI

Lance une jointure entre `ops.skew_demo` et `gold.dim_seller`, puis ouvre le Spark UI
(ou le *query profile*) depuis la cellule.

Trois choses à savoir repérer, ce sont exactement celles de l'objectif d'examen :

| Symptôme | Ce que tu vois | Ce que ça veut dire |
|---|---|---|
| **Data skew** | Dans le résumé des tâches d'un stage : médiane à 30 s, maximum à 10 min. `Max` de *shuffle read* dix fois la médiane | Une clé concentre les données. Une seule tâche fait tout le travail |
| **Shuffling** | Un stage avec beaucoup de *shuffle write* puis *shuffle read* | Les données traversent le réseau pour être regroupées. Inévitable sur un `groupBy` ou une jointure large, coûteux |
| **Disk spilling** | Colonnes *Spill (memory)* et *Spill (disk)* non nulles | La partition ne tenait pas en mémoire. Elle est passée par le disque |

**Question centrale** : sur ta jointure, l'écart entre médiane et maximum vient-il du
*skew* ou de la taille absolue ? Ce n'est pas la même réponse, ni le même remède.

---

## Partie 3 — Les paramètres de tuning

Les quatre que le guide nomme explicitement :

| Paramètre | Ce qu'il fait |
|---|---|
| `spark.sql.shuffle.partitions` | Nombre de partitions après un *shuffle* |
| `spark.sql.autoBroadcastJoinThreshold` | Taille en dessous de laquelle une table est diffusée à tous les exécuteurs au lieu d'être mélangée |
| `spark.default.parallelism` | Parallélisme par défaut des RDD |
| `spark.executor.memory` / `spark.driver.memory` | Mémoire allouée |

**Mesure trois variantes** de la même jointure et consigne-les :

1. Par défaut, telle quelle.
2. `autoBroadcastJoinThreshold = -1` — le *broadcast* est interdit, jointure par *shuffle*.
3. `autoBroadcastJoinThreshold` remis à une valeur généreuse.

> ⚠️ Sur serverless, **certains de ces paramètres sont verrouillés** : le moteur gère
> lui-même le dimensionnement et la mémoire. Note lesquels acceptent d'être modifiés et
> lesquels sont ignorés ou refusés — c'est une information d'examen à part entière, la
> section 1 portant sur les caractéristiques et limites des services de compute.

### `ops.perf_measurements`

| Colonne | Type |
|---|---|
| `scenario` | `string` |
| `variant` | `string` |
| `config` | `string` — le paramètre appliqué |
| `duration_ms` | `bigint` |
| `rows_out` | `bigint` |
| `measured_at` | `timestamp` |

`rows_out` n'est pas décoratif : deux variantes qui ne renvoient pas le même nombre de
lignes ne mesurent pas la même chose, et la comparaison ne veut rien dire.

**Méthode de mesure** : Spark est paresseux. Chronométrer la construction d'un DataFrame
mesure la compilation du plan, pas l'exécution. Il faut forcer le calcul, et te méfier du
cache entre deux variantes.

---

## Partie 4 — Liquid clustering

Le **liquid clustering** (`CLUSTER BY`) remplace le couple partitionnement + Z-ordering
pour les nouvelles tables. Il regroupe physiquement les lignes proches sur les colonnes
choisies, sans figer une arborescence de partitions.

Protocole propre :

1. `ops.perf_baseline` — copie de `gold.fact_order_line`, sans regroupement.
2. `ops.perf_clustered` — même contenu, avec `CLUSTER BY (order_date, seller_id)`,
   puis `OPTIMIZE`.
3. La même requête filtrée sur les deux, mesurée.

Deux copies plutôt qu'une mesure avant/après sur la même table : sinon le cache du
premier passage fausse le second.

**Ce qu'il faut regarder**, plus que la durée : le nombre de fichiers lus. C'est
l'*élagage de fichiers* qui produit le gain, pas une magie du moteur. `DESCRIBE DETAIL`
te donne le nombre de fichiers, le *query profile* te donne ceux réellement lus.

### Predictive optimization

Sur les tables managées d'Unity Catalog, Databricks déclenche seul `OPTIMIZE` et
`VACUUM` quand c'est rentable. Vérifie l'état sur tes tables et sache dire ce que ça
change dans ta charge de travail d'exploitation.

---

## Partie 5 — Tendances de jobs

L'objectif du guide parle de comparer les durées d'exécution actuelles à un historique.
Tu as déjà la matière : `ops.pipeline_runs` porte un `started_at` et un `ended_at` par
tâche depuis M0.

Construis `ops.job_perf_trend` : par tâche, la durée de la dernière exécution, la moyenne
des précédentes, et l'écart relatif. C'est le même motif que la détection de dérive de
M9 — appliqué au pipeline au lieu des données.

---

## Partie 6 — Fiche diagnostic

📖 **Non reproductible en Free Edition** : échecs de démarrage de cluster, conflits de
bibliothèques et saturation mémoire supposent de configurer un cluster, ce que le
serverless ne permet pas. Traité dans
`modules/M12-performance/FICHE-diagnostic-compute.md`, avec QCM.

---

## Critères d'acceptation

Comportementaux et relatifs : aucun seuil de durée en dur — une mesure de performance
dépend du compute, et un grader qui exigerait « moins de 3 secondes » serait faux la
semaine prochaine.

| # | Critère |
|---|---|
| 1 | `ops.skew_demo` existe |
| 2 | Une clé y concentre plus de 30 % des lignes |
| 3 | `ops.perf_measurements` : schéma exact |
| 4 | Au moins 2 scénarios distincts mesurés |
| 5 | Le scénario de jointure compte au moins 3 variantes |
| 6 | Toutes les durées sont strictement positives |
| 7 | Dans un même scénario, toutes les variantes renvoient le **même** `rows_out` |
| 8 | `ops.perf_clustered` déclare des colonnes de regroupement |
| 9 | `ops.perf_baseline` n'en déclare pas |
| 10 | Les deux tables contiennent le même nombre de lignes |
| 11 | Le scénario de regroupement est mesuré sur les deux variantes |
| 12 | `ops.job_perf_trend` : une ligne par tâche, avec durée et écart à la moyenne |

---

## Questions

1. Sur ta jointure déséquilibrée, l'*adaptive query execution* a-t-elle corrigé le *skew*
   toute seule ? Comment l'as-tu vérifié dans le plan ?
2. `autoBroadcastJoinThreshold = -1` a-t-il ralenti ta jointure ? De combien ? Et à
   partir de quelle taille de table de droite le *broadcast* devient-il une mauvaise idée ?
3. Quels paramètres de tuning ton compute serverless a-t-il refusé ou ignoré ? Qu'est-ce
   que ça dit du modèle serverless, et dans quel cas choisirait-on encore un compute
   configurable ?
4. Le liquid clustering a-t-il changé quelque chose sur ta requête ? Si le gain est
   faible, est-ce que ça veut dire qu'il est inutile — ou que ton jeu de test est trop
   petit ? Comment trancher honnêtement ?
5. Tu as trois leviers face à une tâche lente : plus de compute, un meilleur plan, moins
   de données. Classe-les par rapport coût/bénéfice, et dis lequel on essaie toujours en
   premier dans la vraie vie — et pourquoi c'est presque toujours le mauvais choix.
