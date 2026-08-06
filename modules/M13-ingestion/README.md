# M13 — Les méthodes d'ingestion que le projet n'utilise pas

**Section 2 de l'examen · 21 % · ~9 questions** — la deuxième section la plus lourde.
NovaMarket n'en exerce qu'une partie.

**Durée estimée** : 2 h 30 · **Prérequis** : M1 validé

> 🧰 **Commence par `OUTILLAGE.md`** — les bibliothèques et fonctions dont tu auras besoin,
> sans les réponses. Et `docs/07-python-pour-le-parcours.md` si c'est ton premier passage.

---

## Objectif

Tu sais faire de l'ingestion incrémentale avec Auto Loader. L'examen te demandera surtout
de **choisir** entre plusieurs méthodes, et de justifier ce choix sur des critères de
volume, de fréquence, de type de données et de gouvernance.

Ce module met `COPY INTO` en concurrence directe avec Auto Loader sur exactement les mêmes
fichiers, puis couvre les déclencheurs et les connecteurs que le parcours n'a pas croisés.

---

## Partie 1 — `COPY INTO` contre Auto Loader

Charge `bronze.orders_copyinto` depuis **le même répertoire** que `bronze.orders_raw`,
avec les mêmes options de format. À la fin, les deux tables doivent contenir le même
nombre de lignes — c'est le critère qui prouve que tu n'as rien perdu en route.

Puis relance `COPY INTO` **sans rien ajouter** dans le volume, et observe.

### Ce que tu dois pouvoir expliquer ensuite

| | `COPY INTO` | Auto Loader |
|---|---|---|
| Comment il sait ce qu'il a déjà lu | ? | ? |
| Où vit cet état | ? | ? |
| Que se passe-t-il si on le perd | ? | ? |
| Passage à l'échelle sur un répertoire à millions de fichiers | ? | ? |
| Évolution de schéma | ? | ? |
| Peut-il tourner en streaming | ? | ? |

Remplis ce tableau **après avoir manipulé**, pas avant. Les deux colonnes ont des
réponses différentes sur les six lignes, et c'est exactement ce que teste l'objectif
« prioriser entre les méthodes d'ingestion ».

### `ops.ingestion_comparison`

Le livrable de la partie. Schéma imposé :

| Colonne | Type |
|---|---|
| `method` | `string` — `COPY_INTO` ou `AUTO_LOADER` |
| `target_table` | `string` |
| `run_number` | `int` — 1 puis 2 |
| `rows_after` | `bigint` — nombre de lignes **en cible** après l'exécution |
| `rows_added` | `bigint` |
| `notes` | `string` |
| `measured_at` | `timestamp` |

Deux exécutions par méthode. La deuxième doit ajouter **zéro ligne** dans les deux cas —
et les mécanismes qui produisent ce zéro ne sont pas les mêmes.

---

## Partie 2 — Les déclencheurs

Le guide nomme trois types, dont deux que M8 n'a pas utilisés.

| Déclencheur | Quand il se déclenche | Cas d'usage |
|---|---|---|
| **Programmé** | À l'heure dite | Le job de nuit. Fait en M8 |
| **Arrivée de fichier** | Quand un fichier apparaît dans un emplacement | Un partenaire dépose quand il veut |
| **Mise à jour de table** | Quand une table Delta change | Chaîner deux jobs sans se coordonner sur l'horloge |

Configure les deux nouveaux sur des jobs de démonstration à une tâche :

- **Arrivée de fichier** sur `/Volumes/novamarket/landing/files/orders`. Dépose un fichier
  et observe le déclenchement. Note le **délai** entre le dépôt et le démarrage.
- **Mise à jour de table** sur `novamarket.bronze.orders_raw`.

> Le déclencheur d'arrivée de fichier interroge l'emplacement périodiquement — ce n'est
> pas de l'événementiel instantané. Mesurer ce délai t'évitera de promettre du temps réel
> à quelqu'un.

**La question de l'objectif d'examen** : quand choisir un déclencheur temporel plutôt
qu'un déclencheur piloté par la donnée ? Ce n'est pas « la donnée, c'est mieux » — les
deux ont des cas où ils sont mauvais.

---

## Partie 3 — Fiches

Deux fiches de décision, non reproductibles en Free Edition, mais explicitement au
programme :

- `FICHE-lakeflow-connect.md` — connecteurs standard et managés, ce qu'ils font, quand
  ils remplacent du code
- `FICHE-matrice-ingestion.md` — l'arbre de décision complet : volume, fréquence, types,
  gouvernance

---

## Critères d'acceptation

Relatifs, jamais absolus : le grader compare les deux tables **entre elles**, donc les
critères tiennent quelle que soit la vague ingérée.

| # | Critère |
|---|---|
| 1 | `bronze.orders_copyinto` existe |
| 2 | Elle contient **exactement autant de lignes** que `bronze.orders_raw` |
| 3 | Elle porte une colonne de sauvetage |
| 4 | Les colonnes source y sont en `STRING` |
| 5 | `ops.ingestion_comparison` : schéma exact |
| 6 | Les deux méthodes y figurent |
| 7 | Chaque méthode a deux exécutions enregistrées |
| 8 | La deuxième exécution ajoute **0 ligne** pour les deux méthodes |
| 9 | Le `rows_after` final est identique entre les deux méthodes |
| 10 | Le tableau comparatif des six lignes est rempli dans le notebook *(vérification manuelle)* |

Le critère 9 est le plus parlant : deux mécanismes d'état complètement différents, un
même résultat au fichier près.

---

## Questions

1. Remplis le tableau des six lignes. Puis : dans quel cas concret choisirais-tu
   `COPY INTO` alors qu'Auto Loader est disponible ?
2. Tu supprimes le checkpoint d'Auto Loader et tu relances. Que se passe-t-il ? Même
   question en supprimant l'historique de `COPY INTO` — et d'ailleurs, où est-il ?
3. Ton répertoire de landing contient un million de fichiers. Laquelle des deux méthodes
   se dégrade en premier, et pourquoi ?
4. Un partenaire dépose un fichier trois fois par jour, à heures irrégulières. Déclencheur
   temporel ou déclencheur d'arrivée de fichier ? Défends le choix inverse du tien.
5. Une équipe te demande d'ingérer une base Salesforce. Tu peux écrire un script JDBC
   orchestré par un job, ou configurer un connecteur managé Lakeflow Connect. Sur quels
   critères tranches-tu ?
