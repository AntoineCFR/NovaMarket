# Complément M8 — Types de tâches et de déclencheurs

**Objectifs du guide** : *« Configurer les tâches courantes (notebook, requête SQL,
tableau de bord et pipeline) et leurs dépendances »* et *« implémenter des planifications
avec une compréhension des types de déclencheurs (programmé, arrivée de fichier, mise à
jour de table) »*.

Le job de M8 n'utilise que des tâches notebook et une planification programmée. Voici le
reste.

---

## Les types de tâches

| Type | Ce qu'il exécute | Quand c'est le bon choix |
|---|---|---|
| **Notebook** | Un notebook, avec ses paramètres | Logique complexe, PySpark, tout ce qui n'est pas une requête |
| **Requête SQL** | Une requête sauvegardée, sur un SQL warehouse | Rafraîchir un agrégat, une vérification, un `COPY INTO`. Pas de notebook à maintenir |
| **Tableau de bord** | Rafraîchit un tableau de bord AI/BI | Garantir que le tableau reflète le pipeline qui vient de finir |
| **Pipeline** | Déclenche une mise à jour d'un pipeline déclaratif | Intégrer un pipeline Lakeflow dans un DAG plus large |
| **Condition If/else** | N'exécute rien, aiguille | Fait en M8 |
| **For each** | Répète une tâche sur une liste | Fait en M8 |
| **Fichier Python / JAR / dbt / Spark Submit** | Du code packagé | Quand la logique vit dans une bibliothèque versionnée plutôt que dans un notebook |

### Ce que ça change concrètement pour NovaMarket

Trois tâches du parcours gagneraient à changer de type :

- Le chargement des référentiels de M1 est un `COPY INTO` déguisé en notebook → **tâche
  de requête SQL**.
- Le pipeline déclaratif de M7 tourne aujourd'hui à part → **tâche de pipeline**, en aval
  de l'ingestion, dans le même DAG.
- Le tableau de bord qualité de M6 se rafraîchit quand quelqu'un l'ouvre → **tâche de
  tableau de bord**, après `dq_checks`.

Un DAG qui mélange les quatre types est le cas normal en production. Un DAG qui n'utilise
que des notebooks trahit souvent une équipe qui n'a pas exploré l'outil.

> ⚠️ Free Edition : les tâches de requête SQL et de tableau de bord consomment le SQL
> warehouse unique, en `2X-Small`. Elles fonctionnent, elles sont lentes. C'est suffisant
> pour apprendre à les configurer.

---

## Les trois déclencheurs

| Déclencheur | Se déclenche quand | Latence | Piège |
|---|---|---|---|
| **Programmé** | À l'heure dite (cron) | Jusqu'à une période entière | Tourne à vide quand il n'y a rien |
| **Arrivée de fichier** | Un fichier apparaît dans un emplacement | Délai de détection, **pas instantané** | Une rafale de dépôts peut déclencher une cascade d'exécutions |
| **Mise à jour de table** | Une table Delta change | Délai de détection | Une table qui change souvent déclenche souvent |

### Choisir

Le critère n'est pas « le pilotage par la donnée, c'est mieux ». C'est la **tolérance à
la latence** croisée avec la **régularité de la source**.

| Source | Latence acceptable | Choix |
|---|---|---|
| Export quotidien à heure fixe | Quelques heures | Programmé. Simple, prévisible, on sait quand intervenir |
| Dépôts irréguliers d'un partenaire | Minutes | Arrivée de fichier, avec exécutions concurrentes plafonnées à 1 |
| Chaîner deux jobs | Le plus court possible | Mise à jour de table. Se caler sur l'horloge est fragile |
| Rafraîchir un tableau de bord après un pipeline | Immédiat | Mise à jour de table, ou une tâche dans le même job |

**L'argument sous-estimé en faveur du programmé** : il est *prévisible*. On sait quand le
pipeline tourne, donc quand les chiffres bougent, donc quand on peut intervenir sans
gêner personne. Un job qui peut démarrer à tout moment complique l'exploitation.

**Le piège du pilotage par la donnée** : un partenaire qui dépose 200 fichiers d'un coup
peut déclencher 200 exécutions. Le nombre maximal d'exécutions concurrentes est le
garde-fou — et en Free Edition, avec 5 tâches concurrentes au total, il faut le fixer à 1.

---

## À faire, en trente minutes

1. Ajoute au job de M8 une **tâche de requête SQL** qui compte les lignes de
   `gold.fact_order_line` et échoue si le résultat est nul.
2. Ajoute une **tâche de pipeline** qui déclenche le pipeline de M7.
3. Crée un job de démonstration à une tâche, avec un déclencheur **d'arrivée de fichier**
   sur `/Volumes/novamarket/landing/files/orders`. Dépose un fichier, **chronomètre le
   délai** avant démarrage, et note-le.
4. Crée un second job avec un déclencheur de **mise à jour de table** sur
   `novamarket.bronze.orders_raw`.

Le point 3 est le plus instructif : le chiffre que tu vas mesurer est celui qui t'évitera
de promettre du temps réel à quelqu'un.

---

## QCM associés

`exam/qcm-section-4.md`.
