# 🧰 Outillage — M13

*Cette fiche dit **avec quoi**, pas **comment**.*

---

## Ce que tu vas faire

Tu sais ingérer avec Auto Loader. Tu vas apprendre les **autres** voies, et surtout à
dire laquelle choisir — c'est cette question-là que l'examen pose, jamais « écris un
`COPY INTO` ».

Le module le plus court, et un excellent rappel avant l'épreuve.

---

## 1. `COPY INTO`

Du SQL pur.

```sql
COPY INTO <table>
FROM '<chemin>'
FILEFORMAT = CSV
FORMAT_OPTIONS ('sep' = ';', 'header' = 'true', 'encoding' = 'windows-1252')
COPY_OPTIONS ('mergeSchema' = 'true')
```

| Élément | Ce qu'il fait |
|---|---|
| `FILEFORMAT` | CSV, JSON, PARQUET, AVRO… |
| `FORMAT_OPTIONS` | les options du lecteur |
| `COPY_OPTIONS ('force' = 'true')` | **recharger** des fichiers déjà traités |
| `COPY_OPTIONS ('mergeSchema' = 'true')` | accepter de nouvelles colonnes |
| `FILES` / `PATTERN` | restreindre à certains fichiers |

> **La différence structurante avec Auto Loader** : l'état de progression de `COPY INTO`
> vit dans les **métadonnées de la table cible**, pas dans un checkpoint séparé.
>
> Conséquence, et c'est la question que tu as ratée au diagnostic : `TRUNCATE TABLE` vide
> les données mais **ne remet pas** l'historique des fichiers chargés. Relancer le même
> `COPY INTO` ne charge alors **aucune ligne**. Pour tout recharger : `force`, ou recréer
> la table.

## 2. Choisir entre les deux

| Situation | L'outil |
|---|---|
| Quelques fichiers, chargement ponctuel, équipe SQL | `COPY INTO` |
| Des milliers de fichiers, arrivées continues | Auto Loader |
| Le schéma évolue régulièrement | Auto Loader |
| Répertoire très peuplé | Auto Loader en mode notification — `COPY INTO` liste tout à chaque fois |
| Aucun état à créer ni à nettoyer | `COPY INTO` |

## 3. Lakeflow Connect

Les connecteurs managés vers Salesforce, Workday, SQL Server, ServiceNow… Ils apportent
l'état de progression, la capture des changements et la gestion du schéma — **sans code
à maintenir**.

Non reproductible en Free Edition : traité dans `FICHE-lakeflow-connect.md`.

À retenir pour l'examen : **Connect *amène* la donnée, les pipelines déclaratifs la
*transforment*.** Les deux commencent par « Lakeflow » et font des choses différentes.

## 4. JDBC et REST

| Outil | Ce qu'il fait |
|---|---|
| `spark.read.format("jdbc").option("url"/"dbtable"/"user"/"password", ...)` | lire une base relationnelle |
| `.option("partitionColumn"/"lowerBound"/"upperBound"/"numPartitions", ...)` | paralléliser la lecture |
| un client REST dans un notebook | dernier recours |

Ce que JDBC et REST **n'apportent pas**, et qu'il faut alors écrire soi-même : la
pagination, les jetons, l'évolution du schéma, la détection des suppressions, et la
reprise après échec. C'est l'argument central en faveur d'un connecteur managé.

## 5. Les déclencheurs

Rappel de M8, avec l'angle ingestion :

| Déclencheur | Quand |
|---|---|
| Programmé | rythme prévisible |
| Arrivée de fichier | dépôts imprévisibles, latence courte |
| Mise à jour de table | chaîner sans se caler sur l'horloge |

Un déclencheur d'arrivée de fichier sur un dépôt massif provoque une **cascade
d'exécutions** : le plafond d'exécutions concurrentes est le garde-fou. En Free Edition,
fixe-le à 1.

## 6. Semi-structuré

| Outil | Ce qu'il fait |
|---|---|
| `F.from_json(colonne, schema)` | parser une chaîne JSON avec un schéma |
| `F.get_json_object(c, "$.a.b")` | extraire un champ sans déclarer de schéma |
| `F.schema_of_json(exemple)` | déduire un schéma |
| `c.a.b` · `c["a"]` | naviguer dans un `STRUCT` |
| `F.explode` · `F.posexplode` | aplatir un `ARRAY` |
| `variant` | le type Databricks pour du semi-structuré hétérogène |

---

## La matrice de décision

C'est le vrai livrable du module : un tableau qui, pour chaque situation, donne l'outil et
**la raison**. Il est aussi la meilleure fiche de révision de la section 2 — 21 % de
l'examen, le deuxième bloc en poids.

Voir `FICHE-matrice-ingestion.md` après avoir écrit la tienne.

## Le vocabulaire à retenir

**`COPY INTO`** contre **Auto Loader** · **état dans la table** contre **checkpoint
séparé** · **`force`** · **Lakeflow Connect** · **connecteur managé** ·
**`from_json` / `get_json_object`**.

Section 2 — 21 %.
