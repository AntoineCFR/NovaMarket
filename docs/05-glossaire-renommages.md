# Glossaire des renommages — le piège n°1 de la préparation par IA

L'étape 3 du guide *AI Prep Guide* de Databricks est catégorique : tous les modèles ont
été entraînés avant les changements récents de la plateforme, et ils emploient
**avec assurance** des noms de produits périmés en décrivant des workflows dépréciés
comme s'ils étaient courants.

**Colle ce tableau au début de chaque session de révision avec une IA**, y compris avec
moi. C'est le geste qui évite le principal mode d'échec de cette méthode de préparation.

---

## Renommages confirmés

| Ancien nom | Nom actuel | Ce que c'est |
|---|---|---|
| Delta Live Tables (DLT) | **Lakeflow Declarative Pipelines** — ou *Lakeflow Spark Declarative Pipelines* | Pipelines déclaratifs : on décrit des jeux de données et leurs contraintes, le moteur déduit le graphe et gère l'état |
| Databricks Workflows | **Lakeflow Jobs** | Orchestration : DAG de tâches, planification, déclencheurs, nouvelles tentatives |
| Databricks Repos | **Databricks Git Folders** | Intégration Git dans le workspace : branches, commits, pull requests |
| Databricks Asset Bundles (DAB) | **Declarative Automation Bundles** | Packaging déclaratif en YAML de jobs, pipelines et autres ressources, promu entre environnements |
| `APPLY CHANGES INTO` | **`AUTO CDC`** | Application de changements de type CDC dans un pipeline déclaratif |
| Community Edition | **Free Edition** | L'offre gratuite actuelle, sans rapport technique avec l'ancienne |
| Table ACLs / Hive metastore | **Unity Catalog** | Gouvernance : catalogs, schemas, tables, volumes, privilèges, lineage |

---

## Ce qui n'a **pas** changé, et qu'on croit souvent renommé

| Nom | Remarque |
|---|---|
| Auto Loader, `cloudFiles` | Inchangé. C'est la source de streaming pour l'ingestion de fichiers |
| Le module Python **`dlt`** | **Inchangé dans le code.** Le produit s'appelle Lakeflow Declarative Pipelines, l'import reste `import dlt`. Ne « corrige » pas le code |
| Delta Lake, `MERGE INTO`, Change Data Feed | Inchangés |
| Photon, Delta Sharing, Partner Connect | Inchangés |
| Databricks SQL | Inchangé (l'ancien *SQL Analytics* est loin) |

---

## Pièges de contenu, pas seulement de nom

Ce sont les affirmations qu'une IA sort le plus volontiers alors qu'elles sont périmées.

| Ce qu'une IA risque de dire | Ce qui est vrai aujourd'hui |
|---|---|
| « Utilise `OPTIMIZE ... ZORDER BY` pour accélérer les filtres » | **Liquid Clustering** (`CLUSTER BY`) est la recommandation pour les nouvelles tables. Le Z-ordering reste supporté mais n'est plus le choix par défaut |
| « Configure `spark.sql.shuffle.partitions` à 200 et ajuste » | L'*adaptive query execution* ajuste dynamiquement, y compris la gestion du *skew join*. On règle à la main en connaissance de cause, pas par réflexe |
| « Monte le DBFS et écris dans `/mnt/...` » | Les **volumes Unity Catalog** (`/Volumes/catalog/schema/volume/`) sont le chemin gouverné. Les montages DBFS sont hérités |
| « Crée un cluster interactif avec N workers » | Sur beaucoup d'usages, et en Free Edition **exclusivement**, c'est du **serverless** : pas de configuration de nœuds |
| « Lance ton stream avec `trigger(processingTime='5 minutes')` » | Sur serverless, seul **`availableNow`** est disponible en notebook et en job |
| « `APPLY CHANGES INTO` pour le CDC déclaratif » | `AUTO CDC` |
| « Les Repos permettent de versionner » | **Git Folders** |
| « Déploie avec les Asset Bundles » | **Declarative Automation Bundles**, commande CLI `databricks bundle` |

---

## Le prompt de mise en condition

À coller au début de chaque session, avec le guide d'examen officiel en pièce jointe et
ce tableau :

```
Je prépare la certification Databricks décrite dans le guide d'examen joint.
Quand tu m'expliques quelque chose : n'utilise que les noms de produits et les
comportements actuels, et n'invente pas d'objectifs absents du guide. Appuie tout
ce que tu enseignes sur les sources officielles — docs.databricks.com et Databricks
Academy — pas sur des blogs, tutoriels ou forums tiers. Si tu peux naviguer,
cherche sur docs.databricks.com et cite l'URL de la page pour chaque affirmation
importante. Si tu ne peux pas confirmer une affirmation dans la documentation
officielle, ou si tu ne peux pas naviguer, dis-le explicitement plutôt que de
deviner, et indique-moi où vérifier moi-même.
```

---

## Entretien de ce document

Databricks met le guide d'examen à jour sans annonce. La règle du guide IA :
**re-télécharger le guide officiel deux semaines avant l'examen** et vérifier que les
objectifs n'ont pas bougé.

Si un nom change, corrige ce tableau **et** relance une recherche dans le dépôt :

```bash
grep -ril "ancien nom" modules/ solutions/ docs/ graders/
```
