# M8 — Orchestration avec Lakeflow Jobs

**Durée estimée** : 4 h · **Prérequis** : M6 validé

> 🧰 **Commence par `OUTILLAGE.md`** — les bibliothèques et fonctions dont tu auras besoin,
> sans les réponses. Et `docs/07-python-pour-le-parcours.md` si c'est ton premier passage.

---

## Objectif

Tu as huit notebooks qui produisent le bon résultat quand tu les lances dans le bon
ordre. C'est un prototype, pas un pipeline.

Ce module en fait un job qui tourne seul, se rattrape en cas d'échec, ne calcule le gold
que si la qualité est au vert, et te réveille si ça casse.

Et pendant que tu y es, **la source va changer de schéma sans prévenir.**

---

## Contrainte Free Edition

**Maximum 5 tâches concurrentes par compte.** Ce n'est pas un handicap, c'est une
contrainte de conception normale : un DAG à 40 branches parallèles est presque toujours
le signe qu'on n'a pas réfléchi aux dépendances réelles.

Ton DAG ne doit jamais avoir plus de **5 tâches prêtes à démarrer en même temps**. La
tâche `for_each` compte pour autant de tâches que sa concurrence configurée.

---

## Le DAG attendu

```
                 ┌──────────────────┐
                 │ ingest_files     │  for_each sur [orders, events, ref]
                 │ (concurrence 3)  │  retries : 2
                 └────────┬─────────┘
                          │
   ┌──────────────────────┼──────────────────────┐
   │                      │                      │
┌──┴───────────┐   ┌──────┴────────┐    ┌────────┴──────┐
│ ingest_oltp  │   │ silver_orders │    │ silver_events │
└──┬───────────┘   └──────┬────────┘    └────────┬──────┘
   │                      │                      │
┌──┴───────────┐          │                      │
│ scd2         │          │                      │
└──┬───────────┘          │                      │
   └──────────────────────┼──────────────────────┘
                          │
                    ┌─────┴──────┐
                    │ gold       │
                    └─────┬──────┘
                          │
                    ┌─────┴──────┐
                    │ dq_checks  │  publie une valeur de tâche : dq_status
                    └─────┬──────┘
                          │
                   ┌──────┴───────┐
                   │ if dq_status │  tâche conditionnelle
                   │   == 'PASS'  │
                   └──┬────────┬──┘
                  vrai│        │faux
              ┌───────┴──┐  ┌──┴──────────────┐
              │ publish  │  │ alert_and_stop  │
              └──────────┘  └─────────────────┘
```

Concurrence maximale : 3 (le `for_each`) puis 3 (`ingest_oltp`, `silver_orders`,
`silver_events`). Jamais plus de 5. ✓

---

## Ce que tu dois mettre en place

### 1. Paramètres de job

Deux paramètres au niveau du job, lus par tous les notebooks via des widgets :

| Paramètre | Valeur par défaut |
|---|---|
| `catalog` | `novamarket` |
| `run_id` | `{{job.run_id}}` |

`{{job.run_id}}` est une **référence dynamique** : Databricks la remplace à l'exécution.
C'est ce qui va permettre de retrouver toutes les tâches d'une même exécution dans
`ops.pipeline_runs`, et c'est le minimum vital pour déboguer un job de nuit.

Adapte tes notebooks : `run_id` doit venir du paramètre quand il existe, et retomber sur
un `uuid4()` en exécution manuelle.

### 2. La tâche `for_each`

Elle itère sur les trois référentiels : `["categories", "sellers", "products"]`.

Adapte `M1_bronze_ref.py` pour qu'il prenne un widget `ref_name` et ne traite qu'un
référentiel par exécution. Chaque itération écrit sa propre ligne dans
`ops.pipeline_runs`, avec `source_name` = le nom du référentiel.

> Concrètement, `for_each` sert surtout quand la liste est **dynamique** : partitions à
> retraiter, pays à charger, clients à facturer. Sur trois référentiels codés en dur, un
> `for_each` est discutable — trois tâches seraient plus lisibles. Fais-le ici pour
> l'apprendre, et pose-toi la question la prochaine fois.

### 3. Les nouvelles tentatives

`ingest_files` doit avoir **au moins 2 nouvelles tentatives**, avec 60 secondes entre
chaque. Tu vas comprendre pourquoi à l'étape 5.

### 4. La valeur de tâche et la condition

`dq_checks` publie le résultat de la campagne de contrôles :

```python
dbutils.jobs.taskValues.set(key="dq_status", value="PASS")
```

La tâche conditionnelle compare `{{tasks.dq_checks.values.dq_status}}` à `PASS`.

Attention à ce qui est comparé : ce n'est pas « aucun contrôle n'a échoué », c'est
« aucun contrôle **bloquant** n'a échoué ». Tu as tranché ça en M6 — applique-le.

### 5. La dérive de schéma

Téléverse la vague W3 :

```bash
databricks fs cp -r "data/waves/W3/orders" "dbfs:/Volumes/novamarket/landing/files/orders" --overwrite
```

```bash
databricks fs cp -r "data/waves/W3/events" "dbfs:/Volumes/novamarket/landing/files/events" --overwrite
```

Puis lance le job.

**Il va échouer.** Regarde l'erreur avant de lire la suite.

La source a ajouté deux colonnes au fichier de commandes sans prévenir. Auto Loader,
configuré en `schemaEvolutionMode = addNewColumns`, ne peut pas continuer avec un schéma
qu'il ne connaît pas : il lève une exception, met à jour son emplacement de schéma, et
s'arrête. La tentative suivante démarre avec le nouveau schéma et passe.

C'est un comportement **voulu**, et c'est exactement pourquoi la consigne demandait des
nouvelles tentatives. Un job sans retry sur cette tâche te réveille à 3 h du matin pour
un incident qui se résout tout seul.

Vérifie ensuite ce que sont devenues les colonnes `promo_code` et `channel` pour les
lignes ingérées **avant** W3.

### 6. Planification et notifications

- Une planification quotidienne (désactive-la après avoir validé le module : le quota
  Free Edition n'aime pas les jobs qui tournent tous les jours pour rien).
- Une notification par e-mail sur échec.
- Un timeout au niveau du job. Choisis-le et justifie-le : un job sans timeout qui se
  bloque consomme ton quota jusqu'à épuisement.

### 7. `ops.job_runs`

Une dernière tâche écrit le bilan de l'exécution :

| Colonne | Type |
|---|---|
| `job_run_id` | `string` |
| `job_name` | `string` |
| `started_at`, `ended_at` | `timestamp` |
| `n_tasks` | `int` |
| `status` | `string` |
| `notes` | `string` |

---

## Livrables

1. Le job, fonctionnel, avec une exécution réussie après la dérive.
2. `jobs/novamarket_daily.job.yml` — la définition exportée. *Jobs & Pipelines → ton job
   → menu ⋮ → View YAML*. Un job qui n'existe que dans une interface graphique n'est pas
   versionné, donc pas reproductible.
3. `ops.job_runs` alimentée.

---

## Critères d'acceptation

État attendu après ingestion de W3 et exécution complète du job.

| # | Critère | Attendu |
|---|---|---|
| 1 | `bronze.orders_raw` : lignes | **289 272** |
| 2 | `bronze.orders_raw` : fichiers sources distincts | **9** |
| 3 | Les colonnes `promo_code` et `channel` existent | — |
| 4 | Elles sont nulles pour les lignes antérieures à W3 | **287 785** lignes nulles |
| 5 | `bronze.events_raw` : lignes | **139 428** |
| 6 | `silver.order_line` : lignes | **283 556** |
| 7 | `ops.quarantine_order_line` : lignes | **2 240** |
| 8 | Invariant silver + quarantaine | **285 796** |
| 9 | `silver.event` : lignes | **138 313** |
| 10 | `ops.quarantine_event` : lignes | **415** |
| 11 | Un même `run_id` couvre au moins 6 tâches distinctes | — |
| 12 | Le `for_each` a produit 3 lignes de journal pour les référentiels | **3** |
| 13 | `ops.job_runs` : schéma exact, au moins une exécution | — |
| 14 | La dernière exécution est en succès | — |

Le critère 4 est le plus instructif : les 287 785 lignes ingérées avant la dérive ont
maintenant deux colonnes de plus, à `null`. Personne n'a réécrit ces lignes — Delta gère
l'ajout de colonnes sans réécriture, et le `null` signifie ici « la source ne fournissait
pas cette information à l'époque », pas « valeur manquante ».

---

## Questions

1. Pourquoi Auto Loader échoue-t-il au lieu d'ignorer les colonnes inconnues ? Quel
   `schemaEvolutionMode` aurait évité l'échec, et qu'aurait-on perdu ?
2. Ton job a rattrapé la dérive tout seul. Est-ce une bonne chose ? Qu'est-ce qui te dit
   que le contenu de `promo_code` est exploitable, et pas juste présent ?
3. La tâche conditionnelle empêche le gold d'être publié si la qualité est mauvaise.
   Mais `gold` s'exécute **avant** `dq_checks` dans ton DAG. Où est le problème, et
   comment le corrigerais-tu ?
4. Quel timeout as-tu retenu au niveau du job, et sur quelle base ?
5. Ton job relance toutes les tâches à chaque exécution, y compris le calcul complet du
   gold. À partir de quel volume ça ne tient plus, et que fais-tu à ce moment-là ?
