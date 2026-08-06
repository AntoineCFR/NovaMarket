# 🧰 Outillage — M8

*Cette fiche dit **avec quoi**, pas **comment**.*

> **Section 4 de l'examen — 16 %.** Tu y as fait 70 % au diagnostic, avec trois erreurs
> qui racontent toutes la même chose : **tu supposes que la plateforme te protège.** Elle
> ne le fait pas. Garde ça en tête tout le module.

---

## Ce que tu vas faire

Tes notebooks tournent parce que tu cliques dessus. Tu vas les assembler en un job qui
s'exécute seul, dans le bon ordre, avec des paramètres, des reprises sur erreur et une
porte qui se ferme si la qualité n'est pas au rendez-vous.

Et tu ingéreras la vague **W3**, qui fait dériver le schéma. C'est le module où l'option
`cloudFiles.schemaEvolutionMode` cesse d'être théorique.

---

## 1. Paramétrer un notebook

| Outil | Ce qu'il fait |
|---|---|
| `dbutils.widgets.text(nom, defaut, libelle)` | un paramètre saisi |
| `dbutils.widgets.dropdown(...)` · `.combobox(...)` | un paramètre à choix |
| `dbutils.widgets.get(nom)` | le lire |
| `dbutils.widgets.removeAll()` | repartir propre |

Un widget est aussi le point d'entrée d'un **paramètre de tâche** : ce que le job passe
au notebook arrive par là.

## 2. Faire circuler une valeur entre tâches

| Outil | Ce qu'il fait |
|---|---|
| `dbutils.jobs.taskValues.set(key=..., value=...)` | poser une valeur |
| `dbutils.jobs.taskValues.get(taskKey=..., key=..., default=..., debugValue=...)` | la relire en aval |

**C'est le seul mécanisme prévu.** Une variable Python globale ne survit pas d'une tâche à
l'autre : chaque tâche a son propre contexte d'exécution. `debugValue` permet d'exécuter
le notebook à la main hors du job.

## 3. Les références dynamiques

Dans la définition du job, pas dans le code :

| Référence | Ce qu'elle vaut |
|---|---|
| `{{job.id}}` · `{{job.run_id}}` | identifiants d'exécution |
| `{{task.name}}` | nom de la tâche |
| `{{tasks.<tache>.values.<cle>}}` | une valeur de tâche — c'est ce que lit une condition |
| `{{job.start_time.iso_date}}` | la date d'exécution |

## 4. Le graphe

Rien à coder : les dépendances se déclarent dans l'interface, ou en YAML.

| Élément | Ce qu'il fait |
|---|---|
| dépendances `depends_on` | l'ordre |
| tâche **condition** | branche selon une expression |
| tâche **`for_each`** | répète une tâche sur une liste, avec une concurrence |
| types de tâche | notebook · requête SQL · tableau de bord · pipeline · fichier Python |
| `max_retries` · `min_retry_interval_millis` | reprise sur erreur |
| `timeout_seconds` | **par tâche**, plus court que celui du job |
| `max_concurrent_runs` | le garde-fou d'un déclencheur d'arrivée de fichier |

> **Free Edition : 5 tâches concurrentes au total.** La concurrence d'un `for_each` et le
> nombre d'exécutions simultanées se comptent dedans.

## 5. Les déclencheurs

| Déclencheur | Quand le choisir |
|---|---|
| Programmé (cron) | rythme prévisible, tolérance à l'attente |
| Arrivée de fichier | dépôts imprévisibles, latence courte |
| Mise à jour de table | chaîner deux jobs **sans se caler sur l'horloge** |
| Continu | flux permanent |
| Manuel | développement |

## 6. Ce que W3 va exiger

| Outil | Ce qu'il fait |
|---|---|
| `cloudFiles.schemaEvolutionMode` | `addNewColumns` · `rescue` · `none` · `failOnNewColumns` |
| `.option("mergeSchema", "true")` à l'écriture | laisser la table accueillir les nouvelles colonnes |
| `DESCRIBE HISTORY table` | constater ce qui s'est passé |

`addNewColumns` fait **échouer le flux volontairement** : il enregistre le nouveau schéma
puis s'arrête. La relance repart du schéma à jour. D'où la nécessité de nouvelles
tentatives sur la tâche d'ingestion — c'est la question 1 du QCM de la section 4, et tu
l'avais eue juste.

---

## Les trois pièges à ne pas rater

1. **Une tâche sautée n'est pas une tâche échouée.** Un job dont toutes les tâches
   critiques ont été *skipped* se termine **en vert** et n'envoie aucune alerte. Il faut
   une tâche sur la branche `false` qui lève une exception.
2. **Sans timeout, rien n'arrête une tâche bloquée.** Il n'y a pas de valeur par défaut.
   Sur une offre à quota, c'est la journée qui part.
3. **L'ordre du graphe n'est pas une garantie de qualité.** Une porte de contrôle placée
   après la publication ne protège rien.

## Le vocabulaire à retenir

**DAG** · **valeur de tâche** · **tâche condition** · **`for_each`** · **nouvelle
tentative** · **déclencheur d'arrivée de fichier** · **exécutions concurrentes** ·
**évolution de schéma**.

Section 4 — 16 %.
