# 6 · Orchestration — Lakeflow Jobs

**Le mode de panne propre à l'orchestration** : se terminer en vert sans avoir rien fait.
Tout le reste de cette fiche en découle.

---

## Le graphe, pas la liste

On déclare des **dépendances**, pas un ordre. Le moteur en déduit trois choses : l'ordre,
le parallélisme possible, et surtout **le périmètre exact à relancer** après un échec.

```yaml
resources:
  jobs:
    quotidien:
      name: ventes_quotidien
      max_concurrent_runs: 1          # évite les recouvrements

      parameters:
        - name: jour
          default: "{{job.start_time.iso_date}}"   # ← rejouable

      schedule:
        quartz_cron_expression: "0 30 5 * * ?"
        timezone_id: "Europe/Paris"                # sinon l'heure d'été décale tout

      email_notifications:
        on_failure:   [data@exemple.fr]
        on_duration_warning_threshold_exceeded: [data@exemple.fr]

      tasks:
        - task_key: ingestion
          notebook_task:
            notebook_path: ./notebooks/bronze
            base_parameters: { jour: "{{job.parameters.jour}}" }
          timeout_seconds: 3600       # SANS LUI : rien n'arrête une tâche bloquée
          max_retries: 2              # SI la tâche est rejouable
          min_retry_interval_millis: 60000

        - task_key: controle
          depends_on: [{ task_key: ingestion }]
          notebook_task: { notebook_path: ./notebooks/controle }
          timeout_seconds: 900

        - task_key: publication
          depends_on: [{ task_key: controle }]
          condition_task:
            op: EQUAL_TO
            left: "{{tasks.controle.values.qualite_ok}}"
            right: "true"

        - task_key: alerte
          depends_on: [{ task_key: publication }]
          run_if: AT_LEAST_ONE_FAILED     # ← sinon elle est SAUTÉE, jamais exécutée
          notebook_task: { notebook_path: ./notebooks/alerte }
```

---

## Faire circuler une information

Chaque tâche s'exécute dans son propre contexte : aucune variable ne survit d'une tâche à
l'autre. Deux canaux, et deux seulement.

### Les paramètres — de l'orchestrateur vers la tâche

```python
dbutils.widgets.text("jour", "")
jour = dbutils.widgets.get("jour")        # TOUJOURS une chaîne

if not jour:
    raise ValueError("paramètre « jour » absent")   # bruyant, pas de repli silencieux

from datetime import date
jour = date.fromisoformat(jour)           # convertir soi-même
```

| Substitution | Ce qu'elle donne |
|---|---|
| `{{job.parameters.jour}}` | un paramètre du travail, propagé aux tâches |
| `{{job.start_time.iso_date}}` | **la date de démarrage** — identique après relance |
| `{{job.id}}` · `{{task.run_id}}` | traçabilité |

> **Ne calcule jamais « hier » dans le code.** Un traitement qui lit l'horloge ne sait
> faire qu'aujourd'hui : rattraper trois journées suppose de modifier le code, de
> l'exécuter trois fois, puis de le remettre en état — à sept heures du matin, sous
> pression, en oubliant la dernière étape.

### Les valeurs de tâche — d'une tâche à la suivante

```python
# amont
n = charge.count()
dbutils.jobs.taskValues.set(key="lignes", value=n)

# aval — debugValue permet d'exécuter le carnet SEUL, hors du graphe
n = dbutils.jobs.taskValues.get(taskKey="ingestion", key="lignes",
                                default=0, debugValue=0)
```

Sans `debugValue`, tout carnet employant ce mécanisme devient impossible à déboguer à la
main — et l'on finit par commenter les lignes, puis par oublier de les décommenter.

---

## Le vert qui ne veut rien dire

Un graphe « en succès » ne dit pas que le travail a été fait. Il dit qu'**aucune tâche
exécutée n'a échoué**.

Une tâche **sautée** n'est pas une tâche en échec. Un graphe dont toutes les tâches utiles
ont été sautées se termine **en vert**, et n'alerte personne. Le cas réel : vingt-trois
jours de vert pendant qu'aucun fichier n'arrivait, découvert en revue mensuelle.

**La parade : rendre le silence bruyant.**

```python
# tâche placée sur la branche « fichier absent »
jour = dbutils.widgets.get("jour")
raise RuntimeError(f"Aucun fichier source pour le {jour}. Chaîne interrompue.")
```

### `run_if` — réagir à autre chose qu'un succès

`depends_on` signifie **« après le succès de »**. Une dépendance ne se déclenche jamais
sur un échec, quelle que soit la condition qu'on lui accroche.

| Valeur | La tâche démarre quand |
|---|---|
| `ALL_SUCCESS` | toutes les amonts ont réussi — **le défaut** |
| `AT_LEAST_ONE_FAILED` | au moins une a échoué — **la tâche d'alerte** |
| `ALL_DONE` | toutes sont terminées, quel qu'en soit l'état |
| `NONE_FAILED` | aucune n'a échoué, les sautées étant tolérées |

---

## Les réglages qui font la différence

| Réglage | Ce qu'il gouverne | Le piège |
|---|---|---|
| `timeout_seconds` | quand abandonner | **Aucune valeur par défaut.** Une tâche bloquée l'est indéfiniment, et le calcul se facture |
| `max_retries` | les reprises automatiques | N'a de sens que si la tâche est **rejouable** |
| `max_concurrent_runs` | exécutions simultanées | **1** par défaut. Ne l'augmenter que si chaque exécution porte une **tranche différente** |
| `run_if` | la règle de déclenchement | `ALL_SUCCESS` par défaut |

**Les reprises sur une tâche non idempotente programment des doublons.** Une tâche qui
ajoute sans borne de progression, relancée après une coupure survenue en cours d'écriture,
recharge l'intégralité du lot **en s'ajoutant** à ce qui était déjà écrit. Le cas réel :
trois semaines d'analyse pour des doublons qui n'apparaissaient que le samedi.

**Un délai maximal se règle à deux fois la durée habituelle**, sur chaque tâche, sans
exception.

---

## Les déclencheurs

| Type | Quand le choisir | Ce qu'il impose |
|---|---|---|
| **Programmé** | régularité, prévisibilité | connaître l'heure de disponibilité, et prendre une marge |
| **Arrivée de fichier** | dépôts imprévisibles | **plafonner** les exécutions simultanées, sinon cascade |
| **Mise à jour de table** | chaîner deux traitements | rien — c'est la réponse propre au problème de la marge |
| **Continu** | la latence décide vraiment | des machines allumées en permanence |

> Deux traitements planifiés à 5 h et 6 h, et le premier finit désormais à 6 h 10 : la
> correction structurelle n'est pas d'avancer le premier, c'est un **déclencheur par mise
> à jour de table** sur le second.

---

## Les types de tâche

| Type | Pour quoi |
|---|---|
| Notebook | le cas courant |
| **Fichier Python ou JAR** | une logique packagée dans une bibliothèque versionnée |
| Requête SQL | du SQL pur |
| Tableau de bord | rafraîchir une restitution |
| Pipeline | déclencher un pipeline déclaratif |
| `for_each` | répéter une tâche sur une liste, avec un plafond de concurrence |
| Condition | brancher selon une valeur de tâche |

Et l'appel qu'il vaut mieux éviter :

```python
r = dbutils.notebook.run("./traiter", 600, {"jour": jour})
```

Il **ne se voit pas dans le graphe**, ne se reprend pas indépendamment, et transforme
trois tâches lisibles en une seule tâche opaque. À réserver aux boucles dont le nombre
d'itérations n'est connu qu'à l'exécution.

---

## La porte de contrôle

Le motif qui empêche une donnée fausse d'atteindre les consommateurs.

```python
# tâche « controle », placée ENTRE le calcul et la publication
metriques = executer_controles("staging.faits")
bloquants = [m for m in metriques if m["statut"] == "KO" and m["bloquant"]]

dbutils.jobs.taskValues.set(key="qualite_ok", value=(len(bloquants) == 0))
```

La tâche de publication ne démarre que si `{{tasks.controle.values.qualite_ok}}` vaut
`true` ; la branche opposée porte une tâche qui **échoue explicitement**.

Cela suppose que calcul et publication soient **deux étapes distinctes** : écrire dans une
table temporaire, contrôler, puis basculer. Un contrôle placé **après** la publication ne
protège personne — il documente un incident déjà survenu.

---

## Les trois surveillances à câbler dès la mise en production

1. **L'échec** — évidemment.
2. **Le dépassement de durée** — attrape les blocages.
3. **La présence attendue de l'exécution.** Un traitement qui ne se déclenche plus
   n'échoue jamais, donc n'alerte jamais.

```sql
-- les chaînes qui auraient dû tourner cette nuit et dont aucune exécution n'existe
SELECT a.chaine
FROM   ops.chaines_attendues a
LEFT   ANTI JOIN ops.journal_executions j
       ON j.chaine = a.chaine AND j.debut >= current_date()
WHERE  a.actif;
```
