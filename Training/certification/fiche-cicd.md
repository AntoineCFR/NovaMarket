# Fiche express — CI/CD, Git Folders et bundles

Écrite le 1er septembre 2026. **Section 5 de l'examen : 10 %, ~5 questions.**
C'est ta section la plus faible : 60 % au blanc n°2, 67 % au blanc n°1.

Le manuel traite le sujet au chapitre 21 (21.3 à 21.6), mais de façon conceptuelle : il ne
montre ni la structure d'un bundle, ni les commandes. Cette fiche part de
`modules/M11-cicd/`, qui est concret.

---

## 1. Le vocabulaire, avant tout le reste

C'est le piège n°1 de cette section, parce que les noms ont changé récemment et que
toute IA entraînée avant emploiera les anciens **avec assurance**.

| Ce qu'on dit encore | Le nom actuel |
|---|---|
| Databricks Asset Bundles, DAB | **Declarative Automation Bundles** |
| Databricks Repos | **Databricks Git Folders** |
| Databricks Workflows | **Lakeflow Jobs** |
| Delta Live Tables, DLT | **Lakeflow Declarative Pipelines** |

> **La commande CLI, elle, n'a pas changé : `databricks bundle`.** C'est le produit qui a
> été renommé, pas l'outil. Ne « corrige » jamais du code pour l'aligner sur un nom
> commercial — même chose pour le module Python `dlt`, resté `dlt`.

---

## 2. Git Folders — le travail dans le workspace

Un dossier du workspace **est** un dépôt Git. *Workspace → Create → Git folder*, connecté
à GitHub, GitLab ou Azure DevOps.

Les quatre gestes de l'objectif d'examen :

1. Créer une branche depuis l'interface
2. Basculer d'une branche à l'autre — **le contenu des fichiers change**
3. Committer et pousser depuis le workspace
4. Ouvrir une *pull request* — le bouton renvoie chez ton fournisseur Git

**Le piège qui surprend même les habitués** : basculer de branche avec des modifications
non commitées **fait perdre le travail**, dans le workspace comme en ligne de commande.
Committer ou mettre de côté d'abord.

Ce qu'un Git Folder apporte, et que le dossier `Workspace` classique n'a pas : une branche
pour travailler sans toucher à ce qui tourne, un point de retour avec son explication, une
relecture imposée avant fusion, et l'historique de qui a changé quoi.

**Les données n'ont rien à faire dans un dépôt Git.** Le code, les notebooks et le bundle,
oui.

---

## 3. Anatomie d'un bundle

```
bundle/
├── databricks.yml              identité, variables, targets
└── resources/
    ├── novamarket_daily.job.yml       le job
    ├── novamarket_smoke.job.yml       un job de fumée
    └── novamarket_ldp.pipeline.yml    le pipeline
```

Cinq blocs dans `databricks.yml` :

| Bloc | Ce qu'il contient |
|---|---|
| `bundle:` | Le nom du bundle. Il préfixe les ressources déployées |
| `variables:` | Ce qui change d'un environnement à l'autre, avec `default` et `description` |
| `resources:` | Jobs, pipelines, schémas, modèles, tableaux de bord |
| `targets:` | **Les environnements** et leurs surcharges |
| `include:` | D'autres fichiers YAML à agréger |

```yaml
bundle:
  name: novamarket

variables:
  catalogue:
    description: Catalogue cible
    default: novamarket_dev

resources:
  jobs:
    novamarket_daily:
      schedule:
        quartz_cron_expression: "0 30 5 * * ?"
        timezone_id: "Europe/Paris"

targets:
  dev:
    mode: development        # préfixe les ressources, suspend les plannings
    default: true
    variables:
      catalogue: novamarket_dev
  prod:
    mode: production         # noms sans préfixe, plannings actifs
    variables:
      catalogue: novamarket
```

### `targets` = les environnements

**Jamais les tables cibles d'un pipeline.** La confusion est fréquente et elle est
explicitement listée comme piège dans le manuel (p. 375).

### Les deux modes, et les deux problèmes qu'ils règlent

| `mode: development` | `mode: production` |
|---|---|
| **Préfixe** les ressources par l'identité du déployeur | Noms sans préfixe |
| **Suspend** les planifications | Planifications actives |
| Machines réutilisées d'une exécution à l'autre | Machines neuves, reprises automatiques |

Ce sont **deux problèmes distincts réglés par une même option**, et c'est souvent posé
comme tel :

- Le préfixe empêche deux développeurs de s'écraser mutuellement.
- La suspension empêche un traitement en cours d'écriture de tourner toutes les nuits sur
  des données réelles.

---

## 4. Les commandes, dans l'ordre où on les utilise

| Commande | Ce qu'elle fait |
|---|---|
| `databricks bundle init` | Démarrer depuis un modèle |
| **`databricks bundle validate -t dev`** | **Résout les variables, applique les surcharges, affiche la configuration finale — ne modifie rien** |
| `databricks bundle deploy -t dev` | Applique la description à la cible |
| `databricks bundle run <ressource> -t dev` | Déclenche une exécution |
| `databricks bundle deploy -t prod` | Promeut en production |
| **`databricks bundle destroy -t dev`** | **Supprime ce que le bundle a créé** |
| `databricks auth login --host <url>` | S'authentifier |

`validate` est le meilleur outil de compréhension disponible, et celui qu'on néglige : il
montre **ce qu'on a réellement écrit**, par opposition à ce qu'on croyait écrire. À passer
systématiquement avant tout déploiement.

> `bundle remove` et `bundle clean` **n'existent pas**. C'est `destroy`.

---

## 5. Les secrets

| Outil | Ce qu'il fait |
|---|---|
| `databricks secrets create-scope <nom>` | Créer un coffre |
| `databricks secrets put-secret <scope> <cle>` | Y déposer une valeur |
| `{{secrets/<scope>/<cle>}}` | Y faire référence depuis le YAML |

**Jamais de mot de passe dans une variable de bundle.** Le YAML est versionné : la valeur
serait lisible par quiconque a accès au dépôt, et pour toujours. Un dépôt conserve même ce
qu'on en retire ensuite — un secret ayant transité par un historique est **compromis**, il
faut le **remplacer**, pas l'effacer.

Le **nom** du scope et de la clé, en revanche, sont parfaitement publiables. Ce sont même
de bonnes variables de target.

---

## 6. Les cinq pièges

1. **`validate` ne déploie pas.** La confusion fait perdre du temps dans les deux sens :
   croire qu'on a déployé, ou hésiter à valider par crainte de modifier.
2. **Retirer une ressource du YAML la supprime** au déploiement suivant — avec son
   historique d'exécutions, et pour un pipeline, potentiellement ses tables. Le fichier
   fait autorité.
3. **`deploy` est idempotent.** Deux exécutions sans changement ne produisent **rien** :
   aucun doublon, aucune version empilée, aucun effet de bord.
4. **Une ressource créée à la main est invisible au bundle** : ni gérée, ni supprimée, ni
   reproduite ailleurs. Protection d'un côté, source de dérive de l'autre.
5. **Un environnement de test sans données réalistes ne prouve rien.** Un déploiement
   validé sur trois lignes ne dit rien du comportement sur trois cents millions.

### Ce que le YAML supprime, et ce qu'il ne supprime pas

| | Sort au `deploy` suivant |
|---|---|
| La **ressource** — job, pipeline, l'objet dans la liste | **Supprimée**, avec son historique |
| Le **fichier source** — notebook, `.py` | **Intact**. Il vit dans ton dépôt Git |
| Les **tables** d'un pipeline déclaratif | Le manuel dit « peut supprimer » — à vérifier |

---

## À retenir — six phrases

1. Le produit s'appelle **Declarative Automation Bundles** ; la commande est restée
   `databricks bundle`.
2. **`targets` = les environnements**, jamais les tables cibles.
3. `mode: development` **préfixe** et **suspend les plannings** — deux problèmes, une
   option.
4. **`validate` ne modifie rien** et montre la configuration finale. À passer d'abord.
5. Le YAML décrit un **état souhaité** : ce qui n'y figure plus **disparaît**, et deux
   déploiements identiques ne produisent rien.
6. Les secrets vont dans un **coffre** ; le YAML n'en porte que l'emplacement.
