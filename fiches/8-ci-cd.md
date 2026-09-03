# 8 · CI/CD — Git Folders et bundles

Le vocabulaire et les pièges sont dans `exam/fiche-cicd.md`. **Ici, c'est un bundle
complet, de bout en bout.**

> **Noms actuels** : *Declarative Automation Bundles* (ex-*Databricks Asset Bundles*) ·
> *Databricks Git Folders* (ex-*Repos*) · *Lakeflow Jobs* (ex-*Workflows*).
> **La commande CLI, elle, est restée `databricks bundle`.**

---

## L'arborescence

```
ventes/
├── databricks.yml              identité, variables, targets
├── resources/
│   ├── quotidien.job.yml
│   └── ingestion.pipeline.yml
├── src/
│   ├── bronze.py
│   └── silver.py
└── .gitignore
```

## `databricks.yml`

```yaml
bundle:
  name: ventes

include:
  - resources/*.yml

variables:
  catalogue:
    description: Catalogue cible
    default: ventes_dev
  chemin_arrivees:
    description: Répertoire de dépôt des fichiers
    default: /Volumes/ventes_dev/bronze/landing
  coffre:
    description: Nom du scope de secrets
    default: ventes_dev

targets:
  dev:
    mode: development          # préfixe les ressources, SUSPEND les plannings
    default: true
    workspace:
      host: https://adb-xxxx.azuredatabricks.net
    variables:
      catalogue: ventes_dev

  prod:
    mode: production           # noms sans préfixe, plannings actifs
    workspace:
      host: https://adb-xxxx.azuredatabricks.net
    variables:
      catalogue: ventes
      chemin_arrivees: /Volumes/ventes/bronze/landing
      coffre: ventes_prod
```

**`targets` = les environnements.** Jamais les tables cibles d'un pipeline — c'est la
confusion la plus fréquente, et elle est listée comme piège dans le guide.

### Ce que `mode: development` fait, et les deux problèmes qu'il règle

| | `development` | `production` |
|---|---|---|
| Noms des ressources | **préfixés par le déployeur** | tels quels |
| Planifications | **suspendues** | actives |
| Machines | réutilisées | neuves, reprises automatiques |

Le préfixe empêche deux développeurs de s'écraser sur un workspace unique. La suspension
empêche un traitement en cours d'écriture de tourner toutes les nuits sur des données
réelles. **Deux problèmes distincts, une même option** — c'est souvent posé comme tel.

## `resources/quotidien.job.yml`

```yaml
resources:
  jobs:
    quotidien:
      name: ventes_quotidien
      max_concurrent_runs: 1

      parameters:
        - name: catalogue
          default: ${var.catalogue}         # ← la surcharge de target arrive ici
        - name: jour
          default: "{{job.start_time.iso_date}}"

      schedule:
        quartz_cron_expression: "0 30 5 * * ?"
        timezone_id: "Europe/Paris"

      tasks:
        - task_key: bronze
          notebook_task:
            notebook_path: ../src/bronze.py
            base_parameters:
              catalogue: "{{job.parameters.catalogue}}"
              jour: "{{job.parameters.jour}}"
              jeton: "{{secrets/${var.coffre}/api_token}}"   # ← l'emplacement, pas la valeur
          timeout_seconds: 3600
          max_retries: 2

        - task_key: silver
          depends_on: [{ task_key: bronze }]
          notebook_task:
            notebook_path: ../src/silver.py
            base_parameters: { catalogue: "{{job.parameters.catalogue}}" }
          timeout_seconds: 1800

        - task_key: alerte
          depends_on: [{ task_key: silver }]
          run_if: AT_LEAST_ONE_FAILED
          notebook_task: { notebook_path: ../src/alerte.py }
```

---

## Les commandes, dans l'ordre

```bash
databricks auth login --host https://adb-xxxx.azuredatabricks.net

databricks bundle validate -t dev        # résout, vérifie, AFFICHE — ne modifie rien
databricks bundle deploy   -t dev
databricks bundle run quotidien -t dev

databricks bundle deploy   -t prod       # la promotion : le MÊME code
databricks bundle run quotidien -t prod

databricks bundle destroy  -t dev        # supprime ce que le bundle a créé
```

**`validate` est ton meilleur outil de compréhension**, et celui qu'on néglige : il montre
la configuration **finale**, après résolution des variables et application des surcharges.
C'est là qu'on découvre ce qu'on a réellement écrit, par opposition à ce qu'on croyait
écrire. À passer systématiquement avant tout déploiement.

> `bundle remove` et `bundle clean` **n'existent pas**. C'est `destroy`.

---

## Les secrets

```bash
databricks secrets create-scope ventes_prod
databricks secrets put-secret   ventes_prod api_token
```

```yaml
# dans le YAML versionné : l'EMPLACEMENT seulement
jeton: "{{secrets/ventes_prod/api_token}}"
```

```python
# dans un carnet
jeton = dbutils.secrets.get(scope="ventes_prod", key="api_token")
```

**Jamais de mot de passe dans une variable de bundle.** Le YAML est versionné : la valeur
serait lisible par quiconque a accès au dépôt, **et pour toujours**. Un dépôt conserve ce
qu'on en retire ensuite — un secret ayant transité par un historique est **compromis**, il
faut le **remplacer**, pas l'effacer.

Le **nom** du scope et de la clé sont parfaitement publiables ; ce sont même de bonnes
variables de target.

---

## Git Folders

Un dossier du workspace **est** un dépôt Git : *Workspace → Create → Git folder*.

Les quatre gestes de l'objectif d'examen : créer une branche, basculer d'une branche à
l'autre — **le contenu des fichiers change** —, committer et pousser, ouvrir une *pull
request*.

> ⚠️ **Basculer de branche avec des modifications non commitées fait perdre le travail**,
> dans le workspace comme en ligne de commande. Committer ou mettre de côté d'abord.

Les données n'ont rien à faire dans un dépôt Git. Le code, les notebooks et le bundle,
oui.

---

## Prouver la promotion, plutôt que l'affirmer

Un job de fumée qui écrit une ligne dans une table de journal, dans les deux
environnements :

```python
dbutils.widgets.text("catalogue", "")
cat = dbutils.widgets.get("catalogue")

spark.sql(f"""
  INSERT INTO {cat}.ops.deployment_log
  SELECT current_timestamp(), '{cat}', '{{{{bundle.target}}}}', '{{{{job.name}}}}'
""")
```

Après un déploiement sur les deux cibles, la table contient **deux lignes avec deux
catalogs différents et deux noms de job différents**. C'est ce qui prouve que **le même
code** a produit deux déploiements distincts — et c'est toute la définition de la
promotion d'environnement.

Sans ça, « j'ai déployé » n'est qu'une affirmation.

---

## Les cinq pièges

1. **`validate` ne déploie pas.** La confusion coûte du temps dans les deux sens : croire
   qu'on a déployé, ou hésiter à valider par crainte de modifier.
2. **Retirer une ressource du YAML la supprime** au déploiement suivant — avec son
   historique d'exécutions, et pour un pipeline, potentiellement ses tables. Le fichier
   fait autorité.
3. **`deploy` est idempotent.** Deux exécutions sans changement ne produisent **rien** :
   aucun doublon, aucune version empilée.
4. **Une ressource créée à la main est invisible au bundle** : ni gérée, ni supprimée, ni
   reproduite ailleurs. Protection d'un côté, source de dérive de l'autre — c'est ainsi
   que dev et prod divergent sans que rien ne le signale.
5. **Un environnement de test sans données réalistes ne prouve rien.** Un déploiement
   validé sur trois lignes ne dit rien du comportement sur trois cents millions.

### Ce qui disparaît, et ce qui reste

| | Au `deploy` suivant |
|---|---|
| La **ressource** — job, pipeline | **supprimée**, avec son historique |
| Le **fichier source** — notebook, `.py` | **intact** : c'est la charge utile, pas une ressource |
| Les **tables** d'un pipeline déclaratif | le pipeline les possède — à vérifier avant |
