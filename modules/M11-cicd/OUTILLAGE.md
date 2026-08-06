# 🧰 Outillage — M11

*Cette fiche dit **avec quoi**, pas **comment**.*

> **Section 5 — 10 % de l'examen.** Tu y as fait 75 % au diagnostic, avec trois erreurs
> qui tournent autour d'une seule idée : **déclaratif ≠ impératif**. Un fichier de bundle
> décrit un **état souhaité**, pas une suite d'actions.

---

## Ce que tu vas faire

Sortir ton travail du navigateur. Le job, le pipeline et les notebooks deviennent des
fichiers versionnés, déployables sur deux environnements par une commande — et
supprimables par une autre.

Peu de code. Beaucoup de YAML, et une CLI.

> ⚠️ **Vocabulaire.** Le guide dit **Declarative Automation Bundles** (ex-*Databricks
> Asset Bundles*). La commande CLI est restée `databricks bundle`. Et *Databricks Repos*
> s'appelle désormais **Git Folders**.

---

## 1. La CLI

| Commande | Ce qu'elle fait |
|---|---|
| `databricks bundle init` | démarrer depuis un modèle |
| `databricks bundle validate -t <target>` | résout les variables, vérifie, **affiche** — ne modifie rien |
| `databricks bundle deploy -t <target>` | déploie |
| `databricks bundle run <ressource> -t <target>` | exécute |
| `databricks bundle destroy -t <target>` | supprime ce que le bundle a créé |
| `databricks workspace import-dir <local> <distant>` | téléverser un dossier |
| `databricks fs cp -r <local> <volume>` | téléverser des fichiers dans un volume |
| `databricks auth login --host <url>` | s'authentifier |

`validate` est ton meilleur outil de compréhension : il montre **la configuration finale**,
après résolution des variables et application des surcharges de target. C'est là qu'on
voit ce qu'on a vraiment écrit.

## 2. Le fichier `databricks.yml`

| Bloc | Ce qu'il contient |
|---|---|
| `bundle:` | le nom du bundle |
| `variables:` | les paramètres, avec `default` et `description` |
| `resources:` | jobs, pipelines, schémas… |
| `targets:` | **les environnements** et leurs surcharges |
| `include:` | d'autres fichiers YAML à agréger |

Dans un target : `mode: development` ou `production`, `workspace.host`, et les
surcharges de `variables`.

`mode: development` fait deux choses automatiquement : il **préfixe** les ressources par
l'identité du déployeur, et il **suspend les planifications**. Deux problèmes distincts,
une même option — deux développeurs ne s'écrasent plus, et un job de développement ne
tourne pas toutes les nuits sur les données de production.

## 3. Les secrets

| Outil | Ce qu'il fait |
|---|---|
| `databricks secrets create-scope <nom>` | créer un scope |
| `databricks secrets put-secret <scope> <cle>` | y déposer une valeur |
| `{{secrets/<scope>/<cle>}}` | y faire référence depuis le YAML |

**Jamais de mot de passe dans une variable de bundle** : le YAML est versionné, donc
lisible par quiconque a accès au dépôt, et pour toujours. Le bundle peut en revanche
contenir le **nom** du scope et de la clé — ce sont de bonnes variables de target.

## 4. Git Folders

Rien à installer : le dossier du workspace est un dépôt Git. Créer une branche, committer,
pousser, ouvrir une *pull request*.

> Basculer de branche avec des modifications non commitées **fait perdre le travail**,
> exactement comme en ligne de commande. Committer ou mettre de côté avant de changer.

## 5. Paramétrer un notebook déployé

| Outil | Ce qu'il fait |
|---|---|
| `dbutils.widgets.text(nom, defaut)` · `.get(nom)` | recevoir un paramètre de tâche |
| `dbutils.notebook.entry_point...` | le contexte d'exécution |

---

## Les trois pièges du module

1. **`validate` ne déploie pas.** Il résout et vérifie, sans rien modifier.
2. **Retirer une ressource du YAML la supprime** au déploiement suivant. Le fichier fait
   autorité : il décrit un état souhaité, pas une suite d'actions. C'est ce qui surprend
   le plus, et c'est voulu.
3. **`targets` = les environnements**, pas les tables cibles des pipelines.

Et une propriété à connaître : `deploy` est **idempotent**. Deux exécutions sans
changement laissent le workspace identique — rien n'est dupliqué, aucune version empilée.
Un job créé à la main dans l'interface, lui, est **invisible** au bundle : ni supprimé, ni
importé. C'est autant une protection qu'une source de dérive.

## Le vocabulaire à retenir

**Declarative Automation Bundles** · **target** · **surcharge de variable** ·
**`mode: development`** · **idempotence du déploiement** · **scope de secrets** ·
**Git Folders**.

Section 5 — 10 %.
