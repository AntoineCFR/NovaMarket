# M11 — CI/CD : Git Folders et bundles déclaratifs

**Section 5 de l'examen · 10 % · ~5 questions** — section entièrement absente du parcours
jusqu'ici.

**Durée estimée** : 3 h · **Prérequis** : M8 validé (le job devient une ressource du bundle)

> 🧰 **Commence par `OUTILLAGE.md`** — les bibliothèques et fonctions dont tu auras besoin,
> sans les réponses. Et `docs/07-python-pour-le-parcours.md` si c'est ton premier passage.

---

## Objectif

Tout ce que tu as construit vit dans un workspace, cliqué à la main. Si ce workspace
disparaît demain, il ne reste rien.

Ce module transforme le projet en **code versionné et déployable** : une définition en
YAML, deux environnements, une commande pour promouvoir de l'un à l'autre.

> ⚠️ Piège de vocabulaire. Le guide de mai 2026 dit **Declarative Automation Bundles**.
> Toute IA entraînée avant dira « Databricks Asset Bundles » ou « DAB ». La commande CLI,
> elle, est restée `databricks bundle`. Voir `docs/05-glossaire-renommages.md`.

---

## Partie 1 — Git Folders

*Workspace → Create → Git folder*, connecté à un dépôt (GitHub, GitLab, Azure DevOps).

Les gestes à maîtriser, tous listés dans le guide d'examen :

1. Créer une branche depuis l'interface du workspace.
2. Basculer d'une branche à l'autre et constater que **le contenu des fichiers change**.
3. Committer et pousser depuis le workspace.
4. Ouvrir une pull request — le bouton renvoie vers ton fournisseur Git.

**Trois questions à trancher en le faisant :**

- Que se passe-t-il pour tes **notebooks non commités** quand tu changes de branche ?
- Un Git Folder peut-il contenir des données ? Doit-il ?
- Quelle est la différence entre un Git Folder et le dossier `Workspace` classique, du
  point de vue de la reproductibilité ?

> Si tu n'as pas de dépôt distant, initialise-en un vide sur GitHub. Le parcours entier
> tient en 550 Ko hors données — et les données n'ont **rien** à faire dans un dépôt Git.

---

## Partie 2 — Anatomie d'un bundle

```
bundle/
├── databricks.yml          identité, variables, targets
└── resources/
    ├── novamarket_daily.job.yml     le job de M8
    ├── novamarket_smoke.job.yml     un job de fumée, déployable partout
    └── novamarket_ldp.pipeline.yml  le pipeline de M7
```

Quatre blocs dans `databricks.yml` :

| Bloc | Rôle |
|---|---|
| `bundle` | Nom du bundle. Il préfixe les ressources déployées |
| `variables` | Les valeurs qui changent d'un environnement à l'autre |
| `include` | Les fichiers de ressources à charger |
| `targets` | Un bloc par environnement, avec les surcharges de variables |

### Les deux targets

| Target | Mode | Catalog | Comportement |
|---|---|---|---|
| `dev` | `development` | `novamarket_dev` | Ressources préfixées `[dev <ton nom>]`, planifications suspendues, déploiement isolé par utilisateur |
| `prod` | `production` | `novamarket` | Noms sans préfixe, planifications actives |

Le **mode de déploiement** n'est pas cosmétique : en `development`, Databricks préfixe
automatiquement les noms et met les planifications en pause. C'est ce qui permet à trois
personnes de déployer le même bundle dans le même workspace sans s'écraser — et c'est
précisément ce dont tu as besoin, puisque la Free Edition ne donne qu'un workspace.

### Créer le catalog de développement

```sql
CREATE CATALOG IF NOT EXISTS novamarket_dev
COMMENT 'Environnement de developpement. Meme code, moins de donnees.';
CREATE SCHEMA IF NOT EXISTS novamarket_dev.ops;
```

Un catalog vide suffit : le job de fumée y crée ce dont il a besoin.

---

## Partie 3 — Le job de fumée

Un job qui tourne **dans n'importe quel environnement**, y compris un catalog vide. Une
seule tâche, qui écrit une ligne dans `ops.deployment_log` :

| Colonne | Type |
|---|---|
| `deployed_at` | `timestamp` |
| `bundle_name` | `string` |
| `bundle_target` | `string` — `dev` ou `prod` |
| `job_name` | `string` |
| `catalog_used` | `string` |
| `git_branch` | `string` |

C'est ce qui rend la promotion **vérifiable depuis les données** : après avoir déployé et
lancé sur les deux targets, la table contient deux lignes avec deux catalogs différents et
deux noms de job différents. Sans ça, « j'ai déployé » n'est qu'une affirmation.

Les valeurs viennent de références dynamiques du job, pas de constantes en dur :
`{{bundle.target}}`, `{{bundle.name}}`, `{{job.name}}`.

---

## Partie 4 — La CLI

Les quatre commandes de l'objectif d'examen, dans l'ordre où on les utilise :

```bash
databricks bundle validate -t dev
```

```bash
databricks bundle deploy -t dev
```

```bash
databricks bundle run novamarket_smoke -t dev
```

```bash
databricks bundle deploy -t prod
```

`validate` ne touche à rien : il résout les variables, vérifie la syntaxe et affiche la
configuration finale. **Lance-le et lis sa sortie** — c'est le meilleur moyen de
comprendre ce que les surcharges de target ont réellement produit.

`databricks bundle destroy -t dev` supprime ce qui a été déployé. À connaître, et à
utiliser à la fin du module pour ne pas laisser traîner de ressources.

---

## Critères d'acceptation

Comportementaux : le grader lit `ops.deployment_log` dans les deux catalogs et vérifie
que la promotion a réellement eu lieu. Aucun comptage de données, donc valable quelle que
soit la vague ingérée.

| # | Critère |
|---|---|
| 1 | Le catalog `novamarket_dev` existe |
| 2 | `ops.deployment_log` existe dans les **deux** catalogs, avec le schéma imposé |
| 3 | Une exécution enregistrée avec `bundle_target = 'dev'` |
| 4 | Une exécution enregistrée avec `bundle_target = 'prod'` |
| 5 | Les deux exécutions portent des `catalog_used` **différents** — la surcharge de variable a fonctionné |
| 6 | Les deux exécutions portent des `job_name` **différents** — les modes de déploiement ont fonctionné |
| 7 | Le nom du job de dev contient `dev` |
| 8 | `git_branch` renseigné *(avertissement si tu travailles hors Git Folder)* |
| 9 | Le job `novamarket_daily` de M8 est déclaré dans le bundle |
| 10 | Le pipeline de M7 est déclaré dans le bundle |

Les critères 5 et 6 sont ceux qui comptent : ils prouvent que **le même code** a produit
deux déploiements différents. C'est toute la définition de la promotion d'environnement.

---

## Questions

1. Le mode `development` préfixe les noms et suspend les planifications. Quel problème
   précis chacune de ces deux décisions résout-elle ?
2. Tu as deux targets pour un seul workspace. Qu'est-ce que cette configuration **ne
   teste pas** qu'un vrai dev/prod sur deux workspaces testerait ?
3. Où mets-tu les secrets — jeton d'accès, chaîne de connexion — dans cette architecture ?
   Certainement pas dans le YAML. Décris la chaîne complète.
4. `databricks bundle deploy` est-il idempotent ? Que se passe-t-il si tu le lances deux
   fois de suite sans rien changer ? Et si tu supprimes une ressource du YAML ?
5. Ton dépôt Git contient les notebooks et le bundle. Que contient-il **d'autre** qui ne
   devrait pas y être, dans l'état actuel du projet ?
