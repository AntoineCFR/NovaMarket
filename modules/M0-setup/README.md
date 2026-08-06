# M0 — Mise en place de la plateforme

**Durée estimée** : 45 min · **Prérequis** : un workspace Databricks Free Edition actif

Ce module n'évalue pas ta capacité à écrire du Spark. Il installe le terrain de jeu et
te fait manipuler les objets Unity Catalog dont dépendent tous les modules suivants.

---

## Objectif

Construire l'arborescence Unity Catalog du projet, y téléverser les deux premières
vagues de fichiers, et créer le journal d'exécution qui servira de fil rouge sur la
partie métadonnées.

---

## Étapes

### 1. Créer l'arborescence Unity Catalog

Ouvre `M0_setup.py` dans ton workspace et exécute-le. Il crée :

- le catalog `novamarket`
- les schemas `landing`, `bronze`, `silver`, `gold`, `ops`
- le volume `novamarket.landing.files` et ses trois sous-répertoires
- le volume `novamarket.ops.checkpoints`
- la table `novamarket.ops.pipeline_runs`

Deux cellules contiennent des `TODO` : à toi de les compléter.

### 2. Téléverser les vagues W0 et W1

Deux options.

**Option A — Databricks CLI (recommandée).** Installe la CLI, puis authentifie-toi
avec un jeton d'accès personnel (*Settings → Developer → Access tokens*) :

```bash
databricks configure --host https://<ton-workspace>.cloud.databricks.com
```

Puis, depuis la racine du projet :

```bash
databricks fs cp -r "data/waves/W0_ref/ref" "dbfs:/Volumes/novamarket/landing/files/ref" --overwrite
```

```bash
databricks fs cp -r "data/waves/W1_initial/orders" "dbfs:/Volumes/novamarket/landing/files/orders" --overwrite
```

```bash
databricks fs cp -r "data/waves/W1_initial/events" "dbfs:/Volumes/novamarket/landing/files/events" --overwrite
```

**Option B — interface graphique.** *Catalog → novamarket → landing → files → Upload*,
puis sélectionne le sous-répertoire cible et dépose les fichiers. Plus fastidieux :
21 fichiers pour W1.

> Si l'onglet *Access tokens* n'apparaît pas dans tes paramètres, passe par l'option B.
> Le reste du parcours n'a pas besoin de la CLI.

### 3. Vérifier

Exécute la dernière cellule du notebook : elle liste le contenu du volume et compte
les fichiers par répertoire.

---

## Critères d'acceptation

> **Avant le premier grader** : importe `graders/_grader_lib.py` **dans le même dossier**
> que le grader. Chaque grader commence par `%run ./_grader_lib` et échoue avec
> `Notebook not found` s'il ne le trouve pas à côté de lui. Voir `graders/README.md` —
> le plus simple est d'importer le dossier entier, une fois pour tout le parcours.

Le grader `graders/M0_grader.py` vérifie que :

| # | Critère |
|---|---|
| 1 | Le catalog `novamarket` existe |
| 2 | Les schemas `landing`, `bronze`, `silver`, `gold`, `ops` existent |
| 3 | Le volume `novamarket.landing.files` existe, avec les répertoires `orders/`, `events/`, `ref/` |
| 4 | Le volume `novamarket.ops.checkpoints` existe |
| 5 | `ref/` contient exactement 3 fichiers `.csv` |
| 6 | `orders/` contient exactement 7 fichiers `.csv` |
| 7 | `events/` contient exactement 14 fichiers `.jsonl.gz` |
| 8 | La table `novamarket.ops.pipeline_runs` existe avec le schéma exact attendu |
| 9 | Le catalog `novamarket` porte un commentaire non vide |

---

## Points d'attention

- **Ne décompresse pas** les fichiers `.jsonl.gz`. Auto Loader lit le gzip nativement,
  et c'est un point du module suivant.
- Un volume UC managé s'adresse par le chemin POSIX `/Volumes/<catalog>/<schema>/<volume>/…`
  côté Spark et Python, et par `dbfs:/Volumes/…` côté CLI. Ce sont deux vues du même objet.
- Le volume `ops.checkpoints` doit rester **vide** à la fin de M0. Il se remplira en M1.
