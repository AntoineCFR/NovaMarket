# 📖 Fiche — Diagnostic du compute

**Objectif du guide** : *« Diagnostiquer les échecs de démarrage de cluster, les conflits
de bibliothèques et les problèmes de saturation mémoire. »*

**Pourquoi une fiche** : tout cela suppose de configurer un cluster. En Free Edition, le
compute est exclusivement **serverless** : pas de nœuds à dimensionner, pas de
bibliothèques à installer au niveau du cluster, pas de démarrage à observer. Le sujet
reste au programme, donc il faut le connaître.

---

## Les services de compute et leurs modèles de coût

C'est aussi l'objectif de la section 1. Le tableau qu'il faut savoir refaire de tête :

| Service | Pour quoi | Facturation | Démarrage |
|---|---|---|---|
| **Serverless (notebooks et jobs)** | Développement, ETL, tâches planifiées | À l'usage, sans temps d'inactivité facturé | Quelques secondes |
| **Job compute** (classique) | Charges planifiées, éphémère | DBU job, tarif le plus bas | Minutes |
| **All-purpose compute** | Développement interactif, partagé | DBU all-purpose, **~2 à 4× le tarif job** | Minutes |
| **SQL warehouse (serverless)** | BI, requêtes ad hoc, forte concurrence | À l'usage | Secondes |
| **SQL warehouse (pro / classic)** | Idem, dans ton propre réseau | DBU SQL + infrastructure | Minutes |

**Les deux erreurs classiques d'examen** :

1. Faire tourner un ETL planifié sur un all-purpose cluster. Ça marche, et ça coûte deux
   à quatre fois le prix pour rien. Un job doit tourner sur du job compute ou du
   serverless.
2. Choisir un cluster mono-nœud ou à taille fixe pour des analystes qui lancent des
   requêtes ad hoc en parallèle. La bonne réponse est un SQL warehouse, ou un compute à
   forte concurrence avec mise à l'échelle automatique.

La règle mnémotechnique : **interactif et concurrent → SQL warehouse ; planifié et
éphémère → job compute ; développement → serverless.**

---

## Échecs de démarrage de cluster

| Symptôme | Cause fréquente | Piste |
|---|---|---|
| `INSTANCE_UNREACHABLE`, `BOOTSTRAP_TIMEOUT` | Réseau : sous-réseau saturé, groupe de sécurité, absence de route sortante | Vérifier la configuration réseau du workspace |
| Quota de la région dépassé | Le fournisseur cloud refuse de fournir les instances | Changer de type d'instance, ou demander une hausse de quota |
| Échec d'un script d'initialisation | Un `init script` qui plante fait échouer tout le démarrage | Lire les journaux du script, pas ceux du cluster |
| Type d'instance indisponible | La famille demandée n'existe pas dans la zone | Changer de type ou de zone |

**Le réflexe** : les journaux d'événements du cluster (*Event log*) disent **pourquoi**,
alors que l'interface dit seulement **que** ça a échoué.

---

## Conflits de bibliothèques

L'ordre de résolution, du plus prioritaire au moins prioritaire : notebook → cluster →
runtime. Une bibliothèque installée au niveau du notebook masque celle du cluster, qui
masque celle du runtime.

| Symptôme | Cause |
|---|---|
| `ImportError` sur un paquet pourtant installé | Deux versions présentes ; celle qui gagne n'est pas celle qu'on croit |
| Fonctionne en interactif, échoue en job | Le job utilise un runtime différent, ou n'a pas les bibliothèques du cluster interactif |
| `NoSuchMethodError`, `ClassNotFoundException` en Java/Scala | Conflit de JAR entre une dépendance et le runtime |

**La bonne pratique** : figer les versions, et privilégier les **environnements de
notebook** ou les dépendances déclarées dans le bundle plutôt que les installations au
niveau du cluster — c'est reproductible et versionné.

---

## Saturation mémoire

Trois lieux différents, trois remèdes différents. Confondre les trois est l'erreur la
plus commune.

| Où | Symptôme | Cause typique | Remède |
|---|---|---|---|
| **Driver** | `Driver OOM`, notebook qui meurt | Un `collect()` ou un `toPandas()` sur un gros DataFrame ; trop de partitions à piloter | Ne jamais ramener au driver ; agréger d'abord |
| **Executor** | `Executor lost`, tâches qui échouent puis rejouent | Une partition trop grosse, souvent à cause d'un *skew* | Corriger le déséquilibre, augmenter le nombre de partitions |
| **Disk spill** | Pas d'erreur, juste très lent | La partition ne tient pas en mémoire mais tient sur disque | Plus de partitions, ou moins de données par partition |

Le *spill* n'est pas une erreur : c'est un ralentissement silencieux. C'est pour ça
qu'on le cherche dans les métriques et pas dans les journaux.

**Ordre de diagnostic** : d'abord regarder si c'est le driver ou un executor. Ensuite
seulement se demander s'il faut plus de mémoire — parce que dans 80 % des cas, la réponse
est de corriger le plan ou le déséquilibre, pas d'acheter de la RAM.

---

## Ce que tu peux quand même observer en Free Edition

- Le **query profile** d'une requête, qui remplace le Spark UI pour l'essentiel.
- Les métriques de *spill* et de *shuffle*, visibles au niveau du stage.
- L'historique d'exécution des jobs et ses tendances de durée — c'est l'objet de la
  partie 5 de M12.
- Le fait que certains paramètres Spark soient **refusés ou ignorés** : c'est la
  démonstration concrète de ce qu'est un compute managé.

---

## QCM associés

`exam/qcm-section-6.md` et `exam/qcm-section-1.md`.
