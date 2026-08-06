# Couverture de la certification Data Engineer Associate

Adossé au guide officiel **version du 4 mai 2026**. 45 questions, 90 minutes, 33 objectifs
répartis en 7 sections.

Ce document est la référence de couverture : chaque objectif du guide y est tracé vers le
module qui le traite. C'est lui qu'il faut relire deux semaines avant l'examen, avec la
version à jour du guide officiel à côté — Databricks le met à jour sans prévenir.

## Légende

| Symbole | Sens |
|---|---|
| 🔧 | Pratiqué en Free Edition |
| 📖 | Traité en fiche de décision + QCM — non reproductible en Free Edition |
| 📦 | Livré |
| 🔨 | À construire |

---

## Section 1 — Databricks Data Intelligence Platform · 6 % · ~3 questions

| Objectif | Où | Type | État |
|---|---|---|---|
| Composants du socle : architecture, Delta Lake, Unity Catalog | M0, M4 (CDF, *time travel*), M9 (`RESTORE`) | 🔧 | 📦 |
| Services de compute : caractéristiques, limites, **modèles de coût**, choix selon la charge | M12 fiche + `docs/01` | 📖 | 🔨 |

---

## Section 2 — Ingestion et chargement · 21 % · ~9 questions

| Objectif | Où | Type | État |
|---|---|---|---|
| Motifs d'ingestion : batch, streaming, incrémental ; fichiers locaux, connecteurs Lakeflow Connect | M1, M2, M13 | 🔧 | 📦 partiel |
| **`COPY INTO`** pour charger incrémentalement depuis le stockage objet | M13 | 🔧 | 🔨 |
| Auto Loader avec application et évolution de schéma (*directory listing* / *file notification*) | M1, M8 ; mode notification en fiche | 🔧 + 📖 | 📦 partiel |
| Configurer Lakeflow Connect sur des sources d'entreprise | M13 | 📖 | 🔨 |
| Clients JDBC/ODBC ou REST en notebook, orchestrés par Lakeflow Jobs | M2 ; REST en fiche (internet restreint en Free Edition) | 🔧 + 📖 | 📦 partiel |
| **Arbitrer** entre Auto Loader, Lakeflow Connect, connecteurs partenaires : volume, fréquence, types, gouvernance | M13 matrice de décision | 📖 | 🔨 |
| Ingérer du semi-structuré et de l'imbriqué (JSON) vers des tables Delta gouvernées | M1, M3 | 🔧 | 📦 |

---

## Section 3 — Transformation et modélisation · 22 % · ~10 questions

C'est la section la plus lourde de l'examen.

| Objectif | Où | Type | État |
|---|---|---|---|
| Nettoyage bronze → silver : nulls, standardisation des types | M3 | 🔧 | 📦 |
| **Jointures** : inner, left, broadcast, clés multiples, cross join, union et union all | M3 complément, M5 | 🔧 | 🔨 |
| Manipulation de colonnes et de lignes : ajout, suppression, **split**, renommage, filtres, **explode** | M3 complément | 🔧 | 📦 partiel |
| Déduplication et **agrégations** : `count`, `approx_count_distinct`, `mean`, `summary` | M3 complément | 🔧 | 📦 partiel |
| **Paramètres de tuning** : `spark.sql.shuffle.partitions`, `spark.default.parallelism`, mémoire executor/driver, `spark.sql.autoBroadcastJoinThreshold` — et remesurer | M12 | 🔧 + 📖 | 🔨 |
| Différencier et construire les objets **gold** : vues matérialisées, vues, tables de streaming, tables | M5 complément, M7 | 🔧 | 📦 partiel |
| Contrôles qualité et règles de validation sur silver et gold | M3, M6, M7 | 🔧 | 📦 |

---

## Section 4 — Lakeflow Jobs · 16 % · ~7 questions

| Objectif | Où | Type | État |
|---|---|---|---|
| Flux de contrôle : nouvelles tentatives, tâches conditionnelles, branchement, boucles | M8 | 🔧 | 📦 |
| **Types de tâches** : notebook, requête SQL, tableau de bord, pipeline — et leurs dépendances dans le DAG | M8 complément | 🔧 | 📦 partiel |
| Planification et **types de déclencheurs** : programmé, arrivée de fichier, mise à jour de table | M8 complément | 🔧 | 📦 partiel |
| Choisir entre déclencheurs temporels et pilotés par la donnée | M8 complément | 🔧 | 🔨 |

---

## Section 5 — CI/CD · 10 % · ~5 questions

Section entièrement à construire. Attention au piège de vocabulaire : le guide dit
**Declarative Automation Bundles**, anciennement *Databricks Asset Bundles*.

| Objectif | Où | Type | État |
|---|---|---|---|
| Workflow de développement dans le workspace : **Git Folders**, branches, commit, push, pull request | M11 | 🔧 | 🔨 |
| Configuration par environnement : **variables et overrides** de bundle, même base de code sur dev / test / prod | M11 | 🔧 | 🔨 |
| Déployer des Declarative Automation Bundles pour packager et promouvoir jobs et pipelines | M11 | 🔧 | 🔨 |
| **CLI Databricks** pour valider, déployer et gérer les bundles dans un CI/CD automatisé | M11 | 🔧 | 🔨 |

> En Free Edition, un seul workspace. La promotion dev → prod se simule avec deux
> `targets` pointant sur deux catalogs ou deux préfixes de schéma. Le mécanisme de
> variables et d'overrides est identique — c'est lui qui est évalué.

---

## Section 6 — Diagnostic, monitoring et optimisation · 10 % · ~5 questions

| Objectif | Où | Type | État |
|---|---|---|---|
| Tendances de performance via l'historique des exécutions de jobs | M12 | 🔧 | 🔨 |
| Interface Lakeflow Jobs : statuts, graphe de tâches, blocages amont, durées, taux d'échec | M8, M12 | 🔧 | 📦 partiel |
| **Goulots d'étranglement** : *data skew*, *shuffling*, *disk spilling* via les métriques de stage du Spark UI | M12 | 🔧 | 🔨 |
| **Liquid Clustering** et **predictive optimization** | M12 | 🔧 | 🔨 |
| Diagnostic : échecs de démarrage de cluster, conflits de bibliothèques, saturation mémoire | M12 fiche | 📖 | 🔨 |

---

## Section 7 — Gouvernance et sécurité · 15 % · ~7 questions

| Objectif | Où | Type | État |
|---|---|---|---|
| Tables **managées vs externes** : créer, modifier, supprimer, convertir | M10 fiche (les tables externes exigent un *external location*, impossible en Free Edition) | 📖 | 🔨 |
| Contrôles d'accès : **`GRANT`, `REVOKE`, `DENY`** aux principaux niveaux de la hiérarchie | M10 | 🔧 | 🔨 |
| **Masquage de colonnes** et **sécurité au niveau des lignes** | M10 | 🔧 | 🔨 |
| Politiques **ABAC** pour le filtrage de lignes et le masquage pilotés par étiquettes | M10 | 🔧 | 🔨 |

> Continuité avec l'existant : les étiquettes `pii` posées en M6 deviennent le critère des
> politiques ABAC de M10. Le travail de gouvernance déjà fait sert directement.

---

## Synthèse

| Section | Poids | Couverture avant adaptation | Après |
|---|---|---|---|
| 1. Plateforme | 6 % | ~60 % | 100 % |
| 2. Ingestion | 21 % | ~60 % | 100 % |
| 3. Transformation | 22 % | ~65 % | 100 % |
| 4. Jobs | 16 % | ~70 % | 100 % |
| 5. CI/CD | 10 % | ~10 % | 100 % |
| 6. Diagnostic | 10 % | ~5 % | 100 % |
| 7. Gouvernance | 15 % | ~30 % | 100 % |
| **Total pondéré** | | **~48 %** | **100 %** |

Sur les 33 objectifs, **27 sont pratiqués** en Free Edition et **6 sont traités en fiche
de décision** faute de pouvoir les reproduire. Ces six-là sont explicitement étiquetés 📖
pour que tu saches ce que tu n'as jamais fait de tes mains — l'examen ne fait pas la
différence, ton futur employeur si.
