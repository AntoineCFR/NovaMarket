# NovaMarket — parcours Data Engineer sur Databricks Free Edition

Projet fil rouge pour valider des compétences de Data Engineer de bout en bout :
ingestion multi-sources → bronze → silver → gold business-ready, qualité de données,
métadonnées, et orchestration Lakeflow Jobs.

Tout est conçu pour tourner **intégralement sur Databricks Free Edition**, sans
compte cloud, sans service payant, sans accès internet sortant.

---

## Le contexte métier

**NovaMarket** est une marketplace généraliste européenne (FR, BE, DE, ES, IT, NL).
Des vendeurs tiers y écoulent leur catalogue ; NovaMarket prélève une commission
dont le taux dépend du plan d'abonnement du vendeur.

Tu reprends la plateforme data. Rien n'existe. On te donne :

- 6 mois d'historique de commandes exporté du backoffice, plus des livraisons quotidiennes
- un flux d'événements applicatifs (clickstream)
- un référentiel catalogue
- une base OLTP applicative (clients, vendeurs)

Ton objectif final : livrer un socle `gold` qui répond à 6 questions métier
(voir `docs/02-sources-et-modele.md`), documenté, testé, et rafraîchi
automatiquement chaque nuit par un job.

---

## Comment ça marche

| Élément | Rôle |
|---|---|
| `generator/` | Génère les datasets. Déterministe : mêmes fichiers à chaque exécution |
| `data/waves/` | Les fichiers à téléverser dans ton Volume, par vague (W0 → W4) |
| `docs/` | Contraintes plateforme, dictionnaire des sources, conventions de nommage |
| `modules/Mx-*/` | Un énoncé, des critères d'acceptation, un notebook starter à trous |
| `modules/Mx-*/OUTILLAGE.md` | **À lire en premier** : les bibliothèques et fonctions du module, sans les réponses |
| `graders/` | Un notebook de validation par module. Il assert sur **tes** tables. Importe le dossier entier — voir `graders/README.md` |
| `solutions/` | Les corrigés. À n'ouvrir qu'après avoir fait valider le module |

**Règle du jeu** : un module est validé quand son grader passe au vert. Le grader ne
regarde jamais ton code, seulement le résultat dans Unity Catalog — libre à toi de
l'obtenir comme tu veux, tant que les critères d'acceptation sont respectés.

Les données arrivent par **vagues**, comme en production. Chaque vague change les
comptages : les critères d'acceptation d'un module décrivent l'état du système **à ce
moment du parcours**, pas un état absolu. Relancer le grader de M1 après avoir ingéré W4
échouera, et c'est normal.

| Vague | Contenu | Arrive au module |
|---|---|---|
| `W0_ref` + `W1_initial` | référentiels, 6 mois d'historique, 14 jours d'événements | M0 |
| `W2` | premier incrémental, avec rejeu partiel | M1 |
| `W3` | incrémental avec **dérive de schéma** | M8 |
| `W4` | incrémental avec **incident de production** | M9 |

> ⚠️ N'ouvre pas `data/waves/W3/` et `data/waves/W4/` avant d'y être. Tu peux, personne
> ne te regarde — mais tu ne t'entraîneras à rien.

---

## Le parcours

| Module | Sujet | Statut |
|---|---|---|
| **M0** | Setup : Unity Catalog, Volumes, téléversement, table d'audit | 📦 livré |
| **M1** | Landing → Bronze avec Auto Loader (`_rescued_data`, `_metadata`, idempotence) | 📦 livré |
| **M2** | Ingestion de la base OLTP (Lakebase Postgres) par watermark | 📦 livré |
| **M3** | Bronze → Silver : typage, parsing, déduplication, quarantaine | 📦 livré |
| **M4** | Historisation : SCD2, MERGE, Change Data Feed | 📦 livré |
| **M5** | Silver → Gold : modèle en étoile, agrégats, vues métier | 📦 livré |
| **M6** | Qualité & métadonnées : analyse des rescues, métriques DQ, tags UC, lineage | 📦 livré |
| **M7** | Pipeline déclaratif Lakeflow : streaming tables, MV, expectations | 📦 livré |
| **M8** | Orchestration Lakeflow Jobs : DAG, paramètres, `for_each`, conditions, retries | 📦 livré |
| **M9** | Capstone : incident de production, détection, quarantaine, rejeu | 📦 livré |

### Les quatre modules de couverture d'examen

Ajoutés pour couvrir les objectifs que le projet fil rouge ne croisait pas.

| Module | Sujet | Section d'examen |
|---|---|---|
| **M10** | Gouvernance : `GRANT`/`REVOKE`/`DENY`, masquage, filtres de lignes, ABAC | 7 — 15 % |
| **M11** | CI/CD : Git Folders, bundles déclaratifs, targets, CLI | 5 — 10 % |
| **M12** | Performance : Spark UI, *skew*, tuning, liquid clustering | 6 — 10 % |
| **M13** | Ingestion : `COPY INTO`, Lakeflow Connect, déclencheurs | complète la 2 |

Plus trois compléments dans les modules existants : jointures et agrégations (M3), les
quatre objets gold (M5), types de tâches et de déclencheurs (M8).

Et sept **fiches de décision** — des sujets qui n'ont pas de bonne réponse unique, où ce
qui compte est de savoir nommer les options et leur coût. `FICHE-source-malformee.md`
(M1) est celle à lire même si tu n'as le temps que pour une : elle part d'un défaut réel
de ce dépôt et assume que le choix retenu n'est pas celui d'une équipe en production.

Compte une quarantaine d'heures de travail effectif pour l'ensemble.

---

## Préparer la certification

Le parcours couvre **100 % des 33 objectifs** du guide *Databricks Certified Data
Engineer Associate* (version du 4 mai 2026) : 27 pratiqués en Free Edition, 6 traités en
fiche de décision faute d'être reproductibles.

| Document | Rôle |
|---|---|
| `docs/04-couverture-certification.md` | Chaque objectif tracé vers son module. À relire deux semaines avant |
| `docs/05-glossaire-renommages.md` | Les noms de produits périmés qu'une IA emploie par réflexe. **À coller à chaque session** |
| `docs/06-protocole-revision.md` | La boucle de révision, les dix gestes, le rythme |
| `exam/` | 80 QCM par section + 2 examens blancs de 45 questions + le journal |

---

## Démarrage

1. Lis `docs/01-contraintes-free-edition.md` — ce que la plateforme autorise et interdit.
2. Lis `docs/02-sources-et-modele.md` — les sources et les questions métier à servir.
3. Lis `docs/07-python-pour-le-parcours.md` — l'inventaire exact des bibliothèques et
   fonctions utilisées ici. Il distingue le **Python ordinaire** (cellules d'exploration
   uniquement) de l'**API DataFrame**, qui n'est pas de la programmation mais un
   vocabulaire déclaratif d'une trentaine de mots.
4. Fais `modules/M0-setup/README.md`.
5. Enchaîne sur `modules/M1-bronze/README.md`, en commençant par son `OUTILLAGE.md`.

**Si ton objectif est la certification** : passe d'abord les sept fiches de QCM sans
réviser, note tes scores dans `exam/journal.md`, et laisse le diagnostic décider de
l'ordre. Le guide officiel est formel sur ce point — repasser du temps sur ce qu'on
maîtrise déjà est l'erreur la plus coûteuse.

### Régénérer les données

```bash
python generator/generate.py --waves W0_ref W1_initial W2 --clean
```

```bash
python generator/generate_oltp.py
```

Python 3.9+ suffit, aucune dépendance externe.

### Recalculer les valeurs attendues par les graders

`generator/reference_stats.py` réimplémente les règles de la couche silver en Python
pur, sans Spark, et écrit `graders/expected/M3.json`. C'est la référence contre laquelle
les graders sont calibrés — et un garde-fou contre mes propres erreurs de comptage.

```bash
python generator/reference_stats.py
```
