# Databricks — parcours Data Engineer

Un dépôt, plusieurs projets fictifs, une progression. Chaque projet est un **cahier des
charges métier** : une entreprise, ses données, ses besoins. La traduction en architecture
est le travail à faire.

> **Certification obtenue le 3 septembre 2026** — Databricks Certified Data Engineer
> Associate, 89 % pondéré. Le détail est dans `Training/journal.md`.

---

## Les projets

| | Projet | Ce qu'on y travaille | État |
|---|---|---|---|
| **01** | *(à venir)* | Ingestion de fichiers, bronze → gold | 🔨 |
| **02** | *(à venir)* | Bases relationnelles et dépôts partenaires | ⏳ |
| **03** | *(à venir)* | Orchestration | ⏳ |
| **04** | *(à venir)* | Pipelines déclaratifs | ⏳ |
| **05** | *(à venir)* | Bundles et promotion d'environnement | ⏳ |
| — | **NovaMarket** | Tout, en même temps, avec les embûches | 🔨 refonte |

Les préfixes numériques donnent l'ordre conseillé. NovaMarket n'en porte pas : il vient
quand tu décides qu'il vient.

## L'anatomie d'un projet

```
0X-nom/
├── README.md      le cahier des charges — le seul document de l'énoncé
├── data/          les fichiers fournis, par vague s'il y en a plusieurs
├── graders/       la vérification, sur le contenu produit
└── solutions/     à n'ouvrir qu'après
```

**Le cahier des charges ne contient aucune indication technique** : ni nom d'outil, ni
extrait de code, ni conseil de mise en œuvre. Le client décrit son métier, ses sources et
ses attentes — y compris ses propres avertissements, quand ils relèvent du métier.

Une section *« ce que nos outils consommeront »* nomme les tables et colonnes que la
restitution lira. C'est un contrat d'interface, et c'est ce que le grader vérifie. Tout le
chemin pour y arriver est libre.

## `Training/` — le matériel commun

| Dossier | Contenu |
|---|---|
| `journal.md` | Le journal, tous projets confondus : sessions, ratés, termes à revoir |
| `certification/` | Guide officiel, couverture des objectifs, examens blancs, drill de syntaxe |
| `fiches/` | Huit fiches pratiques — le geste et le code — plus le carnet imprimable |
| `reference/` | Contraintes Free Edition, conventions, Python pour le parcours |
| `archive/` | Ce qui a été remplacé mais mérite d'être gardé |

Le compagnon principal reste le **Manuel du Data Engineer** — 400 pages, hors dépôt.
`Training/certification/index-manuel.md` en donne la pagination et la correspondance avec
les objectifs d'examen.

---

## Comment travailler

1. **Lire le cahier des charges**, et rien d'autre. Pas de fiche, pas de manuel encore.
2. **Explorer les données fournies** avant d'écrire la moindre ligne de traitement.
3. **Concevoir**, puis construire. Le manuel et les fiches sont là pour ça — d'abord
   avec leur aide, et de moins en moins.
4. **Passer le grader.** Il juge le résultat, jamais la méthode.
5. **Consigner** dans `Training/journal.md` ce qui a résisté.

Le corrigé s'ouvre après, jamais avant.

## Conventions

Trois choses valables partout, détaillées dans `Training/reference/` :

- **Free Edition, serverless uniquement** — aucun choix d'instance, `.cache()` refusé,
  `availableNow` comme seul mode de déclenchement de flux.
- **Le mode ANSI est actif** : un `cast` qui échoue lève. Sur de la donnée brute, jamais
  `cast`, toujours `try_cast`.
- **Les fichiers de `data/` sont volontairement abîmés** et leurs octets comptent.
  `.gitattributes` interdit toute conversion de fin de ligne — ne la contourne pas.
