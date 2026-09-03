# Index du *Manuel du Data Engineer* — pagination réelle

Relevé le 31 août 2026 sur `Manuel du Data Engineer.pdf` (400 pages). **Le folio imprimé
correspond exactement au numéro de page du PDF** — vérifié sur les pages 7, 100, 207 et 300.
Quand je renvoie à « p. 319 », c'est le chiffre imprimé en tranche.

Source : `H:\kDrive\Carrière\Formation\Data Engineer\Databricks - PySpark\`

---

## Où tu en es

**Page 207 sur 400.** Tu es au milieu du chapitre 11, dans le cas d'entreprise
« quatorze lignes de trop » — une jointure qui duplique. Il te reste **193 pages**.

Ce n'est pas une moitié quelconque. Voici ce qu'elle contient.

| Chapitre | Pages | Section d'examen | Poids |
|---|---|---|---|
| 12 · Agréger et fenêtrer | 211-226 | 3 | 22 % |
| 13 · Le semi-structuré et l'imbriqué | 227-244 | 2, 3 | 21 + 22 % |
| 14 · Historiser | 245-262 | 3, 1 | 22 + 6 % |
| 15 · La modélisation dimensionnelle | 265-280 | 3 | 22 % |
| 16 · Les objets de publication | 281-294 | **3** | 22 % |
| 17 · Qualité, métadonnées, observabilité | 295-310 | 3, 6 | 22 + 10 % |
| 18 · Orchestrer les traitements | 313-328 | **4** | **16 %** |
| 19 · Les pipelines déclaratifs | 329-344 | 4, 3 | 16 + 22 % |
| 20 · Diagnostiquer et optimiser | 345-360 | **6** | **10 %** |
| 21 · Sécuriser, déployer, maîtriser le coût | 361-378 | **7, 5, 1** | **15 + 10 + 6 %** |
| A · Options et réglages | 381-388 | 2, 3 | référence |
| B · SQL ↔ PySpark | 389-394 | 3 | référence |

**Les sections 4, 5, 6 et 7 — soit 51 % de l'examen — sont intégralement dans les pages
que tu n'as pas encore lues**, plus la moitié la plus lourde de la section 3. Les 207
pages déjà lues couvrent les sections 1 et 2, celles où tu es le meilleur.

C'est le fait le plus important de ces deux jours : ton retard de lecture et tes lacunes
d'examen sont **le même retard**. Finir le livre n'est pas un objectif parallèle à la
révision, c'est la révision.

### Ordre de lecture par rentabilité

Si le temps manque, lis dans cet ordre — poids d'examen rapporté au nombre de pages :

**21** (25 % de l'examen en 18 pages) › **18** (16 % en 16) › **20** (10 % en 16) ›
**16** (les quatre objets gold, geste n°6) › **12** › **14** › **19** › **17** › **B** ›
**13** › **15** › **A**

---

## Index détaillé

### Partie I — Fondations

| § | p. | Titre |
|---|---|---|
| **1** | **7** | **Le métier et la chaîne de valeur** |
| 1.1 | 7 | Un métier récent |
| 1.2 | 8 | Pourquoi on n'analyse pas là où l'on enregistre |
| 1.3 | 10 | Trois tentatives |
| 1.4 | 11 | La donnée traverse des couches |
| 1.5 | 13 | Le trajet complet d'une information |
| 1.6 | 14 | Ce qui ne se négocie pas |
| **2** | **21** | **Du fichier à la table : le stockage** |
| 2.1 | 21 | Ranger par ligne ou par colonne |
| 2.2 | 22 | Ce qu'il y a dans le fichier |
| 2.3 | 23 | Un détail qui change tout : la découpabilité |
| 2.4 | 24 | Le journal, ou comment un répertoire devient une table |
| 2.5 | 27 | Le voyage dans le temps, et ce qu'il coûte |
| 2.6 | 30 | Où l'on range les lignes |
| 2.7 | 34 | Le paysage des formats de table |
| **3** | **41** | **Le moteur d'exécution distribué** |
| 3.1 | 41 | Deux rôles qu'il ne faut pas confondre |
| 3.2 | 42 | La partition, unité de tout |
| 3.3 | 44 | Rien ne s'exécute avant qu'on ne le demande |
| 3.4 | 47 | Du code au plan |
| 3.5 | 49 | Le poste de dépense principal |
| 3.6 | 52 | Comment le moteur joint deux tables |
| 3.7 | 54 | Quand le moteur se corrige lui-même |
| 3.8 | 56 | Le débordement, panne silencieuse |
| 3.9 | 57 | Les machines, et ce qu'elles coûtent |
| **4** | **63** | **Le catalogue et les objets gouvernés** |
| 4.1 | 63 | Ce que le catalogue ajoute au stockage |
| 4.2 | 64 | La hiérarchie, et la façon dont les droits s'y propagent |
| 4.3 | 67 | Qui possède les fichiers |
| 4.4 | 70 | Ce qui n'entre pas dans une table |
| 4.5 | 71 | Les objets qui désignent un calcul |
| 4.6 | 71 | Documenter n'est pas une corvée |
| 4.7 | 74 | Savoir qui dépend de quoi |
| 4.8 | 75 | Le catalogue est lui-même une table |
| **5** | **83** | **Écrire du PySpark** |
| 5.1 | 83 | Une seule porte d'entrée |
| 5.2 | 85 | Un DataFrame est une recette, pas un plat |
| 5.3 | 86 | Les cinq façons de désigner une colonne |
| 5.4 | 88 | Les verbes qui font l'essentiel du travail |
| 5.5 | 91 | Regarder ce qu'on vient de fabriquer |
| 5.6 | 94 | Lire un message d'erreur |
| 5.7 | 96 | Trouver soi-même la réponse |

### Partie II — Faire entrer la donnée

| § | p. | Titre |
|---|---|---|
| **6** | **105** | **Sources et contrats de données** |
| 6.1 | 105 | D'où vient l'information |
| 6.2 | 107 | Ce qu'on ne contrôle pas |
| 6.3 | 108 | Le contrat de données |
| 6.4 | 111 | Les trois dérives |
| 6.5 | 113 | Comment la donnée est mise à disposition |
| 6.6 | 114 | Documenter avant d'ingérer |
| **7** | **121** | **Les formats de fichiers** |
| 7.1 | 121 | Le texte délimité, et tout ce qu'il ne dit pas |
| 7.2 | 125 | Que faire d'une ligne mal formée |
| 7.3 | 128 | Le semi-structuré |
| 7.4 | 128 | Les formats colonnaires vus de la lecture |
| 7.5 | 130 | Choisir un format d'échange |
| **8** | **137** | **Bases relationnelles et interfaces applicatives** |
| 8.1 | 137 | Lire une base relationnelle |
| 8.2 | 139 | Qui fait le travail |
| 8.3 | 140 | Le sujet dont on parle rarement |
| 8.4 | 142 | Les interfaces applicatives |
| 8.5 | 145 | Les connecteurs managés |
| 8.6 | 147 | Les identités et les secrets |
| **9** | **155** | **Les motifs d'ingestion** |
| 9.1 | 155 | Tout recharger, ou seulement le nouveau |
| 9.2 | 156 | Le curseur |
| 9.3 | 159 | La capture des changements |
| 9.4 | 160 | Par lots, par micro-lots, en continu |
| 9.5 | 163 | Rejouer sans casser |
| 9.6 | 165 | Recharger le passé |
| 9.7 | 165 | Quand le schéma bouge |

### Partie III — Transformer

| § | p. | Titre |
|---|---|---|
| **10** | **173** | **Nettoyer et typer** |
| 10.1 | 173 | Ce qui se passe quand une conversion échoue |
| 10.2 | 176 | Nettoyer une chaîne de caractères |
| 10.3 | 181 | Les dates, et les fuseaux |
| 10.4 | 184 | Ce que signifie une absence |
| 10.5 | 187 | Écarter sans jeter |
| 10.6 | 188 | Normaliser les référentiels |
| **11** | **195** | **Combiner : jointures et unions** |
| 11.1 | 195 | Ce qu'une jointure produit vraiment |
| 11.2 | 196 | Les formes, et ce que chacune conserve |
| 11.3 | 199 | Vérifier avant de faire confiance |
| 11.4 | 201 | Les clés qui posent problème |
| 11.5 | 203 | Empiler plutôt que rapprocher |
| **12** | **211** | **Agréger et fenêtrer** ← *reprise de lecture* |
| 12.1 | 211 | Réduire |
| 12.2 | 213 | Ce que comptent réellement les fonctions de comptage |
| 12.3 | 216 | Annoter |
| 12.4 | 219 | Ne garder qu'une version |
| 12.5 | 222 | Le coût, et comment le contenir |
| **13** | **227** | **Le semi-structuré et l'imbriqué** |
| 13.1 | 227 | Trois structures, trois usages |
| 13.2 | 229 | Aplatir change le grain |
| 13.3 | 232 | Le piège du tableau vide |
| 13.4 | 235 | Déclarer le schéma, ou le laisser deviner |
| 13.5 | 238 | Jusqu'où aplatir |
| **14** | **245** | **Historiser** |
| 14.1 | 245 | Pourquoi la question se pose |
| 14.2 | 246 | Trois façons de traiter un changement |
| 14.3 | 246 | L'intervalle de validité |
| 14.4 | 249 | Détecter un vrai changement |
| 14.5 | 252 | Appliquer les changements |
| 14.6 | 255 | Interroger une table historisée |
| 14.7 | 256 | Ce que le stockage offre déjà |

### Partie IV — Modéliser et publier

| § | p. | Titre |
|---|---|---|
| **15** | **265** | **La modélisation dimensionnelle** |
| 15.1 | 265 | Pourquoi on dénormalise |
| 15.2 | 265 | Faits et dimensions |
| 15.3 | 266 | Le grain, avant tout le reste |
| 15.4 | 268 | Les clés |
| 15.5 | 272 | Les trois espèces de faits |
| 15.6 | 273 | Les dimensions partagées |
| 15.7 | 274 | La dimension temps |
| **16** | **281** | **Les objets de publication** |
| 16.1 | 281 | Quatre formes, un même contenu |
| 16.2 | 284 | Trois questions pour trancher |
| 16.3 | 285 | Le piège de la définition relative |
| 16.4 | 286 | Ce qu'on promet en publiant |
| 16.5 | 286 | Servir des usages différents |
| 16.6 | 288 | Nommer |
| **17** | **295** | **Qualité, métadonnées, observabilité** |
| 17.1 | 295 | Ce qu'est une métrique de qualité |
| 17.2 | 296 | Six familles de contrôle |
| 17.3 | 299 | Fixer un seuil |
| 17.4 | 300 | Contrôler dans le flux, ou après |
| 17.5 | 302 | Observer une plateforme |
| 17.6 | 305 | À qui parle un contrôle |

### Partie V — Exploiter

| § | p. | Titre |
|---|---|---|
| **18** | **313** | **Orchestrer les traitements** |
| 18.1 | 313 | Le graphe plutôt que la liste |
| 18.2 | 314 | Faire circuler une information |
| 18.3 | 317 | Ce qui déclenche |
| 18.4 | 319 | Le vert qui ne veut rien dire |
| 18.5 | 320 | Reprendre après un incident |
| 18.6 | 323 | Où placer les contrôles |
| **19** | **329** | **Les pipelines déclaratifs** |
| 19.1 | 329 | Décrire un résultat plutôt qu'une procédure |
| 19.2 | 332 | Ce que le moteur produit |
| 19.3 | 334 | La qualité intégrée au flux |
| 19.4 | 337 | Ce qu'on accepte de perdre |
| 19.5 | 340 | Quand choisir l'un ou l'autre |
| **20** | **345** | **Diagnostiquer et optimiser** |
| 20.1 | 345 | Commencer par la bonne question |
| 20.2 | 345 | Chaque question a son outil |
| 20.3 | 349 | Nommer la pathologie |
| 20.4 | 351 | Lire moins |
| 20.5 | 354 | Ranger et entretenir |
| 20.6 | 356 | Toucher aux réglages, prudemment |
| **21** | **361** | **Sécuriser, déployer, maîtriser le coût** |
| 21.1 | 361 | Restreindre au-delà de la table |
| 21.2 | 362 | Gouverner par règle plutôt que par objet |
| 21.3 | 365 | Sortir du navigateur |
| 21.4 | 366 | Décrire l'état souhaité |
| 21.5 | 366 | Un code, plusieurs environnements |
| 21.6 | 370 | Les secrets |
| 21.7 | 370 | Voir la facture avant qu'elle n'arrive |

### Annexes

| § | p. | Titre |
|---|---|---|
| **A** | **381** | **Options et réglages** |
| A.1 | 381 | Lire des fichiers |
| A.2 | 384 | Écrire des fichiers et des tables |
| A.3 | 385 | Bases relationnelles |
| A.4 | 386 | Ingestion incrémentale de fichiers |
| A.5 | 387 | Propriétés d'une table |
| A.6 | 388 | Réglages de session |
| **B** | **389** | **SQL ↔ PySpark** |
| B.1 | 389 | La forme générale |
| B.2 | 390 | Colonnes et conditions |
| B.3 | ~391 | Jointures et ensembles |
| B.4 | ~392 | Agrégats et fenêtres |
| B.5 | ~393 | Chaînes, dates, structures |
| B.6 | ~394 | Objets et écritures |
| — | 395 | **Index des fonctions par usage** |

---

## Correspondance objectif d'examen → chapitre

À utiliser dans les deux sens : après un raté, pour savoir quoi relire ; avant une salve,
pour savoir ce qui va tomber.

| Section d'examen | Poids | Chapitres | Modules NovaMarket |
|---|---|---|---|
| 1 · Plateforme | 6 % | 1, 2 (2.5 *time travel*), 4, 21.7 | M0, M4, M9 |
| 2 · Ingestion | 21 % | 6, 7, 8, 9, A.1, A.4 | M1, M2, M13 |
| 3 · Transformation | 22 % | 5, 10, 11, 12, 13, 14, 15, **16**, 17, B | M3, M4, M5, M6, M7 |
| 4 · Lakeflow Jobs | 16 % | **18**, 19 | M8 |
| 5 · CI/CD | 10 % | **21.3, 21.4, 21.5, 21.6** | M11 |
| 6 · Diagnostic | 10 % | 3 (3.5-3.8), **20**, 17.5 | M12 |
| 7 · Gouvernance | 15 % | 4.2, 4.3, **21.1, 21.2** | M10 |
