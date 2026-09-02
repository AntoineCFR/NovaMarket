# Journal de révision

**Examen : jeudi 3 septembre 2026, 9 h 45**

*Recalé le 3 août : départ le 7 au matin.*

| Fenêtre | Dates | Capacité | Objet |
|---|---|---|---|
| **1 — le projet** | 3 → 6 août | 24 h | M1 → M5 : la chaîne `bronze → silver → gold` complète |
| *Absence* | 7 → 24 août | — | Fiches de décision et glossaire, 20 min/jour. Re-télécharger le guide vers le 20 |
| **2 — le reste** | 25 août → 2 sept | 48 h | M6 → M13, puis M10/M11/M12 · les deux blancs · les dix gestes |

> **42 h planifiées sur 48 en fenêtre 2.** La marge a fondu avec les quatre jours perdus
> fin juillet. Si tu prends du retard, ce qui saute est M13 puis le complément *tâches et
> déclencheurs*. Ce qui ne saute jamais : les deux examens blancs et les dix gestes.

Planning détaillé, jour par jour, dans `docs/06-protocole-revision.md`.

> Six heures par jour, dont **quatre de compute au maximum** : au-delà, le quota Free
> Edition coupe le compute jusqu'au lendemain. Garde toujours deux heures de travail hors
> Databricks sous le coude.

C'est ce document que tu reliras la veille — pas les corrigés. Deux réflexes à y
consigner systématiquement :

- **Tout terme inconnu**, même mineur. C'est souvent la seule différence entre deux
  options d'un QCM.
- **Toute affirmation d'une IA que tu n'as pas pu vérifier** dans la documentation
  officielle. Pas de lien, pas de confiance.

---

## Diagnostic initial — 29 et 31 juillet 2026, à froid

Sept fiches, 80 questions, 53 minutes, avant tout module.

| Section | Poids | Score | % | Questions ratées | Priorité |
|---|---|---|---|---|---|
| 1. Plateforme | 6 % | 6 / 10 | 60 % | 2, 4, 5, 8 | 7ᵉ |
| 2. Ingestion | 21 % | 10 / 12 | 83 % | 3, 9 | 4ᵉ |
| 3. Transformation | 22 % | **7 / 12** | **58 %** | 1, 2, 4, 8, 12 | **1ʳᵉ** |
| 4. Lakeflow Jobs | 16 % | 7 / 10 | 70 % | 3, 7, 10 | **3ᵉ** |
| 5. CI/CD | 10 % | 9 / 12 | 75 % | 2, 4, 7 | 5ᵉ |
| 6. Diagnostic | 10 % | **6 / 12** | **50 %** | 2, 3, 4, 5, 7, 8 | **2ᵉ** |
| 7. Gouvernance | 15 % | 10 / 12 | 83 % | 8, 10 | 6ᵉ |

**Total : 55 / 80 = 69 %. Pondéré par les coefficients d'examen : 70 %.**

Ressaisie vérifiée : les sept lignes de synthèse correspondent exactement aux réponses
question par question. Aucune erreur de recopie.

**Priorité = poids × faiblesse.** La section 3 seule vaut deux fois la section 6, et
quatre fois la section 1 — perdre 40 % d'une section à 6 % coûte moins qu'en perdre 17 %
d'une section à 22 %.

### Ce chiffre de 70 % est surévalué. Voici de combien.

Dans la version initiale des fiches, la bonne réponse était en **B** dans 67 cas sur 80.
Un candidat répondant « B » sans lire aurait obtenu **67 / 80, soit 84 %** — mieux que le
score réel.

| Réponses données | Nombre | Justes |
|---|---|---|
| « B » | 50 | 47 — **94 %** |
| autre chose que « B » | 30 | 8 — **27 %** |

27 %, c'est le hasard sur quatre options. Cette asymétrie ne prouve pas que les 47 bonnes
réponses en B étaient devinées — elles sont souvent la formulation nuancée, et la
formulation nuancée est souvent juste. Mais elle interdit de traiter 70 % comme une
estimation de niveau : **à positions rebrassées, le même état de connaissance donnerait
nettement moins.**

C'est la réponse à la question posée — « un bon résultat cache-t-il de la chance ? ». Ici,
oui, et c'est mesurable. Les deux examens blancs ont donc été rebrassés (voir
`exam/README.md`) : ils sont, eux, exploitables comme mesure.

**Rythme** : 40 secondes par question contre 120 disponibles à l'examen. Le temps ne sera
pas la contrainte — mais répondre à l'instinct est précisément le mode qu'un biais de
position récompense. Le 25 août, sur le blanc n°1 : prendre les deux minutes.

### Les sept causes derrière les 25 erreurs

Les erreurs ne sont pas dispersées. Elles se rangent en sept familles — c'est ce qu'il
faut travailler, pas les questions une par une.

| # | Cause | Où | Module qui la traite |
|---|---|---|---|
| 1 | **PySpark n'est pas du SQL.** `unionAll` est un alias de `union` et aucun ne déduplique ; `union` aligne par **position** (d'où `unionByName`) ; `summary()` ≠ `describe()` ; `autoBroadcastJoinThreshold = -1` **désactive** la diffusion | S3 · Q1, Q2, Q8, Q12 | M3 |
| 2 | **Driver et exécuteurs, et où se lit quoi.** `collect()` sature le **driver** ; le *spill* se lit dans les métriques de stage et **ne lève aucune erreur** ; le gain d'un regroupement se mesure en **fichiers lus**, pas en secondes | S6 · Q2, Q3, Q4, Q7 | M12 |
| 3 | **La plateforme n'a aucun garde-fou par défaut.** Une tâche sautée n'est pas une tâche échouée : le job finit **en vert** ; sans timeout, rien n'arrête une tâche bloquée | S4 · Q3, Q10 | M8 |
| 4 | **Le vocabulaire récent.** Liquid clustering (`CLUSTER BY`), *predictive optimization*, SQL warehouse serverless, serverless = aucun choix d'instance | S6 · Q5, Q8 · S1 · Q2, Q8 | M12, M10, M0 |
| 5 | **L'état de l'ingestion incrémentale.** `TRUNCATE` ne remet pas l'historique des fichiers de `COPY INTO` ; un watermark en `>` strict perd la ligne de bordure, définitivement et sans bruit | S2 · Q3, Q9 | M13, M2 |
| 6 | **Déclaratif ≠ impératif.** `validate` ne modifie rien ; retirer une ressource du YAML la **supprime** au déploiement suivant ; `targets` = environnements | S5 · Q2, Q4, Q7 | M11 |
| 7 | **Les restrictions d'Unity Catalog.** La hiérarchie exacte est `metastore → catalog → schema → table` ; ni masque ni filtre sur une **vue** ; `MERGE` cesse d'être supporté sur une table à politique | S1 · Q4 · S7 · Q8, Q10 | M0, M10 |

Une lecture d'ensemble : **la 3 est la seule famille de raisonnement**, les six autres sont
des trous de connaissance. Un trou se comble en le lisant une fois ; un réflexe faux —
lire du PySpark comme du SQL — se corrige en écrivant du code et en se faisant contredire
par le résultat. C'est M3, et c'est aussi la section la plus lourde de l'examen.

Deux ratés isolés méritent une note à part, parce qu'ils ne relèvent d'aucune famille :

- **S3 Q4** — jointure gauche, deux lignes à droite pour **une** clé sur 1 000 : la réponse
  est « plus de 1 000 », pas « 2 000 ». Une seule ligne de gauche est dupliquée. Lecture
  trop rapide de l'énoncé, pas erreur de concept.
- **S1 Q5** — `UNDO LAST WRITE` a été préféré à `RESTORE TABLE ... TO VERSION AS OF`. La
  commande choisie **n'existe pas**. Réflexe à installer : devant quatre options dont une
  seule est une vraie commande, éliminer les inventées avant de raisonner.

---

## Les dix gestes

Coche quand tu sais le faire **sans hésiter ni chercher**.

- [ ] 1. Créer catalog, schema, volume, et téléverser par la CLI
- [ ] 2. Auto Loader avec sauvetage et évolution de schéma
- [ ] 3. `COPY INTO` sur le même fichier, et savoir dire lequel choisir
- [ ] 4. Silver typé avec quarantaine explicite
- [ ] 5. Les six formes de jointure, et laquelle déclenche un *broadcast*
- [ ] 6. Vue, vue matérialisée, table de streaming, et leurs rafraîchissements
- [ ] 7. Job à quatre tâches avec retry, condition et valeur de tâche
- [ ] 8. Bundle déployé sur deux `targets` avec variables et overrides
- [ ] 9. Ouvrir un Spark UI et nommer le goulot
- [ ] 10. Masque de colonne et filtre de lignes, vérifiés **dans les deux sens**

---

## Suivi des modules

Une ligne par module. `Prévu` vient du README du module ; remplis `Réel` à la fin de
chaque session — c'est l'écart entre les deux qui dira, au retour de vacances, si la
fenêtre 2 tient encore.

| Module | Sujet | Prévu | Clos le | Réel | Grader | Notes |
|---|---|---|---|---|---|---|
| **Diagnostic** | 7 fiches de QCM, à froid | 3 h 30 | **31 juil 2026, 8 h 16** | 53 min | 55/80 — 70 % pondéré | Score surévalué : biais de position, voir ci-dessus |
| **M0** | Setup Unity Catalog, volumes, table d'audit | 45 min | **31 juil 2026, ~12 h 15** | 45-60 min | **9/9 · 0 warn · validé** | `_grader_lib` à importer à côté du grader — doc corrigée depuis |
| ~~**M1 · V1**~~ | ~~Landing → Bronze, Auto Loader~~ | ~~2 h 30~~ | **31 juil 2026, 16 h 30** | ~2 h 30 | *jamais passé* | **Abandonné** — voir notes |
| **M1 · V2** | Landing → Bronze, Auto Loader **+ passe de réparation** | 3 h 15 | **4 août 2026, 8 h 24** | **~6 h 35** | **34/35** | Repris de zéro — voir notes |
| **M2** | Ingestion OLTP par watermark | 2 h | **4 août 2026, 12 h 11** | **3 h 33** | **validé** | Voie A (Lakebase) — voir notes |
| **M3** | Bronze → Silver, typage et quarantaine | 4 h 15 | | | | Section 3 = 1ʳᵉ priorité du diagnostic · le raccord des adresses y tend un piège de jointure |
| ↳ *Complément* | Jointures et agrégations | 1 h 30 | | | *(pas de grader)* | Remonté ici depuis la fin de fenêtre |
| **M4** | Historisation SCD2, MERGE, CDF | 4 h | | | | |
| **M5** | Silver → Gold, modèle en étoile | 4 h 30 | | | | |
| ↳ *Complément* | Les quatre objets gold | 30 min | | | *(fiche)* | |
| **M6** | Qualité, métadonnées, lineage | 3 h 30 | | | | |
| **M7** | Pipeline déclaratif Lakeflow | 3 h | | | | |
| **M8** | Orchestration Lakeflow Jobs | 4 h | | | | |
| ↳ *Complément* | Types de tâches et déclencheurs | 30 min | | | *(fiche)* | |
| **M9** | Capstone — incident de production | 4 h | | | | |
| **M13** | `COPY INTO`, Lakeflow Connect, déclencheurs | 2 h 30 | | | | |
| — | *— absence —* | | | | | |
| **M10** | Gouvernance, masquage, filtres, ABAC | 3 h | | | | Section 7 déjà à 83 % — moins prioritaire que prévu |
| **M11** | CI/CD, Git Folders, bundles | 3 h | | | | |
| **M12** | Performance, Spark UI, clustering | 3 h 30 | | | | Section 6 = 2ᵉ priorité, la plus faible à 50 % |

**Total prévu : 45 h 15** de modules, plus 2 h 30 de compléments.

### Notes par module

#### M1 · V1 — abandonné le 31 juillet 2026 à 16 h 30

L'énoncé demandait de retrouver ~1 087 lignes défectueuses dans `_rescued_data`. Test
décisif exécuté à 16 h 05 sur `orders_2026-05.csv` (181 lignes défectueuses, schéma
explicite, `rescuedDataColumn` posé) : **0 ligne sauvée**. Le lecteur CSV ne récupère pas
les champs excédentaires, il les **tronque**. L'attendu venait d'une réimplémentation
Python jamais confrontée à Spark.

Premier correctif : faire constater la perte par le grader. **Écarté** — bronze promet
que rien ne se perd, et documenter le manquement n'est pas le réparer.

Décision retenue : un **second flux de réparation** relit la ligne brute et reconstitue
les adresses dans `bronze.orders_address_repair`, que M3 recolle par `coalesce`. Le flux
principal ne bouge pas — lui figer un schéma tuerait l'évolution de schéma que W3 doit
faire jouer.

Module repris **de zéro**, volontairement, comme entraînement.

**Ce que ça a changé au fond** : la leçon d'origine (« la colonne de sauvetage rattrape
les lignes malformées ») était fausse *et* rassurante. La nouvelle tient en une phrase —
**un compte de lignes juste ne prouve rien sur le contenu des lignes** — et elle se
termine par une réparation, pas par un constat.

**Prolongement, 16 h 55.** Question posée en retour : deux tables bronze pour une source,
est-ce ce que ferait une vraie équipe ? Non — c'est la 4ᵉ option sur 4. Les trois autres :
faire corriger la source, parser correctement du premier coup (`split(ligne, ";", 14)`,
possible **parce que** le champ abîmé est le dernier), normaliser avant d'ingérer. Le
choix retenu est **pédagogique** : l'option 2 supprimerait `schemaEvolutionMode` du
programme, qui est un objectif d'examen. D'où `modules/M1-bronze/FICHE-source-malformee.md`
et la question d'analyse n°7. À relire pendant l'absence : c'est du raisonnement
d'architecture, le genre qu'un entretien creuse plus qu'une syntaxe.

#### M1 · V2 — clos le 4 août 2026 à 8 h 24

**~6 h 35 réelles sur 3 h 15 prévues.** Le dépassement tient à deux attendus faux de ma
part (`_rescued_data` sur CSV, puis sur JSON) et à la reprise complète du module — pas au
niveau de difficulté. Grader : **34/35**, l'unique KO étant lui aussi un critère faux de
ma part, corrigé depuis.

#### M2 — clos le 4 août 2026 à 12 h 11

**3 h 33 réelles sur 2 h prévues.** Le dépassement est entièrement de la plomberie : voie
Lakebase (création de l'instance, `psql` à installer, chemin d'interface faux dans mon
README) plus la contrainte `.cache()` découverte en route. Le module lui-même n'a pas
résisté — le piège de bordure `>` / `>=` a été **vécu, pas récité**.

**Rectification du 4 août, ~15 h.** Le corrigé calculait le nouveau watermark par une
troisième action sur `delta`, donc une troisième lecture de la source. Sur un Postgres
vivant, le watermark aurait pu dépasser ce qui venait d'être écrit. Corrigé : lecture en
retour de la cible filtrée sur `_ingest_batch_id`. **Le code validé le 4 août au matin
porte encore l'ancienne version** — sans conséquence ici, mais à savoir.

---

## Journal des sessions

**Ce qui compte ici, et rien d'autre : le temps passé sur les exercices des modules**,
délimité par les annonces explicites (« j'attaque », « je fais une pause », « je
reprends », « j'en reste là »). Les révisions, les échanges de mise au point et toute la
formation menée en dehors de ce parcours n'y figurent pas — ce n'est pas ce que ce
tableau mesure.

`Début` et `Fin` bornent la session ; `Actif` est le temps réellement travaillé, pauses
déduites. Les marqueurs du 31 juillet sont reconstitués à partir des dates de
modification des fichiers — ils n'avaient pas été pris sur le moment.

| Date | Début | Fin | Actif | Travaillé |
|---|---|---|---|---|
| 29 juil | 13 h 16 | 17 h 25 | 43 min | Diagnostic, fiches 1 à 6 |
| 31 juil | 08 h 11 | 08 h 16 | 5 min | Diagnostic, fiche 7 |
| 31 juil | ~08 h 30 | ~12 h 15 | *n. c.* (45–60 min) | M0 |
| 31 juil | ~13 h | 16 h 30 | ~2 h 30 | M1 V1 |
| **3 août** | **10 h 20** | **12 h 34** | **2 h 14** | M1 V2 |
| **3 août** | **13 h 43** | **17 h 09** | **3 h 26** | M1 V2, suite — arrêt après `M1_bronze_events` |
| **4 août** | **~7 h 30** *(déclaré a posteriori)* | **8 h 24** | **~55 min** | M1 V2 — `ref`, W2, graders |
| **4 août** | **8 h 38** | **12 h 11** | **3 h 33** | M2 |
| **5 août** | **9 h 51** | **13 h 15** *(pause, déclarée a posteriori)* | **3 h 24** | M3 — déduplication |
| **5 août** | **13 h 55** | **14 h 03** *(pause)* | **8 min** | M3, suite |
| **5 août** | **14 h 30** | **16 h 13** *(pause)* | **1 h 43** | M3, suite — nettoyage, typage, quarantaine |
| *— absence, puis reprise tardive —* | | | | |
| **31 août** | **9 h 19** | **9 h 38** | **19 min** | Blanc n°1 — 36/45 |
| **1er sept** | **7 h 55** | **8 h 21** | **26 min** | Blanc n°2 — 34/45 |
| **1er sept** | **18 h 37** | **18 h 56** | **19 min** | Blanc n°3 — 43/45 |

### Total par jour

| Jour | Sessions | Actif |
|---|---|---|
| 29 juil | 1 | 43 min |
| 31 juil | 3 | ~3 h 25 |
| 3 août | 2 | **5 h 40** |
| 4 août | 2 | **4 h 28** |
| 5 août | 3 | **5 h 15** |
| 31 août | 1 | **19 min** *(journée en cours)* |
| | | |
| **Depuis lundi 3 août** | | **10 h 07** |
| **Cumul depuis le 29 juillet** | | **~14 h 35** |

---

## Fenêtre 2 — consolidation

| Étape | Prévu | Fait le | Réel | Résultat |
|---|---|---|---|---|
| Remise en main au retour | 2 h | | | — |
| **Blanc n°1** chronométré | 1 h 30 | **31 août** | **19 min** | *voir « Examens blancs »* |
| Réparation blanc n°1 | 3 h | | | — |
| Les dix gestes, sans notes | 6 h | | | … / 10 |
| **Blanc n°2** à froid | 1 h 30 | | | *voir « Examens blancs »* |
| Réparation blanc n°2 | 3 h | | | — |

---

## Ce que chaque session m'a appris

Le grain fin : ni du temps, ni du module, mais la connaissance elle-même. Une ligne chaque
fois qu'une session révèle une lacune ou renverse une certitude.

| Date | Objectif travaillé | Ce que je n'ai pas su | Vérifié où |
|---|---|---|---|
| 4 août | Watermark d'ingestion (S2), révision post-M2 : 6/7 | Je croyais que le `>=` **fermait** la bordure. Il ne ferme que le cas d'égalité : une transaction validée après ma lecture mais horodatée *strictement avant* le watermark reste invisible pour toujours. `now()` en Postgres renvoie l'heure de **début de transaction**, ce qui rend le cas courant | Corrigé M2 + à confirmer sur docs.databricks.com |
| 4 août | Évaluation paresseuse (S3) | Je pensais qu'un DataFrame filtré était « extrait une fois ». **C'est un plan, pas de la donnée** : `count()`, `write` et `agg()` le rejouent chacun depuis la source. Sur un Postgres vivant, trois actions = trois instantanés, et le watermark peut dépasser ce qui a été écrit | Corrigé M2 rectifié le 4 août · à revoir en M3 |
| 31 août | Blanc n°1, 36/45 | **Six ratés sur neuf sont dans des chapitres du manuel non lus, quatre dans le seul chapitre 21.** Les trois autres sont dans des chapitres **déjà lus** : où vit l'état de `COPY INTO` (9.5, deux questions) et `USE SCHEMA` ≠ `CREATE TABLE` (4.2). Les sections 5 et 7, les deux scores les plus suspects du diagnostic, sont bien celles qui s'effondrent : 75 %→67 % et 83 %→62 % | `exam/mock-exam-1.md` annoté |
| 31 août | Salve A — chapitre 21 lu, puis 12 questions : **11/12**, contre-salve **3/3** | **La cause n°7 du diagnostic — les restrictions d'Unity Catalog — m'a coûté deux points dans la même journée** : Q28 du blanc n°1 (traversée `USE` confondue avec action `CREATE`) puis Q10 de la salve. Une table portant un masque ou un filtre **n'accepte plus le `MERGE`** : ce n'est pas un résultat faux, c'est un refus, et la chaîne SCD2 s'arrête la nuit suivante. J'avais raisonné « les politiques s'évaluent à la lecture, donc une écriture n'est pas concernée » — vrai pour une vue, faux ici. Fermée à la contre-salve | Manuel **21.2, p. 363** et *Limites et pièges*, p. 376 |
| 31 août | Salve B — chapitre 20 lu, puis 11 questions : **9/11** | **À REVOIR — raté deux fois de suite, sous deux formulations différentes.** Devant un échec où *aucune application n'apparaît*, j'ai désigné l'onglet des étapes, puis le plan d'exécution. Ces deux objets sont **produits par le pilote** : sans application, il n'existe ni plan ni étape à consulter. La chaîne monte en cinq couches — démarrage machine, bibliothèques, application, plan, étapes — et les deux premières se lisent **uniquement dans le journal d'événements du cluster**. La bonne question devant un échec n'est pas « qu'est-ce qui cloche dans mon code » mais « **jusqu'où la chaîne est-elle montée** » | Manuel **tableau 20.1, p. 346** |
| 31 août | Salve C — chapitre 18 lu, puis 12 questions : **11/12** | Pour alerter sur un **échec**, j'ai proposé `depends_on` + condition sur une valeur de tâche. Impossible : une tâche qui échoue **ne publie aucune valeur**, la condition n'a rien à évaluer, et la tâche de notification est **sautée** — je reproduisais le graphe vert que je venais d'identifier correctement deux questions plus tôt. `depends_on` signifie « **après le succès de** » : pour réagir à un échec il faut changer la règle de déclenchement, pas ajouter un test. `run_if` : `ALL_SUCCESS` (défaut) · `AT_LEAST_ONE_FAILED` · `ALL_DONE` · `NONE_FAILED`. Fermé en volée 3 | Manuel **p. 322** |
| 31 août | Salve D — chapitres 19 et 16 lus, puis 12 questions : **12/12** | Première salve sans raté. Acquis dans la foulée : l'arbitrage vue / vue matérialisée se tranche sur **coût × fréquence**, jamais sur le coût seul ; une **table de flux est aveugle aux corrections rétroactives** — c'est le critère de choix, pas la nature « qui ne fait que grandir » ; une définition matérialisée ne doit contenir **aucune référence au présent** ; dans un pipeline déclaratif, `spark.read.table` **ne crée pas de dépendance** et donne un résultat faux à la première exécution ; un **rafraîchissement complet efface l'historique d'une SCD2**. **Réserve** : ces questions portaient sur des chapitres lus dans l'heure. Le vrai test reste le blanc n°2, à froid | Manuel ch. 16 et 19 |
| 31 août | Salve E — chapitre 14 lu, puis 12 questions : **12/12** | Deuxième salve parfaite. Acquis : l'**intervalle semi-ouvert** (début inclus, fin exclue) garantit une ligne et une seule à toute date — fermer à la date du changement crée un doublon d'une journée ; joindre une dimension historisée **sans condition de période** multiplie les faits, et **pas uniformément** (les entités qui changent le plus sont les plus dupliquées) ; `concat_ws` **ignore les absences**, d'où le `coalesce` obligatoire avant toute empreinte ; un `MERGE` **échoue** si la source contient deux fois la clé ; le CDF **ne vaut que pour l'avenir** ; le versionnement de table répond au niveau de la **table**, jamais de l'**entité** | Manuel ch. 14 |
| 31 août | Salve F — **mêlée**, 15 questions, 7 sections, sujets non annoncés : **13/15** | Les deux ratés ont la **même racine** : j'ai supposé l'outil accommodant là où il est strict. `unionByName` sans `allowMissingColumns=True` **lève** au lieu de compléter par des `NULL`. Une ligne JSON syntaxiquement invalide n'est **pas** ignorée : elle va dans `_corrupt_record`. La leçon ANSI a été transférée au `cast` mais pas au-delà — **la règle générale est : sur une API Spark, ne jamais supposer le comportement conciliant**. Points fermés en revanche : journal d'événements du cluster (raté 2 fois, juste à la 3ᵉ), `run_if`, `RESTORE ... TO VERSION AS OF` (raté au diagnostic de juillet), bordure de watermark, `row_number` | `exam/qcm-section-3.md`, à confirmer sur docs.databricks.com |
| **1er sept** | **Blanc n°2 à froid — 34/45, 76 % pondéré, contre 82 % la veille** | **Le résultat le plus utile de toute la préparation.** Cinq des onze ratés portent sur des chapitres lus **la veille** (Q19, Q21, Q27, Q34, Q40), et **trois avaient été répondus juste en salve le jour même** : Q27 deux fois, Q34, Q40. Les salves mesuraient la compréhension **à chaud, sur un chapitre lu dans l'heure, sujet annoncé**. Ce blanc mesure la rétention **à froid, sujets mêlés, dix-huit heures après**. Ce ne sont pas la même chose, et l'écart vaut six points. **Conséquence : lire un chapitre de plus rapporte moins que repasser ceux d'hier.** | `exam/mock-exam-2.md` annoté |
| 1er sept | Salve ingestion — `exam/fiche-ingestion.md` : **11/12** | Le déblocage est venu d'une **erreur de présentation de ma part** : ma fiche listait Structured Streaming, Auto Loader et Lakeflow Connect côte à côte comme s'ils étaient de même rang. Ils sont à **trois niveaux** : le moteur (Structured Streaming), la source (`.format("cloudFiles")` = Auto Loader, `.format("kafka")`), le service managé (Lakeflow Connect). **Auto Loader *est* du Structured Streaming.** `COPY INTO` est le seul intrus : commande SQL, pas de checkpoint. Fiche corrigée. Raté restant : le défaut de `schemaEvolutionMode` est `addNewColumns` **sans** schéma explicite, `none` **avec** — et sous `addNewColumns` l'échec du flux est **volontaire** | `exam/fiche-ingestion.md` |
| 1er sept | Salve CI/CD — `exam/fiche-cicd.md` : **12/12** | Section la plus faible des deux blancs (60 % et 67 %), traitée en partant du **dépôt** (`modules/M11-cicd/`) et non du manuel, qui est conceptuel et ne montre ni `destroy`, ni la structure d'un bundle, ni les commandes. C'est le trou de méthode du 31 août : le livre seul ne suffit pas pour la section 5 | `exam/fiche-cicd.md` |
| 1er sept | Salve optimisation — `exam/fiche-optimisation.md` : **11/12** | **Le déblocage : tout se ramène aux statistiques min-max par fichier.** Ranger, c'est resserrer ces intervalles ; il n'y a **pas d'index** au sens relationnel. Raté : j'ai cru que la **forte cardinalité** condamnait *toute* technique de rangement. Elle ne condamne que le **partitionnement**, qui crée un répertoire par valeur — quatre millions de clients, quatre millions de répertoires, donc le problème des petits fichiers. `CLUSTER BY` ne crée aucun répertoire : il ordonne les lignes **dans** les fichiers. **La forte cardinalité est la raison pour laquelle le liquid clustering existe.** Et la Q34 du blanc n°2 (mesurer en fichiers lus, pas au chronomètre) est passée au troisième essai | `exam/fiche-optimisation.md` |
| **1er sept, soir** | **Blanc n°3 — 43/45, 96 % pondéré** | **Le vrai signal n'est pas le score** : ce blanc est le moins indépendant des trois, écrit le jour même à partir de la matière révisée le jour même. Ce qui compte : **cinq questions portaient sur les chapitres 12, 13, 15 et 17, non lus — les cinq sont justes**. Les chapitres manquants n'étaient pas le trou redouté. Et un raté qui résiste à tout : **`TRUNCATE` + `COPY INTO` = zéro ligne**. Deux fois au blanc n°1, dans mes *Termes à revoir* depuis le 31 juillet, juste en salve ce matin, **expliqué correctement à l'oral quatre heures avant** — et raté quand même. « Seulement les nouvelles » répond à *que fait `COPY INTO` normalement*, pas à *que se passe-t-il après un `TRUNCATE`* | `exam/mock-exam-3.md` annoté |
| **2 sept, matin** | **Salve de décroissance à froid — 15/15** *(après correction)* | **L'hypothèse de décroissance ne s'est pas vérifiée.** Après une nuit et sans relecture, les quatre faits tombés le plus récemment sont tous justes, `TRUNCATE` + `COPY INTO` compris — celui qui avait résisté à cinq expositions. **Le seul « raté » était une question mal formulée de Claude**, que j'ai contestée à raison : « le trou de bordure est-il fermé par `>=` ? » — au sens littéral de *bordure* (le cas d'égalité), **oui**, avec déduplication. C'est même la réponse attendue à l'examen. Ce que Claude visait était la **marge de sécurité**, que le journal liste comme un terme **distinct** : `now()` rend l'heure de début de transaction, donc une ligne validée après ma lecture porte un horodatage strictement inférieur au watermark et n'est jamais relue — et dédoublonner n'y peut rien, puisqu'elle n'a jamais été extraite. **Deux problèmes, deux parades : `>=` pour l'égalité, la marge pour la visibilité différée** | Manuel 9.2, p. 156 |
| **2 sept, ap.-midi** | **Guide officiel vérifié · atelier médaillon · ch. 12 et 17 lus · salve finale 15/15** | **Le guide officiel du 4 mai est inchangé** : sept sections, poids et 33 objectifs identiques à `docs/04`. Quatre précisions ajoutées : des **questions non notées s'ajoutent aux 45** sans être identifiées ; Lakeflow Connect se décline en connecteurs **standard** et **managed** ; deux cours recommandés manquaient à mon parcours (*Data Interoperability with UC*, *Get Started with Data Governance*) ; le guide contient **cinq vraies questions d'examen retirées**. L'une d'elles **corrige la fiche compute** : pour des analystes SQL concurrents, la réponse officielle est *high-concurrency cluster avec autoscaling* — parce que le SQL warehouse **n'était pas dans les options**. La leçon vaut plus que le fait : **on coche la moins fausse des quatre, pas la réponse qu'on aurait écrite**. Salve finale 15/15, incluant les chapitres 12 et 17 lus dans l'heure et les deux faits appris deux heures plus tôt | Guide officiel PDF, `exam/fiche-compute.md` §2 bis |
| | | | |
| | | | |
| | | | |
| | | | |
| | | | |
| | | | |
| | | | |

---

## Termes à revoir

Amorcé par le diagnostic du 29 juillet : chaque ligne correspond à une erreur. Coche
quand tu peux l'expliquer à voix haute sans notes, et **continue la liste** à chaque
module.

| Terme | Ce que c'est | Section | Vu en |
|---|---|---|---|
| `_rescued_data` contre `_corrupt_record` | **Écart au schéma** (ligne lisible, type ou colonne inattendus) contre **échec d'analyse** (ligne inexploitable, tous champs nuls). Deux colonnes, deux questions | 2 | M1 |
| `rescuedDataColumn` × `inferColumnTypes` | Sans inférence de type, la sauvetage n'a presque rien à capturer sur un CSV. Les deux options se répondent | 2 | M1 |
| Troncature CSV silencieuse | Jetons en trop = jetés. Compte de lignes juste, contenu mutilé | 2, 6 | M1 |
| `unionByName` | Aligne par **nom**. `union` aligne par position, silencieusement | 3 | M3 |
| `summary()` vs `describe()` | `summary()` ajoute les quartiles et accepte des percentiles | 3 | M3 |
| `autoBroadcastJoinThreshold` | Seuil de diffusion automatique. `-1` la **désactive** | 3 | M3, M12 |
| *Spill (memory)* / *Spill (disk)* | Métriques de stage. Ralentit sans lever d'erreur | 6 | M12 |
| Driver vs exécuteur | `collect()` rapatrie sur le **driver** — cause n°1 des OOM | 6 | M12 |
| Liquid clustering (`CLUSTER BY`) | Recommandation actuelle. Le Z-ordering n'est plus le défaut | 6 | M12 |
| *Predictive optimization* | `OPTIMIZE` et `VACUUM` automatiques sur les tables **managées** | 6 | M10, M12 |
| Élagage de fichiers | La vraie mesure d'un regroupement : fichiers lus / fichiers totaux | 6 | M12 |
| Tâche *skipped* vs *failed* | Sautée ≠ échouée. Le job finit **en vert** | 4 | M8 |
| Timeout de tâche | Aucun par défaut. Plus court que celui du job | 4 | M8 |
| `databricks bundle validate` | Résout les variables et vérifie, **sans rien modifier** | 5 | M11 |
| `targets` | Les environnements et leurs surcharges. Pas les tables cibles | 5 | M11 |
| État souhaité | Retirer du YAML **supprime** au déploiement suivant | 5 | M11 |
| `RESTORE TABLE ... TO VERSION AS OF` | La seule commande de retour arrière qui existe | 1 | M4 |
| SQL warehouse serverless | Interactif + concurrent + démarrage en secondes | 1 | M0 |
| `metastore → catalog → schema → table` | `USE` requis à **tous** les niveaux supérieurs | 1 | M0 |
| Masque et filtre de lignes | **Impossibles sur une vue.** Se posent sur les tables | 7 | M10 |
| Historique de `COPY INTO` | Dans les métadonnées de la table. `TRUNCATE` ne le remet pas | 2 | M13 |
| Bordure de watermark | `>` strict perd les transactions validées après la lecture | 2 | M2 |
| Marge de sécurité de watermark | Le `>=` ne ferme que l'égalité. Une transaction horodatée **strictement avant** le watermark et validée après la lecture échappe encore : d'où `watermark - INTERVAL 15 MINUTES`. Niveau entretien plus que QCM | 2 | M2 |
| Idempotence de la cible | Ce qui décide du coût d'une ré-ingestion, ce n'est pas le volume : c'est de savoir si rejouer une ligne déclenche quelque chose en aval | 2 | M2 |
| Transformation vs **action** | Un DataFrame est un **plan**. `select`, `filter`, `withColumn`, `join` n'exécutent rien. `count`, `write`, `first`, `collect`, `show`, `display` rejouent le plan **entier depuis la source**, à chaque fois | 3 | M2, M3 |
| Mode **ANSI** et famille `try_*` | Avec ANSI actif — le défaut ici et sur Databricks SQL — un `cast` raté **lève** au lieu de rendre `NULL`. Parades : garde `when`/`rlike`, `try_cast`, `try_to_timestamp`, `try_to_number` | 3 | M3, M6 |
| Motif de date **Java** | `MM` mois / `mm` minutes · `HH` 24 h / `hh` 12 h · `dd` jour du mois / `DD` jour de l'année · `yyyy` année / **`YYYY` année ISO de la semaine** — faux au réveillon, juste le reste de l'année | 3 | M3 |
| Réconciliation absolue | Un watermark est incrémental, donc aveugle à ce qui disparaît. On le double d'un comptage complet source/cible, moins fréquent | 2, 6 | M2, M6 |
| Journal d'événements du cluster | Là où se lisent les échecs **avant** toute application : démarrage, capacité, conflit de bibliothèques. L'interface du moteur et le plan n'existent pas à ce stade | 6 | M12 |
| `run_if` | `depends_on` = « après le **succès** de ». Une tâche d'alerte sur échec exige `AT_LEAST_ONE_FAILED` ; un résumé systématique, `ALL_DONE` | 4 | M8 |
| Coût × fréquence | Le critère du choix vue / vue matérialisée. On matérialise ce qui est **lu bien plus souvent qu'écrit**, jamais l'inverse | 3 | M5 |
| Intervalle semi-ouvert | `[valid_from, valid_to)` : début inclus, **fin exclue**. Fermer à la date du changement compte la journée deux fois | 3 | M4 |
| `unionByName` et `allowMissingColumns` | Par défaut **lève** si les colonnes diffèrent. Le remplissage par `NULL` exige `allowMissingColumns=True` | 3 | M3 |
| Ligne JSON invalide | Va dans `_corrupt_record`, **jamais** dans `_rescued_data`, et n'est **pas** ignorée silencieusement | 2 | M1 |
| Jointure sur dimension historisée | Sur la clé **et** l'intervalle. Sans la période, les faits se multiplient par le nombre de versions, de façon **non uniforme** | 3 | M4, M5 |
| `WHEN NOT MATCHED BY SOURCE` | La seule clause qui ferme un enregistrement **disparu** de la source | 3 | M4 |
| Définition relative au présent | `current_date()` dans une vue matérialisée : la fenêtre se décale à chaque rafraîchissement, **sans erreur ni valeur aberrante** | 3 | M5 |
| Table de flux et corrections | Elle ne relit jamais le passé. Si la source corrige rétroactivement, il faut une **vue matérialisée** | 3 | M7 |
| Rafraîchissement complet | Reconstruit depuis la source, donc **efface l'historique d'une SCD2**. À exclure dès la déclaration | 3, 4 | M7, M4 |
| `debugValue` | Sans lui, un carnet lisant une valeur de tâche **ne peut plus s'exécuter seul** hors du graphe | 4 | M8 |
| `MERGE` sur table à politique | Un masque ou un filtre de lignes rend la table **incompatible** avec la fusion. Ce n'est pas un résultat faux, c'est un refus — la chaîne SCD2 s'arrête | 7 | M10, M4 |

---

## Affirmations à vérifier

Ce qu'une IA m'a dit et que je n'ai pas encore confirmé sur docs.databricks.com.

| Affirmation | Source | Vérifiée ? |
|---|---|---|
| « Les champs CSV en trop atterrissent dans `_rescued_data` sous forme de `_c14`, `_c15` » | Claude, énoncé et grader de M1 | **31 juil — FAUX.** Testé sur `orders_2026-05.csv`, 181 lignes défectueuses, schéma explicite, `rescuedDataColumn` posé : **0 ligne sauvée**. Le lecteur CSV **tronque** les jetons excédentaires. Grader, énoncé et corrigé corrigés depuis |
| « Les lignes JSON illisibles atterrissent dans `_rescued_data` » | Claude, critère 10 de M1 | **3 août — FAUX aussi.** Elles vont dans **`_corrupt_record`**. `_rescued_data` reste vide. Deux mécanismes distincts : *écart au schéma* contre *échec d'analyse*. Grader, énoncés M1/M3/M6 et corrigés corrigés depuis |
| « `ref_categories_raw` compte 42 lignes » | Claude, critère 11 de M1 | **4 août — FAUX.** Le fichier en contient **39** (8 top-catégories × 4 à 6 sous-catégories). Valeur écrite de mémoire au lieu d'être lue dans `graders/expected/W0_ref.json`, qui disait 39 depuis le début |
| « Lakebase s'ouvre par *Apps → Lakebase Postgres* » | Claude, README de M2 | **4 août — chemin faux ou périmé.** Trouvé ailleurs dans l'interface. `databricks psql` existe bien, mais exige `psql` installé localement — prérequis externe non mentionné |
| « `spark.sql(..., args={...})` accepte des paramètres nommés dans un `MERGE` » | Claude, M2 | **4 août — VRAI**, vérifié par aller-retour `set` / `get` : branche INSERT, branche UPDATE et cas vide, les trois passent |
| « Mets un `.cache()` quand un DataFrame sert deux fois » | Claude, M2 et fiches d'outillage | **4 août — IMPOSSIBLE en Free Edition.** `PERSIST TABLE is not supported on serverless compute` (SQLSTATE 0A000). Contrainte ajoutée à `docs/01`, corrigés M2 et M3 nettoyés. **Le concept reste au programme de l'examen** — seule la pratique est impossible ici |
| « Les prix propres s'écrivent avec un point ; le filtre de diagnostic est `^-?[0-9]+(\.[0-9]+)?$` » | Claude, en séance sur M3 | **5 août — FAUX.** Le contrat définit `unit_price` comme **décimal à virgule** (`^[0-9]+,[0-9]{2}$`, cf. `M6_qualite_solution.py`). Mon filtre a signalé 287 502 lignes « polluées » sur 287 785 : c'est la norme qu'il comptait, pas le bruit. Le vrai compte est **1 102**. Filtre improvisé au lieu d'être repris du corrigé M3, TODO A |
| « Un `cast` impossible rend `NULL` » — et « le garde-fou sur la chaîne vide est cosmétique » | Claude, corrigés M3 et M6, fiches d'outillage | **5 août — FAUX ici : le mode ANSI est actif.** `CANNOT_PARSE_TIMESTAMP` (22007) sur `to_timestamp`, `CAST_INVALID_INPUT` (22018) sur `cast`. **Toute la stratégie de quarantaine reposait sur `isNull()` et se serait arrêtée sur la première valeur sale.** Corrigés M3 (3 fonctions + `event_ts_expr`) et M6 (`try_cast`) réparés, `docs/01` complété. Le garde `when`/`rlike` que j'avais qualifié de décoratif est en réalité **porteur** |
| « Les jetons excédentaires d'un CSV vont dans la colonne de sauvetage, la ligne est conservée » | Claude, **corrigé de `mock-exam-2.md`, Q11** | **1er sept — FAUX, et déjà falsifié le 31 juillet.** Le corrigé de M1 avait été réparé ; la réparation n'a **jamais été propagée aux examens blancs**. J'ai répondu juste et le corrigé m'a compté faux. Point crédité. Leçon : un corrigé de ce dépôt n'est pas une autorité — le journal, si |
| « Passer le curseur en `>=` ne ferme pas le trou de bordure » | Claude, salve de décroissance du 2 sept | **2 sept — question mal formulée, point crédité.** *Bordure* désigne le cas d'**égalité**, que `>=` ferme bien, avec déduplication — c'est la réponse attendue à l'examen. Claude visait la *marge de sécurité*, que ce journal liste pourtant comme un terme distinct. Deuxième défaut de ses propres supports que je relève en deux jours, après le corrigé Q11 du blanc n°2 |
| | | |

---

## Examens blancs

| Date | Examen | Score | Sections perdues |
|---|---|---|---|
| *29 et 31 juil* | *diagnostic, 7 fiches* | *55 / 80 — 70 % pondéré* | *3 (58 %), 6 (50 %), 1 (60 %)* |
| **31 août** | **n°1** | **36 / 45 — 80 % · 82 % pondéré** | **7 (62 %), 5 (67 %), 2 (78 %)** |
| **1er sept** | **n°2** | **34 / 45 — 76 % pondéré** *(Q11 créditée, corrigé faux)* | **5 (60 %), 6 (67 %), 4 (71 %), 7 (71 %)** |
| **1er sept** | **n°3** *(format du jour J)* | **43 / 45 — 96 % pondéré** | **5 (80 %), 2 (89 %)** — le reste à 100 % |
| | n°2 | … / 45 | |

Compare surtout ta **progression par section** entre les deux : c'est elle qui dit si les
révisions ont porté, pas le score global.

> Le diagnostic figure en italique parce qu'il n'est **pas comparable** aux deux blancs :
> ses options portaient le biais de position décrit plus haut, celles des blancs ont été
> rebrassées. Compare les blancs entre eux ; sers-toi du diagnostic pour les priorités,
> pas pour mesurer une progression.

---

## Dernière vérification, deux semaines avant

- [ ] Re-télécharger le guide d'examen officiel et vérifier que les objectifs n'ont pas
      bougé — Databricks le met à jour sans annonce
- [ ] Relire `docs/04-couverture-certification.md` en entier
- [ ] Relire `docs/05-glossaire-renommages.md`
- [ ] Refaire les dix gestes
