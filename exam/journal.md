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

### Total par jour

| Jour | Sessions | Actif |
|---|---|---|
| 29 juil | 1 | 43 min |
| 31 juil | 3 | ~3 h 25 |
| 3 août | 2 | **5 h 40** |
| 4 août | 2 | **4 h 28** |
| 5 août | 3 | **5 h 15** *(journée en cours)* |
| | | |
| **Depuis lundi 3 août** | | **10 h 07** |
| **Cumul depuis le 29 juillet** | | **~14 h 15** |

---

## Fenêtre 2 — consolidation

| Étape | Prévu | Fait le | Réel | Résultat |
|---|---|---|---|---|
| Remise en main au retour | 2 h | | | — |
| **Blanc n°1** chronométré | 1 h 30 | | | *voir « Examens blancs »* |
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
| | | |

---

## Examens blancs

| Date | Examen | Score | Sections perdues |
|---|---|---|---|
| *29 et 31 juil* | *diagnostic, 7 fiches* | *55 / 80 — 70 % pondéré* | *3 (58 %), 6 (50 %), 1 (60 %)* |
| | n°1 | … / 45 | |
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
