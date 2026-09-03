# Planning express — trois jours de rush

**Écrit le lundi 31 août 2026**, révisé le même jour après inventaire du *Manuel du Data
Engineer*, puis **le 1er septembre au soir** quand la date réelle de l'examen a été confirmée. Il remplace la fenêtre 2 de `docs/06-protocole-revision.md`, qui tablait sur
48 h et n'a jamais eu lieu.

> ✅ **Date confirmée le 1er septembre au soir : l'examen est bien le jeudi 3 septembre,
> 9 h 45**, comme le disaient le journal et le protocole depuis le début. Le « mercredi »
> du 31 août était une erreur. **Une journée pleine est donc récupérée** — voir *Jour 3*.

---

## Ce que l'inventaire du manuel a changé

Tu es page **207 sur 400**. Ce n'est pas une moitié quelconque.

Les 207 pages lues couvrent les fondations et l'ingestion — **sections 1 et 2 de l'examen,
celles où tu es déjà le meilleur** (60 % et 83 % au diagnostic). Les 193 pages restantes
contiennent les chapitres 18 (orchestration), 20 (diagnostic), 21 (gouvernance, CI/CD,
coût), 16 (objets de publication), 12, 14, 17 et 19.

**Les sections 4, 5, 6 et 7 — 51 % de l'examen — sont intégralement dans les pages non
lues**, plus la moitié la plus lourde de la section 3.

Autrement dit : **ton retard de lecture et tes lacunes d'examen sont le même retard.**
Finir le livre n'est pas un objectif parallèle à la révision. C'est la révision. Ton
engagement à le lire en entier est donc le meilleur du plan, et il devient l'ossature de
ces deux jours plutôt qu'un à-côté.

Pagination complète et correspondance objectif → chapitre : `exam/index-manuel.md`. Le
folio imprimé correspond exactement au numéro de page du PDF, vérifié.

---

## La boucle

Quatre salves, deux par jour. Chaque salve suit les mêmes cinq temps.

| Temps | Ce qui se passe | Durée |
|---|---|---|
| **1 · Salve** | Je pose 12 à 16 questions en QCM cliquable, style examen. Sans notes, sans livre, sans Databricks ouvert | 25 min |
| **2 · Correction** | Pour chaque raté : ta méprise nommée, pourquoi la bonne réponse est bonne, et **le chapitre et les pages exactes** | 20 min |
| **3 · Lecture ciblée** | Tu lis les pages désignées. C'est là que le livre se lit — pas en parallèle, *en réponse à une erreur* | 40 min |
| **4 · Contre-salve** | Je reteste **les mêmes objectifs, autrement formulés**, 6 à 8 questions | 15 min |
| **5 · Consignation** | Une ligne dans `exam/journal.md`, tableau *Ce que chaque session m'a appris* | 5 min |

Le temps 4 est celui qu'on est tenté de sauter, et c'est le seul qui prouve quelque chose.
Une bonne réponse juste après la correction ne mesure que ta mémoire à court terme. Une
bonne réponse à la **même idée reformulée** mesure la compréhension. Quand tu rates la
contre-salve, le concept n'est pas acquis et on y revient — c'est prévu, ce n'est pas un
échec.

> **Ce qui s'est réellement passé** : les jours 1 et 2 ont été bouclés en une journée et
> demie, six salves au lieu de quatre, et trois fiches express écrites en cours de route
> (`fiche-ingestion.md`, `fiche-cicd.md`, `fiche-optimisation.md`, plus `fiche-compute.md`).
> Les deux gestes prévus au jour 2 n'ont **pas** été faits — ils basculent au jour 3.

Les questions des salves sont **écrites à la demande**, pas tirées de `exam/qcm-section-*.md`.
Ces fiches-là, tu les as déjà vues en juillet et elles portent le biais de position. Les
deux blancs, eux, ont été rebrassés : ils restent la mesure.

---

## Jour 1 — lundi 31 août

| | Bloc | Durée |
|---|---|---|
| 0 | **Blanc n°1** chronométré — *en cours* | 1 h 30 |
| 1 | **Dépouillement + carte des ratés** : chaque erreur → section, chapitre, pages | 45 min |
| 2 | **Lecture 21** — Sécuriser, déployer, maîtriser le coût, p. **361-378** | 50 min |
| 3 | **Salve A · sections 7 et 5** (gouvernance, CI/CD) + correction + contre-salve | 1 h |
| 4 | **Lecture 20** — Diagnostiquer et optimiser, p. **345-360** | 45 min |
| 5 | **Salve B · section 6** (diagnostic, optimisation) + correction + contre-salve | 50 min |
| 6 | **Lecture 18** — Orchestrer les traitements, p. **313-328** | 45 min |
| 7 | Journal | 15 min |

**~6 h 40, zéro compute.** Le chapitre 21 passe en premier parce que ses dix-huit pages
couvrent 25 % de l'examen — sections 7, 5 et l'objectif coût de la section 1. Aucune autre
tranche du livre n'a ce rendement.

Ces trois chapitres correspondent aux trois modules jamais ouverts : M10, M11, M12. Tu vas
donc les apprendre par le livre, en lecture, sans les construire. C'est l'arbitrage central
de ces deux jours et il est assumé plus bas.

## Jour 2 — mardi 1er septembre

| | Bloc | Durée |
|---|---|---|
| 1 | **Salve C · section 4** (jobs, déclencheurs) sur la lecture d'hier soir + correction | 45 min |
| 2 | **Lecture 19 + 16** — pipelines déclaratifs p. **329-344**, objets de publication p. **281-294** | 1 h 15 |
| 3 | 🔧 **Geste 7** — un job à 4 tâches : nouvelle tentative, condition, valeur de tâche | 40 min |
| 4 | 🔧 **Geste 10** — un masque de colonne et un filtre de lignes, vérifiés **dans les deux sens** | 35 min |
| 5 | **Lecture 12 + 14** — agréger et fenêtrer p. **211-226**, historiser p. **245-262** | 1 h 15 |
| 6 | **Salve D · section 3** (transformation) + correction + contre-salve | 1 h |
| 7 | **Blanc n°2** chronométré, à froid | 1 h 30 |

**~7 h, ~1 h 15 de compute** — sous le plafond de 4 h de la Free Edition.

Deux gestes seulement sur dix, et ce sont ceux-là : ils tiennent en moins de 40 minutes
chacun et cimentent des sections à 16 % et 15 %. Les huit autres sautent.

Ne corrige pas le blanc n°2 le soir même si tu es cuit. Compare-le au n°1 **section par
section** : c'est la progression par section qui informe, pas le score global.

## Reliquat de lecture

Après le jour 2, il restera les chapitres **13** (p. 227-244), **15** (p. 265-280),
**17** (p. 295-310) et les annexes **A** et **B** (p. 381-394) — environ 90 pages.

L'annexe **B, SQL ↔ PySpark** (p. 389-394, six pages) est la plus rentable des cinq et la
plus courte : lis-la même si tu ne lis rien d'autre. La section 3 pèse 22 % et la moitié de
ses questions se joue sur la traduction d'une intention SQL en verbe PySpark.

---

## La veille au soir · 45 min, et rien de plus

Un seul document : **`exam/journal.md`**.

1. Les **sept causes** derrière les 25 erreurs du diagnostic
2. **Termes à revoir** — chaque ligne à voix haute, sans regarder la colonne de droite
3. **Affirmations à vérifier** — les huit falsifiées. Ce sont des choses que tu as crues
   vraies et qui ne l'étaient pas : c'est exactement la forme d'un distracteur de QCM

**Aucun corrigé, aucun nouveau chapitre, aucun code.**

Deux réflexes à emporter, tirés de tes propres ratés :

- Devant quatre options, **éliminer d'abord celles qui n'existent pas**. `UNDO LAST WRITE`
  t'a coûté une question en juillet parce que la commande est inventée.
- **Prendre les deux minutes.** Tu as fait le diagnostic à 40 secondes par question pour
  120 disponibles. Le temps n'est pas ta contrainte, et répondre à l'instinct est
  précisément le mode qu'un distracteur bien écrit récompense.

---

## Jour 3 — mercredi 2 septembre

**Objectif : ne rien ajouter, faire tenir.**

Trois mesures commandent cette journée. Le blanc n°3 est à 96 % mais il est surestimé — je
l'ai écrit le jour même sur la matière du jour même. Les cinq questions portant sur les
chapitres **non lus** sont toutes justes : le manque de lecture n'est plus le problème. Et
les deux derniers ratés sont des **énoncés lus trop vite**, pas des trous.

Reste la mesure qui décide de tout : au blanc n°2, **trois faits justes en salve la veille
sont tombés dix-huit heures plus tard**. Jeudi matin, tu seras à **trente-six heures** de
la révision de mardi.

| | Bloc | Durée |
|---|---|---|
| 1 | **Salve de décroissance**, à froid, **avant toute relecture** — 15 questions mêlées | 40 min |
| 2 | **Re-télécharger le guide d'examen officiel** et le confronter à `docs/04` | 30 min |
| 3 | Réparation de ce qui est tombé au bloc 1 | 45 min |
| 4 | 🔧 **`COPY INTO` de tes mains** : charger, `TRUNCATE`, relancer, **regarder la table rester vide** | 20 min |
| 5 | 🔧 **Geste 7** — job à 4 tâches : nouvelle tentative, condition, valeur de tâche | 40 min |
| 6 | 🔧 **Geste 10** — masque de colonne et filtre de lignes, vérifiés **dans les deux sens** | 35 min |
| 7 | **Chapitres 12 et 17** — p. 211-226 et p. 295-310 | 1 h |
| 8 | **Termes à revoir**, à voix haute, en entier | 1 h |
| 9 | Logistique : test système, pièce d'identité, conditions de passage | 20 min |

**~6 h 20, dont ~1 h 35 de compute** — sous le plafond de la Free Edition.

### Pourquoi le guide officiel passe en priorité n°1

C'est dans ton propre protocole — *« re-télécharger le guide officiel deux semaines avant
et vérifier que les objectifs n'ont pas bougé »* — et **ça n'a jamais été fait**. Tout ce
qui a été construit repose sur une transcription de la version du 4 mai 2026. Databricks
met le guide à jour sans annonce. C'est la seule action de la journée qui puisse révéler
un angle mort **complet**, et elle coûte trente minutes.

### Pourquoi les gestes, maintenant

Tu n'as pas touché Databricks depuis le 5 août : deux jours entiers de révision sans une
ligne de code, avec un quota de compute intact.

Et surtout, le bloc 4 règle un cas particulier. **`TRUNCATE` puis `COPY INTO` t'a échappé
quatre fois sur cinq expositions** — deux au blanc n°1, une au blanc n°3, alors qu'entre
les deux tu l'avais juste en salve et que tu me l'as expliqué correctement à l'oral. La
répétition ne fonctionne pas sur ce fait. Alors cesse de le réviser et **fais-le** : la
table qui reste vide sous tes yeux réglera en quinze minutes ce que cinq passages n'ont
pas réglé.

Le geste laisse une trace que la lecture ne laisse pas — c'est exactement ce qu'il faut
pour survivre à une nuit.

### Ce qui saute, et pourquoi

Les chapitres **13 et 15** ne seront pas lus. Le blanc n°3 a montré que tu tiens leurs
objectifs au niveau où l'examen les pose — `explode_outer`, le grain d'une table de faits,
le type d'historisation, tous justes. Trente-six pages qui rapporteraient moins qu'une
heure de gestes.

---

## Jeudi 3 septembre — rien

Pas de fiche, pas de chapitre, pas de salve, pas de dernière relecture à 8 h.

Deux réflexes à emporter, et rien d'autre :

- **Relis chaque énoncé une seconde fois avant de choisir.** Tes trois derniers ratés sont
  des questions dont l'énoncé portait la réponse.
- **Ne rends pas ta copie en sortant.** À ton rythme — dix-neuf minutes aux blancs n°1 et
  n°3 — tu auras fini vers 10 h 05 pour une épreuve qui se termine à 11 h 15. Ces
  soixante-dix minutes existeront : repasse sur ce que tu as hésité à cocher.

---

## Ce que ce plan abandonne, explicitement

Pour que ce soit dit et pas subi : **M6 à M13 ne seront pas construits**, M4 et M5
seulement lus. Tu passeras l'examen sans avoir jamais déployé un bundle, ouvert un Spark UI
ni posé une politique ABAC de tes mains. Tu les connaîtras par le chapitre 21 et par les
salves, ce qui suffit à un QCM et ne suffit pas à un entretien.

L'examen ne fera pas la différence. Le dépôt reste entier pour les faire après, dans le bon
ordre et sans la montre.

Seuil de réussite : **de mémoire ~70 %, non vérifié**. Confirme-le sur ta convocation
plutôt que sur ma parole — c'est la règle qu'on s'est donnée le 31 juillet.
