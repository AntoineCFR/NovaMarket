# Protocole de révision

Adapté du guide officiel *AI Prep Guide: Any Databricks Certification*. Il complète le
parcours NovaMarket : le projet t'apprend à faire, ce protocole t'apprend à répondre.

**Examen : jeudi 3 septembre 2026, 9 h 45.**

---

## Le rituel de chaque session

Trois choses à coller en tête de session, dans cet ordre :

1. Le **guide d'examen officiel** en pièce jointe (version en cours — re-télécharge-le
   deux semaines avant l'examen).
2. Le tableau de `docs/05-glossaire-renommages.md`.
3. Le prompt de mise en condition, en fin de ce même document.

Sans ça, tu obtiendras des réponses assurées sur des produits qui n'existent plus.

---

## La boucle en 5 temps, une section à la fois

| Temps | Ce que tu demandes |
|---|---|
| **1. Orienter** | « Donne-moi un aperçu en français simple des objectifs de la [SECTION] du guide joint. Une phrase par objectif. » |
| **2. Diagnostiquer** | « Interroge-moi avec 10 QCM sur la [SECTION], dans le style d'un vrai examen Databricks. Ne montre les réponses qu'après les miennes. » |
| **3. Approfondir** | « Enseigne-moi [OBJECTIF RATÉ, COPIÉ MOT POUR MOT DU GUIDE], en t'appuyant uniquement sur docs.databricks.com et Databricks Academy. Cite l'URL de chaque affirmation ; si tu ne peux pas vérifier, dis-le. Structure : concept, fonctionnement sur Databricks, quand l'utiliser plutôt qu'une alternative, erreurs classiques, un exemple de code exécutable. » |
| **4. Pratiquer** | Un examen blanc complet — `exam/mock-exam-1.md` et `-2.md` dans ce dépôt, ou généré à la demande. |
| **5. Réparer** | « Voici les questions ratées : [COLLER]. Pour chacune : explique ma méprise, pourquoi la bonne réponse est bonne, et donne-moi une question voisine qui teste le même concept autrement. » |

**Diagnostique avant de réviser.** L'erreur la plus coûteuse est de repasser du temps sur
ce que tu maîtrises déjà.

> **Diagnostic passé les 29 et 31 juillet 2026 — 55 / 80, 70 % pondéré.** Résultat détaillé,
> analyse du biais de position des fiches et les sept causes derrière les 25 erreurs :
> `exam/journal.md`. Rang de priorité mesuré : **3 › 6 › 4 › 2 › 5 › 7 › 1**.
>
> Deux conséquences sur ce qui suit. La section 3 (transformation, 22 %) est la seule
> famille d'erreurs qui relève d'un **réflexe** et non d'un trou de connaissance — lire du
> PySpark comme du SQL. Elle se corrige en écrivant du code, donc dans M3, pas en lisant.
> Et la section 7 (gouvernance, 15 %) était déjà à 83 % sans avoir rien travaillé : M10
> vaut moins de temps que prévu, M12 en vaut davantage.

---

## Les 10 gestes à savoir faire sans réfléchir

Le guide est formel : aucune certification Databricks ne se passe en lisant. Chaque
tâche ci-dessous tient en moins de 30 minutes en Free Edition. **Toute tâche que tu ne
sais pas faire est ta prochaine cible de révision.**

| # | Geste | Section | Module |
|---|---|---|---|
| 1 | Créer catalog, schema et volume Unity Catalog, y téléverser un fichier par la CLI | 1, 7 | M0 |
| 2 | Ingérer un CSV par Auto Loader avec colonne de sauvetage et évolution de schéma | 2 | M1 |
| 3 | Charger le même fichier avec `COPY INTO` et savoir dire lequel choisir, et pourquoi | 2 | M13 |
| 4 | Écrire un silver typé avec quarantaine explicite plutôt qu'un `WHERE` silencieux | 3 | M3 |
| 5 | Enchaîner les six formes de jointure et savoir laquelle déclenche un *broadcast* | 3 | M3 |
| 6 | Construire une vue, une vue matérialisée et une table de streaming, et savoir comment chacune se rafraîchit | 3 | M5, M7 |
| 7 | Monter un job à quatre tâches avec nouvelle tentative, condition et valeur de tâche | 4 | M8 |
| 8 | Déployer un bundle sur deux `targets` avec des variables et des overrides | 5 | M11 |
| 9 | Ouvrir un Spark UI et nommer le goulot : *skew*, *shuffle* ou *spill* | 6 | M12 |
| 10 | Poser un masque de colonne et un filtre de ligne, puis vérifier **les deux** comportements | 7 | M10 |

---

## Le planning

Calé sur la disponibilité réelle : **absence du 6 au 24 août**, jusqu'à 8 h par jour en
semaine le reste du temps, quelques heures les week-ends.

> **Recalé le 3 août 2026** : départ fixé au **7 août au matin**, retour supposé inchangé.
> Le plan initial tablait sur huit jours de fenêtre 1 ; il en reste quatre, et le
> diagnostic a par ailleurs alourdi M1 et M3. Ce qui suit est la version à jour.

| Fenêtre | Dates | Jours | Capacité |
|---|---|---|---|
| ~~Fenêtre 1 initiale~~ | ~~29 juil → 5 août~~ | ~~8~~ | ~~54 h~~ |
| **Fenêtre 1 réelle** — le projet | 3 → 6 août | 4 (tous en semaine) | **24 h** |
| *Absence* | 7 → 24 août | 18 | — |
| **Fenêtre 2** — consolidation | 25 août → 2 sept | 9 (7 sem. + 2 WE) | **48 h** |

**72 h disponibles pour ~64 h de travail.** La marge s'est réduite de moitié. Elle reste
suffisante, mais elle n'absorbe plus une semaine perdue : le projet ne tiendra pas
entièrement avant le départ, et **une partie bascule volontairement en fenêtre 2**.

### Ce qui bascule, et pourquoi

Il reste **37 h 30** de projet pour 24 h disponibles. On ne comprime pas : on coupe à un
endroit qui a du sens.

La coupure est posée **après la couche gold**. Au 6 août au soir, la chaîne
`bronze → silver → gold` existe et répond aux questions métier — c'est un état cohérent,
qu'on peut laisser dormir dix-huit jours sans rien perdre. Tout ce qui suit (qualité,
pipeline déclaratif, orchestration, capstone) forme un second bloc qui se reprend d'un
seul tenant.

Bénéfice de côté : les vagues **W3** (dérive de schéma, en M8) et **W4** (incident de
production, en M9) tombent désormais dans les jours qui précèdent l'examen. Ce sont les
deux exercices les plus riches du parcours ; les avoir frais est un avantage, pas un
pis-aller.

### Deux règles avant de commencer

**Six heures par jour, pas huit.** À pleine charge tu termines en huit jours et tu passes
les neuf suivants à attendre — en ayant traversé les modules sans faire le travail qui
compte : répondre aux questions d'analyse, tenir tes propres arbitrages. Six heures
laissent 40 % de marge, et cette marge est ce qui absorbe les imprévus.

**Le quota Free Edition va te bloquer.** En cas de dépassement, le compute est coupé
**jusqu'au lendemain**. Six heures de Spark quotidien, c'est beaucoup. Structure chaque
journée en **~4 h de compute et ~2 h hors compute** — lecture des fiches, QCM, rédaction
de tes réponses. Une coupure coûte alors une demi-journée, pas une journée.

---

### Fenêtre 1 · 3 → 6 août · jusqu'à la couche gold

*Fait : le diagnostic (29 et 31 juillet) et M0 (31 juillet, validé 9/9).*

| Jour | Compute (~4 h) | Hors compute (~2 h) | Total |
|---|---|---|---|
| **Lun 3 août** | M1 V2 — les trois flux + la passe de réparation (2 h 30) · M2 (2 h) | Analyse M1, questions 1 à 7 (45 min) | 5 h 15 |
| **Mar 4 août** | M3 — silver, typage, quarantaine (3 h) | Questions d'analyse M3 (1 h 15) · **complément jointures et agrégations** (1 h 30) | 5 h 45 |
| **Mer 5 août** | M4 — SCD2, MERGE, CDF (3 h) · M5 début (1 h 30) | Questions d'analyse M4 (1 h 30) | 6 h |
| **Jeu 6 août** | M5 fin — gold, modèle en étoile (2 h 30) | Complément *objets gold* (30 min) · relecture, journal, mise au propre (1 h) | 4 h |

**21 h planifiées sur 24.** À l'arrivée : `bronze → silver → gold`, une chaîne complète
qui répond aux six questions métier.

Trois remarques sur ce découpage :

- **Le complément jointures est en tête de liste, pas en fin.** La section 3 est première
  au diagnostic par une marge du simple au double. C'est le seul endroit du parcours où
  du temps supplémentaire se justifie par la mesure.
- **Le jeudi est volontairement léger.** Départ le lendemain matin : mieux vaut finir sur
  une mise au propre que sur un module entamé.
- **M6 est le tampon.** Si les trois premiers jours vont mieux que prévu, prends-le le
  jeudi (3 h 30) au lieu de la relecture. Sinon, laisse-le partir en fenêtre 2. Ne
  sacrifie jamais les questions d'analyse pour le gagner : elles sont le module, le
  grader n'en est que le contrôle.

---

### Absence · 7 → 24 août

Tout le nécessaire est en markdown, lisible hors ligne, sans Databricks :

- les sept fiches de décision — tables externes, diagnostic compute, Lakeflow Connect,
  matrice d'ingestion, objets gold, tâches et déclencheurs, **sources malformées** ;
- `docs/05-glossaire-renommages.md` ;
- les corrigés des QCM passés à froid le 29 juillet, qui se relisent tout autrement une
  fois le projet fait.

Environ 6 h de matière sur 18 jours : **20 minutes par jour suffisent**, et zéro si tu es
vraiment en vacances. Sache seulement qu'un arrêt total coûtera une demi-journée de
remise en main au retour, contre une heure sinon — et que la fenêtre 2 n'a plus les six
heures de marge qu'elle avait.

La fiche à lire en priorité si tu n'en lis qu'une : **`FICHE-source-malformee.md`**. C'est
la seule qui parte d'un défaut réel rencontré dans ton propre pipeline, et le
raisonnement qu'elle demande — nommer quatre options et leur coût — est celui qu'un
entretien creuse bien plus qu'une syntaxe.

**Et surtout, les treize `OUTILLAGE.md`.** Une par module, lisibles hors ligne, sans
Databricks. Celles de M6 à M12 correspondent à des modules que tu n'auras pas encore
faits : les lire pendant l'absence, c'est arriver le 25 août en sachant déjà de quoi ils
parlent. Vingt minutes par jour y suffisent largement, et c'est le meilleur emploi de ce
temps mort.

**Vers le 20 août, où que tu sois** : re-télécharge le guide d'examen officiel et vérifie
que les objectifs n'ont pas bougé. Cinq minutes, et c'est le seul risque que rien dans ce
dépôt ne peut couvrir.

---

### Fenêtre 2 · 25 août → 2 septembre · consolidation

Elle porte désormais **deux** charges : la fin du projet et toute la consolidation.

| Jour | Contenu | Durée |
|---|---|---|
| **Mar 25 août** | Remise en main sur le pipeline (1 h) + **M6 — qualité et métadonnées** (3 h 30) | 4 h 30 |
| **Mer 26 août** | **M7 — pipeline déclaratif** (3 h) + **M13 — `COPY INTO`, Connect, déclencheurs** (2 h 30) | 5 h 30 |
| **Jeu 27 août** | **M8 — orchestration** (4 h, vague **W3**) + complément *tâches et déclencheurs* (30 min) | 4 h 30 |
| **Ven 28 août** | **M9 — capstone** (4 h, vague **W4**) + **examen blanc n°1** chronométré (1 h 30) | 5 h 30 |
| **Sam 29 – Dim 30 août** | Réparation du blanc n°1 · **M10 — gouvernance** (3 h) | 6 h |
| **Lun 31 août** | **M11 — CI/CD** (3 h) + **M12 — performance** (3 h 30) | 6 h 30 |
| **Mar 1er sept** | **Examen blanc n°2** à froid + correction complète · les dix gestes | 6 h |
| **Mer 2 sept** | Glossaire, journal, dernières fiches. Aucun code. Coucher tôt | 3 h |

**42 h planifiées sur 48.** La marge a fondu — c'était le prix de quatre jours perdus
avant le départ. Elle reste réelle, mais elle ne pardonne plus une journée blanche.

Trois points de vigilance sur cette fenêtre :

- **Le blanc n°1 est passé le 28**, après M9 et non avant. Il n'a d'intérêt que si le
  projet est assez avancé pour que les erreurs soient instructives ; passé trop tôt il ne
  mesurerait que ce qui n'a pas encore été vu.
- **W3 et W4 arrivent le 27 et le 28.** Ce sont les deux exercices les plus riches, et ils
  supposent M8 puis M9 dans l'ordre. Ne les avance pas.
- **Si tu prends du retard**, ce qui saute en premier est M13 (2 h 30, couvert par ailleurs
  en fiche) puis le complément *tâches et déclencheurs*. Ce qui ne saute jamais : les deux
  examens blancs et les dix gestes.

### La veille et le jour même

- Relire `exam/journal.md`, pas les corrigés.
- Relire `docs/05-glossaire-renommages.md` : les pièges de vocabulaire sont les points les
  moins chers à sécuriser.
- 45 questions en 90 minutes, soit deux minutes par question. Ne pas s'enliser : marquer
  et revenir.
- Examen **jeudi 3 septembre à 9 h 45**.

> Les examens blancs générés par IA ne sont **pas** une mesure calibrée de ton niveau.
> Ton vrai signal, c'est la couverture complète des objectifs du guide (`docs/04`) plus
> l'aisance sur les dix gestes ci-dessus.

---

## Journal de révision

Un fichier, `exam/journal.md`, avec quatre colonnes : date, objectif travaillé, ce que
je n'ai pas su, où j'ai vérifié. C'est ce que tu reliras la veille — pas les corrigés.

Deux réflexes à y consigner systématiquement :

- **Tout terme inconnu**, même s'il semble mineur. C'est souvent la seule différence
  entre deux options d'un QCM.
- **Toute affirmation d'une IA que tu n'as pas pu vérifier** dans la documentation. Pas
  de lien, pas de confiance.

---

## Les cinq priorités si tu manques de temps

1. Mettre l'IA en condition à chaque session : guide d'examen + tableau des renommages.
2. Diagnostiquer avant de réviser.
3. Exécuter chaque bout de code proposé et vérifier sur docs.databricks.com. Demander
   l'URL — pas de lien, c'est que le modèle devine.
4. Recouper les sujets clés avec une **seconde** IA. Un désaccord entre deux modèles est
   un signal : va lire la documentation.
5. Faire les dix gestes. Lire ne suffira pas.
