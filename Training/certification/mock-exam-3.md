# Examen blanc n°3 — format du jour J

**45 questions · 90 minutes · aucune aide autorisée**

Répartition conforme au guide officiel du 4 mai 2026, telle que transcrite dans
`docs/04-couverture-certification.md` :

| Section | Poids | Questions |
|---|---|---|
| 1 · Plateforme | 6 % | 3 |
| 2 · Ingestion et chargement | 21 % | 9 |
| 3 · Transformation et modélisation | 22 % | 10 |
| 4 · Lakeflow Jobs | 16 % | 7 |
| 5 · CI/CD | 10 % | 5 |
| 6 · Diagnostic et optimisation | 10 % | 4 |
| 7 · Gouvernance et sécurité | 15 % | 7 |

Les sujets sont **mélangés**, comme à l'examen. Les positions des bonnes réponses ont été
réparties délibérément — aucune position n'est favorisée.

> **Ce blanc couvre aussi les chapitres 12, 13, 15 et 17 du manuel, que tu n'as pas lus.**
> C'est voulu : l'examen posera ces objectifs de toute façon. Un blanc qui n'interroge que
> ce qu'on a révisé ne mesure rien.

Chronomètre-toi. Deux minutes par question — prends-les.

---

## Résultat — 1er septembre 2026, 18 h 37 → 18 h 56

**43 / 45 — 96 %.** Pondéré : **96 %.**

| Section | Poids | n°1 | n°2 | **n°3** |
|---|---|---|---|---|
| 1 · Plateforme | 6 % | 100 % | 100 % | **100 %** |
| 2 · Ingestion | 21 % | 78 % | 83 % | **89 %** |
| 3 · Transformation | 22 % | 100 % | 82 % | **100 %** |
| 4 · Jobs | 16 % | 83 % | 71 % | **100 %** |
| 5 · CI/CD | 10 % | 67 % | 60 % | **80 %** |
| 6 · Diagnostic | 10 % | 80 % | 67 % | **100 %** |
| 7 · Gouvernance | 15 % | 62 % | 71 % | **100 %** |
| **Pondéré** | | **82 %** | **76 %** | **96 %** |

> **Réserve de méthode.** Ce blanc est le **moins indépendant** des trois : je l'ai écrit
> aujourd'hui, à partir de la matière révisée aujourd'hui. Il mesure une progression
> réelle, mais il la surestime par rapport à un examen écrit par quelqu'un d'autre.

> **Le signal le plus utile est ailleurs.** Cinq questions portaient sur les chapitres
> **12, 13, 15 et 17 — non lus** : `count` et les nulls (Q12), `explode_outer` (Q15), le
> type d'historisation (Q21), le grain d'une table de faits (Q38), le motif de `MERGE`
> SCD2 (Q40). **Les cinq sont justes.** Les chapitres manquants n'étaient pas le trou
> qu'on redoutait, au moins au niveau où ces objectifs se posent.

---

**1.** Une écriture erronée a écrasé `gold.fact_order_line` il y a trois heures. L'équipe
veut revenir à l'état précédent.

- **A.** `ROLLBACK TABLE gold.fact_order_line`
- **B.** `RESTORE TABLE gold.fact_order_line TO VERSION AS OF 412`
- **C.** `UNDO LAST WRITE ON gold.fact_order_line`
- **D.** `REVERT TABLE gold.fact_order_line TO TIMESTAMP '2026-09-01 07:00:00'`

Réponse candidat : B  ✅

**2.** Une équipe reçoit environ 60 000 petits fichiers JSON par jour, déposés de façon
irrégulière, et le schéma évolue occasionnellement. Quelle méthode d'ingestion ?

- **A.** Auto Loader avec `schemaLocation` et colonne de sauvetage
- **B.** `COPY INTO` planifié toutes les heures
- **C.** `spark.read.json` avec écriture en écrasement
- **D.** Un connecteur Lakeflow Connect pointé sur le répertoire

Réponse candidat : A  ✅

**3.** `dfA.unionByName(dfB)` où `dfB` porte une colonne absente de `dfA`, sans autre
paramètre. Résultat ?

- **A.** Une erreur : les schémas ne correspondent pas
- **B.** La colonne est conservée, remplie de `NULL` pour `dfA`
- **C.** La colonne est silencieusement ignorée
- **D.** Les colonnes sont alignées par position

Réponse candidat : A  ✅

**4.** Une tâche de notification doit s'exécuter précisément lorsqu'une tâche amont a
échoué. Quel réglage ?

- **A.** `run_if: ALL_DONE`
- **B.** Une condition sur une valeur de tâche publiée par l'amont
- **C.** `run_if: AT_LEAST_ONE_FAILED`
- **D.** `max_retries: 0` sur la tâche de notification

Réponse candidat : C  ✅

**5.** Dans un bundle, `targets` désigne :

- **A.** Les tables de destination des pipelines déclarés
- **B.** Les environnements et leurs surcharges de variables
- **C.** Les catalogs Unity Catalog visés
- **D.** Les ressources sur lesquelles porte `bundle run`

Réponse candidat : B  ✅

**6.** Un traitement planifié passe en échec après douze minutes. L'interface d'exécution
du moteur n'affiche aucune requête pour cette exécution. Où chercher ?

- **A.** L'onglet des étapes de la dernière exécution réussie
- **B.** Le plan d'exécution de la requête principale
- **C.** L'historique des versions de la table cible
- **D.** Le journal d'événements du cluster

Réponse candidat : D  ✅

**7.** Sur quel objet est-il impossible de poser un masque de colonne ?

- **A.** Une vue
- **B.** Une table managée
- **C.** Une table externe
- **D.** Une table de streaming

Réponse candidat : A  ✅

**8.** `TRUNCATE TABLE bronze.commandes` est suivi d'un `COPY INTO` identique au
précédent. Combien de lignes sont chargées ?

- **A.** Toutes
- **B.** Seulement les nouvelles
- **C.** Aucune
- **D.** La commande échoue

Réponse candidat : B  ❌ — juste : **C**

> **Ton raté le plus tenace.** Il t'a déjà coûté deux questions au blanc n°1 (Q24 et Q33), il est dans tes *Termes à revoir* depuis le 31 juillet, tu l'as eu juste en salve ce matin, et tu me l'as **expliqué correctement à l'oral** il y a quatre heures. « Seulement les nouvelles » est la bonne réponse à *« que fait `COPY INTO` normalement »* — pas à *« que se passe-t-il après un `TRUNCATE` »*. L'historique des fichiers survit, donc aucun fichier n'est considéré comme neuf, donc **zéro ligne**. → `exam/fiche-ingestion.md`

**9.** Un incident a bloqué la chaîne trois jours. Une fois réparée, il faut retraiter les
trois journées manquantes sans toucher au code. Qu'est-ce qui rend ce rattrapage possible ?

- **A.** La période reçue en **paramètre**, plutôt que calculée depuis l'heure courante
- **B.** Une nouvelle tentative automatique configurée sur la tâche
- **C.** Un rafraîchissement complet du pipeline
- **D.** Un `COPY INTO` relancé avec l'option de forçage

Réponse candidat : A  ✅

**10.** Un job `for each` est configuré avec une concurrence de 6 sur une liste de 20
éléments. Combien de tâches s'exécutent simultanément ?

- **A.** 6
- **B.** 20
- **C.** 1
- **D.** Variable, selon la charge du cluster

Réponse candidat : A  ✅

**11.** Un compte dispose de `SELECT` sur `ventes.gold.faits` mais ses requêtes échouent.
Que manque-t-il le plus probablement ?

- **A.** `MODIFY` sur la table
- **B.** `OWNER` sur le schéma
- **C.** `BROWSE` sur le metastore
- **D.** `USE CATALOG` sur `ventes` et `USE SCHEMA` sur `gold`

Réponse candidat : D  ✅

**12.** `count(montant)` et `count(*)` rendent des résultats différents sur la même table.
Pourquoi ?

- **A.** `count(*)` ignore les lignes entièrement nulles
- **B.** `count(montant)` ne compte pas les valeurs nulles de cette colonne
- **C.** `count(montant)` déduplique les valeurs
- **D.** `count(*)` est approximatif sur les grandes tables

Réponse candidat : B  ✅

**13.** Le checkpoint d'un flux Auto Loader est supprimé, puis le traitement est relancé
sur le même répertoire.

- **A.** Seuls les fichiers déposés depuis la suppression sont traités
- **B.** L'état est reconstruit depuis les métadonnées de la table cible
- **C.** Le flux refuse de démarrer
- **D.** Tous les fichiers sont rechargés, y compris ceux déjà traités

Réponse candidat : D  ✅

**14.** Une équipe retire un pipeline du fichier de bundle, puis déploie. Que devient le
**notebook source** de ce pipeline ?

- **A.** Il est supprimé du workspace et du dépôt Git
- **B.** Il reste : ce n'est pas une ressource, mais la charge utile
- **C.** Il est archivé dans un dossier de sauvegarde
- **D.** Il est supprimé du workspace, conservé dans le dépôt

Réponse candidat : D  ❌ — juste : **B**

> Tu as retenu la moitié de ma réponse de ce matin : « conservé dans le dépôt » est juste. Mais le notebook n'est **pas non plus supprimé du workspace** — les fichiers sont synchronisés depuis ton projet, indépendamment des ressources déclarées. Seule la **ressource** (le pipeline, le job) disparaît. → `exam/fiche-cicd.md`, tableau de fin

**15.** Un tableau JSON vide doit produire une ligne dans le résultat, avec la colonne
issue du tableau à `NULL`. Quelle fonction ?

- **A.** `explode`
- **B.** `explode_outer`
- **C.** `posexplode`
- **D.** `flatten`

Réponse candidat : B  ✅

**16.** Une fonction de masquage est écrite ainsi : « si l'appelant n'appartient pas au
groupe `prestataires`, renvoyer la valeur en clair ». Qui voit la valeur en clair ?

- **A.** Tout compte hors du groupe `prestataires`, comptes de service compris
- **B.** Le personnel autorisé uniquement
- **C.** Personne : la fonction est correcte
- **D.** Les prestataires uniquement

Réponse candidat : A  ✅

**17.** Un flux d'événements arrive sur un bus Kafka. Comment l'ingérer ?

- **A.** Auto Loader pointé sur le répertoire de déversement
- **B.** `COPY INTO` avec le format `kafka`
- **C.** Un connecteur Lakeflow Connect dédié
- **D.** `spark.readStream.format("kafka")` avec un checkpoint

Réponse candidat : D  ✅

**18.** Une chaîne conditionne tous ses traitements à la présence du fichier de la veille.
Le fournisseur cesse de déposer pendant trois semaines. Que voit l'équipe chaque matin ?

- **A.** Le graphe en échec, avec notification
- **B.** Le graphe en attente
- **C.** Le graphe en vert, toutes tâches sautées, aucune alerte
- **D.** Le graphe en échec après expiration du délai

Réponse candidat : C  ✅

**19.** Le mode ANSI est actif. Une colonne bronze contient la valeur `abc`. On écrit
`CAST(col AS INT)`.

- **A.** `NULL`
- **B.** `0`
- **C.** Une exception qui interrompt la requête
- **D.** La chaîne est tronquée au premier caractère numérique

Réponse candidat : C  ✅

**20.** Une table de faits volumineuse et neuve sera filtrée surtout sur `id_client`,
dont la cardinalité dépasse trois millions. Quelle organisation physique ?

- **A.** `CLUSTER BY (id_client)`
- **B.** `PARTITIONED BY (id_client)`
- **C.** Un index sur `id_client`
- **D.** Aucune : la cardinalité est trop forte

Réponse candidat : A  ✅

**21.** Une dimension `dim_seller` doit conserver toutes les versions successives d'un
vendeur, avec leurs intervalles de validité. De quel type d'historisation s'agit-il ?

- **A.** SCD type 1
- **B.** SCD type 2
- **C.** SCD type 3
- **D.** Change Data Feed

Réponse candidat : B  ✅

**22.** `databricks bundle validate -t prod` est lancé. Que se passe-t-il dans le
workspace de production ?

- **A.** Les ressources sont créées mais laissées inactives
- **B.** Les planifications sont mises à jour, pas le code
- **C.** Un déploiement partiel a lieu, limité aux variables
- **D.** Rien : la commande résout, vérifie et affiche sans modifier

Réponse candidat : D  ✅

**23.** Une équipe active trois nouvelles tentatives automatiques sur toutes ses tâches.
Sur laquelle est-ce dangereux ?

- **A.** Une tâche qui remplace une tranche de dates par écrasement
- **B.** Une tâche de contrôle qui ne fait que lire
- **C.** Une tâche qui ajoute des lignes sans borne de progression
- **D.** Une tâche qui écrit dans une table temporaire

Réponse candidat : C  ✅

**24.** Une vue matérialisée calcule le chiffre d'affaires des 30 derniers jours avec
`WHERE jour >= current_date() - INTERVAL 30 DAYS`, rafraîchie chaque nuit. Huit mois plus
tard ?

- **A.** Le rafraîchissement échoue
- **B.** La fenêtre se décale silencieusement, sans erreur ni valeur aberrante
- **C.** Rien : la borne est réévaluée à chaque rafraîchissement
- **D.** Le rafraîchissement incrémental bascule en recalcul complet

Réponse candidat : B  ✅

**25.** Un compute serverless est demandé pour une requête interactive. Que ne peut-on
**pas** configurer ?

- **A.** Le catalog par défaut de la session
- **B.** Le type et le nombre d'instances
- **C.** Les paramètres de session Spark
- **D.** Le délai d'expiration de la requête

Réponse candidat : B  ✅

**26.** Une table `silver.client_scd2`, alimentée chaque nuit par `MERGE`, reçoit un
filtre de lignes imposé par la gouvernance. Que faut-il anticiper ?

- **A.** Le `MERGE` ne verra que les lignes visibles et créera des doublons
- **B.** Rien : les politiques s'évaluent à la lecture
- **C.** Le filtre s'appliquera aussi aux écritures
- **D.** La table peut devenir incompatible avec l'opération de fusion, ce qui casse la chaîne

Réponse candidat : D  ✅

**27.** Une organisation veut que toute colonne portant l'étiquette `pii` soit masquée, y
compris dans les tables créées demain. Quel dispositif ?

- **A.** Un masque posé colonne par colonne à la création
- **B.** Une revue trimestrielle avec application des masques manquants
- **C.** Une vue expurgée par schéma
- **D.** Une politique attachée au catalog, déclenchée par l'étiquette

Réponse candidat : D  ✅

**28.** Un carnet lit une valeur publiée par une tâche amont. Ouvert seul pour être
débogué, il échoue. Que manque-t-il ?

- **A.** Une valeur de repli dans l'appel de lecture
- **B.** Un widget portant le même nom que la clé
- **C.** Les droits sur le job amont
- **D.** Un appel au carnet amont pour produire la valeur

Réponse candidat : A  ✅

**29.** Une jointure gauche entre 1 000 lignes à gauche et une table de droite dont
**une** clé apparaît deux fois. Combien de lignes en sortie ?

- **A.** Exactement 1 000
- **B.** 2 000
- **C.** 1 001
- **D.** 999

Réponse candidat : C  ✅

**30.** Un mot de passe doit être utilisé par un job déployé par bundle. Où le placer ?

- **A.** Dans une variable de bundle, surchargée par target
- **B.** Dans un fichier `.env` ajouté au `.gitignore`
- **C.** Dans un coffre à secrets, dont seul l'emplacement figure dans le YAML
- **D.** Dans les `base_parameters` de la tâche

Réponse candidat : C  ✅

**31.** Une ligne JSON syntaxiquement invalide est rencontrée par un flux configuré avec
une colonne de sauvetage. Où atterrit-elle ?

- **A.** Dans la colonne de sauvetage
- **B.** Elle est ignorée sans trace
- **C.** Dans la colonne d'enregistrement corrompu
- **D.** Le flux échoue

Réponse candidat : C  ✅

**32.** Une déduplication par fenêtre sur `order_line_id` : deux lignes portent exactement
le même critère de tri. Quelle fonction garantit qu'il n'en subsiste qu'une ?

- **A.** `rank()`
- **B.** `dense_rank()`
- **C.** `first_value()`
- **D.** `row_number()`

Réponse candidat : D  ✅

**33.** Un traitement est passé de six à soixante-dix minutes en un trimestre, sans
changement de code ni de volume, et sans aucune erreur. Que chercher en priorité ?

- **A.** Des octets écrits sur disque dans les métriques d'étape
- **B.** Un conflit de bibliothèques au démarrage
- **C.** Une dérive des droits sur les tables lues
- **D.** Une modification du plan par l'optimiseur

Réponse candidat : A  ✅

**34.** Deux traitements se suivent : le second lit ce que le premier produit. Ils sont
planifiés à 5 h et 6 h. Le premier finit désormais à 6 h 10. Quelle correction
structurelle ?

- **A.** Avancer le premier à 4 h
- **B.** Un déclencheur par mise à jour de table sur le second
- **C.** Fusionner les deux dans un seul graphe
- **D.** Passer le second en mode continu

Réponse candidat : B  ✅

**35.** Auto Loader tourne avec un `schemaLocation` et **sans schéma explicite**. Une
nouvelle colonne apparaît dans les fichiers déposés. Comportement par défaut ?

- **A.** La colonne est ajoutée à la volée, sans interruption du flux
- **B.** La colonne est ignorée silencieusement
- **C.** La colonne part dans la colonne de sauvetage, le schéma ne bouge pas
- **D.** Le flux échoue, puis reprend avec le nouveau schéma au redémarrage

Réponse candidat : D  ✅

**36.** Qu'est-ce qui fait qu'un répertoire de fichiers Parquet devient une table Delta,
avec transactions et voyage dans le temps ?

- **A.** Un **journal de transactions** qui enregistre chaque écriture et fait autorité sur les fichiers valides
- **B.** Un index construit au moment de la création de la table
- **C.** L'enregistrement du chemin dans le metastore
- **D.** La présence d'un fichier de schéma à la racine du répertoire

Réponse candidat : A  ✅

**37.** Un fournisseur d'énergie déclare ses relevés en table de flux. La source révise
régulièrement des relevés anciens quand un compteur s'avère défaillant. Conséquence ?

- **A.** Les corrections sont intégrées au lot suivant
- **B.** Le pipeline échoue en détectant la modification
- **C.** La table est reconstruite automatiquement
- **D.** Les corrections ne sont jamais relues et les chiffres divergent

Réponse candidat : D  ✅

**38.** Quel est le grain de la table `fact_order_line` ?

- **A.** Une ligne par commande
- **B.** Une ligne par ligne de commande
- **C.** Une ligne par client et par jour
- **D.** Une ligne par produit

Réponse candidat : B  ✅

**39.** Une tâche attend un service externe indisponible depuis vendredi soir. Aucun
délai maximal n'est configuré. Que s'est-il passé jusqu'au lundi ?

- **A.** La tâche a échoué après une heure, valeur par défaut
- **B.** L'orchestrateur a libéré les ressources et remis la tâche en file
- **C.** Elle est restée en attente, en maintenant actives les ressources de calcul
- **D.** Elle a été sautée et le graphe s'est terminé en vert

Réponse candidat : C  ✅

**40.** Un `MERGE` SCD2 doit fermer une version et en insérer une autre pour la même clé.
Comment procède-t-on ?

- **A.** Deux `MERGE` successifs obligatoirement
- **B.** Un `UPDATE` puis un `INSERT` dans une transaction
- **C.** Une source où chaque changement apparaît deux fois, dont une avec une clé de correspondance nulle
- **D.** C'est impossible en une seule opération

Réponse candidat : C  ✅

**41.** Trois développeurs déploient le même bundle sur le même workspace. Qu'est-ce qui
les empêche de s'écraser mutuellement ?

- **A.** `mode: development`, qui préfixe les ressources par l'identité du déployeur
- **B.** Un verrouillage automatique des déploiements concurrents
- **C.** Une target distincte par personne
- **D.** Des catalogs différents surchargés par variable

Réponse candidat : A  ✅

**42.** Une sauvegarde réellement indépendante d'une table de 8 To est demandée avant une
migration. Quelle forme de copie ?

- **A.** `SHALLOW CLONE`
- **B.** Un export en fichiers hors de la plateforme
- **C.** `DEEP CLONE`
- **D.** Indifférent : les deux clones produisent un objet indépendant

Réponse candidat : C  ✅

**43.** Une ingestion par curseur `WHERE updated_at > dernier_watermark` tourne depuis un
an. Une ligne est supprimée en source. Que devient-elle dans la cible ?

- **A.** Elle reste indéfiniment : un curseur ne voit pas les suppressions
- **B.** La suppression est propagée au passage suivant
- **C.** Le curseur se bloque
- **D.** Elle est marquée obsolète automatiquement

Réponse candidat : A  ✅

**44.** Un traitement échoue en saturation mémoire **du pilote**. Quelle cause chercher en
premier ?

- **A.** Une table trop grosse diffusée dans une jointure
- **B.** Un déséquilibre de clé sur les exécuteurs
- **C.** Un débordement sur disque
- **D.** Une action qui rapatrie un gros résultat sur le pilote

Réponse candidat : D  ✅

**45.** Une équipe pose un masque sur `gold.client.email` et vérifie qu'un analyste non
autorisé ne voit plus l'adresse. L'audit conclut pourtant à un défaut. Pourquoi ?

- **A.** Le masque n'a pas été propagé aux vues dérivées
- **B.** Les mêmes analystes ont `SELECT` sur `silver.client`, où l'adresse est en clair
- **C.** La fonction n'a pas été testée avec un compte autorisé
- **D.** Le masque ne s'applique pas lors d'un export

Réponse candidat : B  ✅

---

<details>
<summary><b>Corrigé — ne déplier qu'après avoir répondu aux 45 questions</b></summary>

| # | Rép. | Section | Pourquoi |
|---|---|---|---|
| 1 | B | 1 | `RESTORE TABLE … TO VERSION AS OF` est la seule commande qui existe |
| 2 | A | 2 | Beaucoup de fichiers, arrivées irrégulières, schéma mouvant : Auto Loader |
| 3 | A | 3 | Sans `allowMissingColumns=True`, `unionByName` **lève** |
| 4 | C | 4 | `depends_on` = « après le succès de ». Réagir à un échec exige `run_if` |
| 5 | B | 5 | `targets` = les environnements, jamais les tables cibles |
| 6 | D | 6 | Aucune application n'a démarré : la cause est en amont du moteur |
| 7 | A | 7 | Ni masque ni filtre sur une vue — elle hérite des politiques des tables |
| 8 | C | 2 | `TRUNCATE` ne remet pas l'historique des fichiers chargés |
| 9 | A | 2 | Un traitement qui calcule lui-même « hier » ne sait faire qu'aujourd'hui. La date descend par paramètre |
| 10 | A | 4 | La concurrence configurée est la réponse |
| 11 | D | 7 | La traversée doit être accordée à chaque niveau supérieur |
| 12 | B | 3 | `count(colonne)` ignore les nulls ; l'écart les mesure |
| 13 | D | 2 | L'état vit dans le checkpoint : l'effacer remet le compteur à zéro |
| 14 | B | 5 | Le fichier source n'est pas une ressource. Seule la ressource est supprimée |
| 15 | B | 3 | `explode_outer` conserve les tableaux vides ou nuls |
| 16 | A | 7 | Formulation ouverte par défaut : tout ce qui n'est pas prévu voit en clair |
| 17 | D | 2 | Un bus a une source Spark native ; ni fichier, ni connecteur managé |
| 18 | C | 4 | Tâche sautée ≠ tâche échouée : le graphe finit en vert |
| 19 | C | 3 | Sous ANSI, un `cast` impossible **lève**. Parade : `try_cast` |
| 20 | A | 6 | Le partitionnement crée un répertoire par valeur ; `CLUSTER BY` non |
| 21 | B | 3 | SCD2 : versions successives avec intervalles de validité |
| 22 | D | 5 | `validate` résout, vérifie et affiche — sans rien modifier |
| 23 | C | 4 | Une tâche qui ajoute sans borne est doublée par un réessai |
| 24 | B | 3 | La borne est figée au calcul ; la fenêtre glisse en silence |
| 25 | B | 1 | Serverless : aucun choix d'instance |
| 26 | D | 7 | Une politique peut rendre la table incompatible avec la fusion |
| 27 | D | 7 | ABAC : politique attachée au catalog, déclenchée par étiquette |
| 28 | A | 4 | Sans valeur de repli, le carnet ne s'exécute plus hors du graphe |
| 29 | C | 3 | Une seule ligne de gauche est dupliquée : 1 001, pas 2 000 |
| 30 | C | 5 | Coffre à secrets ; le YAML n'en porte que l'emplacement |
| 31 | C | 2 | Échec d'analyse → `_corrupt_record`. La sauvegarde capte les écarts au schéma |
| 32 | D | 3 | Seul `row_number` départage des lignes à critère de tri identique |
| 33 | A | 6 | Débordement : ne lève aucune erreur, se lit dans les métriques d'étape |
| 34 | B | 4 | Le second démarre quand le premier a produit, quelle que soit l'heure |
| 35 | D | 2 | `addNewColumns` est le défaut **sans** schéma explicite : l'échec est volontaire, la reprise récupère la colonne |
| 36 | A | 1 | Le journal de transactions : il fait autorité sur les fichiers valides, d'où transactions et voyage dans le temps |
| 37 | D | 2 | Une table de flux ne relit jamais le passé |
| 38 | B | 3 | Le grain se lit dans le nom : une ligne par ligne de commande |
| 39 | C | 4 | Sans délai maximal, rien n'arrête une tâche bloquée, et le calcul se facture |
| 40 | C | 3 | Le motif en deux temps, avec une clé de correspondance nulle pour l'insertion |
| 41 | A | 5 | Le préfixage automatique du mode développement |
| 42 | C | 7 | Seule la copie profonde recopie les fichiers et protège réellement |
| 43 | A | 2 | Une ligne supprimée n'a plus d'`updated_at` à comparer |
| 44 | D | 6 | Saturation du pilote = rapatriement d'un gros résultat |
| 45 | B | 7 | Un masque ne protège rien si la table amont reste lisible |

### Grille de lecture

| Score | Interprétation |
|---|---|
| 38+ / 45 | Bien placé |
| 32–37 | Correct. Identifie les deux sections les plus faibles |
| < 32 | Reprends les fiches correspondantes avant l'examen |

Ces seuils sont indicatifs. Un examen blanc généré n'est pas une mesure calibrée.

</details>
