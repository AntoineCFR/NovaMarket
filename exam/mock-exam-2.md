# Examen blanc n°2

**45 questions · 90 minutes · aucune aide autorisée**

Même répartition officielle que le n°1 : 3 · 9 · 10 · 7 · 5 · 4 · 7. Questions
entièrement différentes.

À passer **après** avoir corrigé le n°1 et repris les objectifs ratés — pas le même jour.

---

**1.** Une équipe doit revenir à l'état d'une table tel qu'il était hier à 14 h. Quelle
approche ?

- **A.** `ALTER TABLE t ROLLBACK`
- **B.** `RESTORE TABLE t TO TIMESTAMP AS OF '...'`
- **C.** Rejouer le pipeline complet
- **D.** Restaurer une sauvegarde externe

**2.** Deux jobs écrivent simultanément dans la même table Delta. Que se passe-t-il ?

- **A.** Les écritures sont mises en file et attendent
- **B.** Contrôle de concurrence optimiste : l'écriture conflictuelle échoue et doit être rejouée
- **C.** Les données sont fusionnées automatiquement
- **D.** La table est corrompue

**3.** Une charge ETL planifiée tourne sur un all-purpose cluster. Principal argument pour
la basculer sur du job compute ?

- **A.** Tarif DBU nettement inférieur pour une charge éphémère
- **B.** Plus de bibliothèques disponibles
- **C.** Meilleures performances
- **D.** Meilleure gouvernance

**4.** Auto Loader est configuré en `rescue`. Un fichier arrive avec deux colonnes
inconnues. Que se passe-t-il ?

- **A.** Les colonnes sont ignorées
- **B.** Le flux échoue
- **C.** Leur contenu part dans la colonne de sauvetage, sans interruption
- **D.** Les colonnes sont ajoutées à la table

**5.** Quelle méthode d'ingestion convient pour un flux Kafka ?

- **A.** `COPY INTO`
- **B.** Structured Streaming directement sur la source
- **C.** Auto Loader
- **D.** Un connecteur managé

**6.** Un pipeline lit un export quotidien complet d'un référentiel. Quel motif d'écriture ?

- **A.** `append`
- **B.** `overwrite` — c'est un instantané, pas un delta
- **C.** Auto Loader en streaming
- **D.** `MERGE` sur la clé

**7.** Quel est l'inconvénient principal d'un `overwrite` sur un instantané de référentiel ?

- **A.** La perte de l'historique des valeurs antérieures
- **B.** Le coût de stockage
- **C.** La lenteur
- **D.** L'impossibilité de le planifier

**8.** Une colonne `prix` contient `"14,90 €"`, `" 14,90"` avec une espace insécable, et
`"14.90"`. Quelle stratégie de nettoyage est la plus robuste ?

- **A.** Une liste blanche : ne garder que chiffres, virgule, point et signe
- **B.** `trim()` puis `replace(",", ".")`
- **C.** `cast("double")` direct
- **D.** `regexp_replace("€", "")`

**9.** Pourquoi une liste blanche vaut-elle mieux qu'une liste noire pour ce nettoyage ?

- **A.** Elle est plus rapide
- **B.** Elle est plus lisible
- **C.** Elle préserve les accents
- **D.** On ne peut pas énumérer les caractères qu'on n'a pas encore vus

**10.** Un fichier CSV est encodé en `windows-1252`, lu par défaut en UTF-8. Symptôme ?

- **A.** Caractères accentués remplacés par des caractères de substitution
- **B.** Échec de lecture
- **C.** Colonnes décalées
- **D.** Lignes manquantes

**11.** Une adresse contient le séparateur du CSV, non échappé. Avec une colonne de
sauvetage active, que se passe-t-il ?

- **A.** Les colonnes suivantes sont décalées silencieusement
- **B.** Les jetons excédentaires vont dans la colonne de sauvetage, la ligne est conservée
- **C.** La ligne est rejetée
- **D.** La lecture échoue

**12.** Quelle est la bonne pratique pour les lignes invalides en couche silver ?

- **A.** Les supprimer par un `WHERE`
- **B.** Les corriger automatiquement
- **C.** Les écarter vers une table de quarantaine, avec la donnée brute et le motif
- **D.** Les laisser passer avec des nulls

**13.** Un `expect_or_drop` dans un pipeline déclaratif : que deviennent les lignes en échec ?

- **A.** Comptées dans les métriques, mais **écartées** — elles ne sont pas conservées
- **B.** Le pipeline échoue
- **C.** Conservées avec un drapeau
- **D.** Écrites dans une table d'erreurs automatique

**14.** Quelle jointure compte les lignes de gauche sans correspondance à droite, sans
ramener les colonnes de droite ?

- **A.** `left_anti`
- **B.** `full_outer`
- **C.** `left_semi`
- **D.** `inner`

**15.** `df.summary()` par rapport à `df.describe()` :

- **A.** identiques
- **B.** `describe()` est plus complet
- **C.** `summary()` ajoute les quartiles et accepte des percentiles personnalisés
- **D.** `summary()` ne marche que sur les colonnes numériques

**16.** Une table de dimension SCD2 est jointe à un fait sans condition temporelle. Quel
est le risque ?

- **A.** Seule la version courante est retenue
- **B.** Aucune ligne ne remonte
- **C.** La jointure échoue
- **D.** Chaque ligne de fait est dupliquée pour chaque version de la dimension

**17.** Dans une convention `[valid_from, valid_to)`, une transaction datée exactement de
`valid_to` appartient à…

- **A.** les deux
- **B.** la version qui se ferme
- **C.** aucune des deux
- **D.** la version suivante

**18.** Quel est le principal argument en faveur de `valid_to` à `NULL` plutôt qu'à une
date sentinelle ?

- **A.** L'honnêteté : on ne sait pas quand la version cessera d'être valide
- **B.** Le gain de stockage
- **C.** La performance des jointures
- **D.** La compatibilité SQL

**19.** Un `MERGE` SCD2 doit fermer une version et en insérer une autre pour la même clé.
Comment procède-t-on ?

- **A.** Deux `MERGE` successifs obligatoirement
- **B.** Une source où chaque changement apparaît deux fois, dont une avec une clé de correspondance nulle
- **C.** C'est impossible
- **D.** Un `UPDATE` suivi d'un `INSERT` dans une transaction

**20.** Que fait `spark.sql.shuffle.partitions` ?

- **A.** Fixe le nombre de partitions après un *shuffle*
- **B.** Fixe le nombre de fichiers écrits
- **C.** Contrôle la diffusion des petites tables
- **D.** Limite la mémoire par exécuteur

**21.** Un job doit tourner tous les jours à 4 h, et l'équipe veut savoir quand les
chiffres bougent. Quel déclencheur ?

- **A.** Continu
- **B.** Programmé
- **C.** Arrivée de fichier
- **D.** Mise à jour de table

**22.** Quel est l'avantage sous-estimé d'un déclencheur programmé ?

- **A.** Il est plus rapide
- **B.** Il ne consomme pas de compute
- **C.** Il est prévisible : on sait quand intervenir sans gêner
- **D.** Il est moins cher

**23.** Une tâche de job échoue systématiquement à la première tentative et réussit à la
seconde. Quelle cause probable dans un contexte Auto Loader ?

- **A.** Une évolution de schéma en mode `addNewColumns`
- **B.** Un conflit de bibliothèques
- **C.** Un problème réseau
- **D.** Un manque de mémoire

**24.** Un DAG de job a besoin de 8 branches parallèles, mais le compte est limité à 5
tâches concurrentes. Que se passe-t-il ?

- **A.** Le DAG est refusé au déploiement
- **B.** Les tâches s'exécutent par vagues, dans la limite du plafond
- **C.** Le job échoue
- **D.** Trois tâches sont ignorées

**25.** Comment un job peut-il échouer explicitement quand un contrôle qualité est au rouge ?

- **A.** Une condition suffit
- **B.** Ce n'est pas possible
- **C.** Une tâche sur la branche `false` qui lève une exception
- **D.** Un timeout court

**26.** Quel type de tâche permet d'exécuter une logique packagée dans une bibliothèque
versionnée plutôt que dans un notebook ?

- **A.** Tâche de tableau de bord
- **B.** Tâche conditionnelle
- **C.** Tâche fichier Python ou JAR
- **D.** Tâche de requête SQL

**27.** Une ressource est retirée du YAML d'un bundle, puis `deploy` est relancé. Résultat ?

- **A.** Le déploiement échoue
- **B.** La ressource est archivée
- **C.** La ressource reste
- **D.** La ressource est supprimée du workspace

**28.** Comment simuler deux environnements avec un seul workspace ?

- **A.** Deux targets surchargeant une variable de catalog
- **B.** C'est impossible
- **C.** Deux utilisateurs
- **D.** Deux dépôts Git

**29.** Qu'est-ce que deux targets sur un même workspace ne testent pas ?

- **A.** L'isolation des permissions et la séparation des identités
- **B.** La résolution des variables
- **C.** Le mode de déploiement
- **D.** La syntaxe du YAML

**30.** Un notebook a des modifications non commitées dans un Git Folder. Le développeur
change de branche. Risque ?

- **A.** Suppression du dossier
- **B.** Aucun
- **C.** Fusion automatique
- **D.** Perte des modifications non commitées

**31.** Quelle commande CLI supprime les ressources déployées par un bundle ?

- **A.** `databricks workspace delete`
- **B.** `databricks bundle destroy`
- **C.** `databricks bundle remove`
- **D.** `databricks bundle clean`

**32.** Comment repérer une dérive de performance d'un job dans le temps ?

- **A.** `DESCRIBE HISTORY`
- **B.** Le Spark UI de la dernière exécution
- **C.** L'historique des exécutions dans l'interface Lakeflow Jobs
- **D.** Les journaux du cluster

**33.** Un stage montre un *shuffle write* important suivi d'un *shuffle read* important.
Que peut-on en dire ?

- **A.** C'est un *spill*
- **B.** C'est un *skew*
- **C.** C'est anormal, il faut le supprimer
- **D.** Les données traversent le réseau pour être regroupées — attendu sur un `groupBy` ou une jointure large, mais coûteux

**34.** Sur quoi mesurer objectivement l'effet d'un regroupement liquide ?

- **A.** Le nombre de partitions
- **B.** La taille de la table
- **C.** La durée d'exécution
- **D.** Le nombre de fichiers réellement lus

**35.** Qu'est-ce que la *predictive optimization* prend en charge ?

- **A.** La prédiction du coût des requêtes
- **B.** Le déclenchement automatique d'`OPTIMIZE` et de `VACUUM` sur les tables managées
- **C.** La mise en cache des résultats
- **D.** Le choix des jointures

**36.** Quelle est la différence entre une table managée et une table externe du point de
vue de la maintenance ?

- **A.** Aucune
- **B.** L'optimisation automatique ne s'applique qu'aux tables managées
- **C.** La table managée exige un `VACUUM` manuel
- **D.** La table externe est compactée plus souvent

**37.** Comment convertir une table externe en table managée ?

- **A.** Recréer la table et recopier les données
- **B.** `CONVERT TO DELTA`
- **C.** Ce n'est pas possible
- **D.** `ALTER TABLE t SET MANAGED`

**38.** Quel privilège permet de traverser un schéma sans pouvoir lire ses tables ?

- **A.** `BROWSE`
- **B.** `SELECT`
- **C.** `MODIFY`
- **D.** `USE SCHEMA`

**39.** Un `REVOKE SELECT ON SCHEMA` est exécuté. Que devient le `USE SCHEMA` accordé
précédemment ?

- **A.** Il expire au bout de 24 h
- **B.** Il devient un `DENY`
- **C.** Il reste : révoquer un privilège ne touche pas les autres
- **D.** Il est révoqué aussi

**40.** Une table alimentée par `MERGE` reçoit un masque de colonne. Risque ?

- **A.** Le masque est ignoré
- **B.** La table passe en lecture seule
- **C.** Le `MERGE` peut ne plus être supporté, ce qui casse le pipeline
- **D.** Aucun

**41.** Une politique de sécurité pilotée par une table de correspondance. Quel est le
défaut le plus fréquent ?

- **A.** La table de pilotage est modifiable par ceux-là mêmes qu'elle est censée restreindre
- **B.** La lenteur des jointures
- **C.** L'impossibilité de la sauvegarder
- **D.** Le coût de lecture

**42.** Quel est l'intérêt d'un `DENY` par rapport à l'absence de `GRANT` ?

- **A.** Aucun
- **B.** Il est temporaire
- **C.** Il perce un trou dans un octroi large, neutralise un héritage, et documente une interdiction
- **D.** Il s'applique aux propriétaires

**43.** Une couche gold est publiée avant que les contrôles qualité ne s'exécutent, et une
condition bloque seulement la tâche de publication du bilan. Quel est le problème ?

- **A.** Le job échoue inutilement
- **B.** Les contrôles sont trop lents
- **C.** Les analystes lisent déjà le gold : la barrière n'en est pas une
- **D.** Aucun

**44.** Quelle est la bonne réponse à un contrôle qualité qui échoue sur un invariant
comme « aucune clé dupliquée » ?

- **A.** Alerter et continuer
- **B.** Ignorer si le taux est faible
- **C.** Corriger automatiquement
- **D.** Arrêter le pipeline : un invariant violé signifie que le code est faux

**45.** Un contrôle de volumétrie sur un référentiel est écrit `row_count >= 1`. Le
référentiel est re-livré avec 5 % de ses lignes. Que fait le contrôle ?

- **A.** Il échoue
- **B.** Il bloque le pipeline
- **C.** Il avertit
- **D.** Il passe au vert : il vérifiait la non-vacuité, pas la plausibilité

---

<details>
<summary><b>Corrigé — ne déplier qu'après avoir répondu aux 45 questions</b></summary>

| # | Rép. | Section | Pourquoi |
|---|---|---|---|
| 1 | B | 1 | Le *time travel* Delta accepte une version ou un horodatage |
| 2 | B | 1 | Concurrence optimiste : la seconde écriture échoue et se rejoue |
| 3 | A | 1 | Le tarif DBU du job compute est nettement inférieur |
| 4 | C | 2 | `rescue` n'interrompt pas : le contenu inconnu va au sauvetage |
| 5 | B | 2 | Un bus de messages se lit directement en Structured Streaming |
| 6 | B | 2 | Un instantané complet se remplace, il ne s'empile pas |
| 7 | A | 2 | L'`overwrite` écrase l'état antérieur ; le *time travel* Delta atténue |
| 8 | A | 3 | Liste blanche : on ne garde que ce qu'on sait lire |
| 9 | D | 3 | Une liste noire ne peut pas couvrir ce qu'on n'a pas encore vu — l'espace insécable en est l'exemple type |
| 10 | A | 2 | Les octets invalides deviennent des caractères de substitution, sans erreur |
| 11 | B | 2 | Les jetons excédentaires sont sauvés, la ligne reste |
| 12 | C | 3 | Une quarantaine explicite, avec donnée brute et motif, permet le rejeu |
| 13 | A | 3 | `expect_or_drop` compte et écarte : il ne conserve rien |
| 14 | A | 3 | `left_anti` |
| 15 | C | 3 | `summary()` ajoute les quartiles |
| 16 | D | 3 | Sans condition temporelle, chaque version se joint à chaque fait |
| 17 | D | 3 | Borne haute exclue : elle appartient à la version suivante |
| 18 | A | 3 | Une date sentinelle affirme une fin qui n'existe pas |
| 19 | B | 3 | Le motif en deux temps, avec une clé de correspondance nulle pour l'insertion |
| 20 | A | 3 | Nombre de partitions après *shuffle* |
| 21 | B | 4 | Régularité + prévisibilité = programmé |
| 22 | C | 4 | On sait quand le pipeline tourne, donc quand intervenir |
| 23 | A | 4 | `addNewColumns` échoue volontairement puis apprend le schéma |
| 24 | B | 4 | Le plafond de concurrence sérialise, il ne fait pas échouer |
| 25 | C | 4 | Sans exception levée, un job aux tâches *skipped* finit en succès |
| 26 | C | 4 | Tâche fichier Python ou JAR |
| 27 | D | 5 | Le YAML fait autorité : il décrit un état souhaité |
| 28 | A | 5 | Deux targets, une variable de catalog surchargée |
| 29 | A | 5 | Ni l'isolation des permissions ni la séparation des identités |
| 30 | D | 5 | Même logique que Git en ligne de commande |
| 31 | B | 5 | `databricks bundle destroy` |
| 32 | C | 6 | L'historique des exécutions donne les durées successives |
| 33 | D | 6 | Un *shuffle* est attendu sur une agrégation large, mais coûteux |
| 34 | D | 6 | Le gain vient de l'élagage de fichiers, pas de l'horloge |
| 35 | B | 6 | `OPTIMIZE` et `VACUUM` automatiques sur les tables managées |
| 36 | B | 7 | L'optimisation automatique ne couvre que les tables managées |
| 37 | D | 7 | `ALTER TABLE ... SET MANAGED`, sans réécrire les données |
| 38 | D | 7 | `USE SCHEMA` permet de traverser sans lire — d'où l'intérêt de séparer les deux |
| 39 | C | 7 | Révoquer un privilège ne touche pas les autres |
| 40 | C | 7 | Les politiques de masquage restreignent le `MERGE` dans plusieurs cas |
| 41 | A | 7 | Une porte blindée avec la clé sur la serrure |
| 42 | C | 7 | Trois usages : percer, neutraliser un héritage, documenter |
| 43 | C | 4 | Une barrière placée après la production ne bloque rien — elle constate |
| 44 | D | 6 | Un invariant violé signifie que le code est faux : continuer propage l'erreur |
| 45 | D | 6 | Le seuil vérifiait la non-vacuité, pas la plausibilité. Un seuil relatif l'aurait attrapé |

### Grille de lecture

Identique au n°1. Compare surtout ta **progression par section** entre les deux examens :
c'est elle qui dit si tes révisions ont porté, pas le score global.

</details>
