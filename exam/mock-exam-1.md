# Examen blanc n°1

**45 questions · 90 minutes · aucune aide autorisée**

Répartition conforme au guide officiel : plateforme 3 · ingestion 9 · transformation 10 ·
jobs 7 · CI/CD 5 · diagnostic 4 · gouvernance 7. Les questions sont mélangées, comme à
l'examen.

Chronomètre-toi. L'endurance est un objectif en soi : 45 questions de mise en situation
en 90 minutes, c'est deux minutes par question, sans pause.

---

**1.** Un pipeline lit des fichiers CSV déposés une fois par jour dans un volume, une
douzaine de fichiers. L'équipe travaille exclusivement en SQL. Quelle méthode d'ingestion ?

- **A.** Auto Loader dans un notebook PySpark
- **B.** `COPY INTO` dans une tâche de requête SQL
- **C.** `spark.read.csv()` avec `overwrite`
- **D.** Un connecteur managé Lakeflow Connect

**2.** Quel est l'ordre correct de la hiérarchie Unity Catalog ?

- **A.** workspace → catalog → schema → table
- **B.** account → catalog → database → table
- **C.** metastore → catalog → schema → table
- **D.** catalog → metastore → schema → table

**3.** `dfA.union(dfB)` où B a les mêmes colonnes dans un ordre différent, types
compatibles. Résultat ?

- **A.** Données fausses, sans erreur
- **B.** DataFrame vide
- **C.** Erreur de schéma
- **D.** Alignement automatique sur les noms

**4.** Une tâche de job doit transmettre un statut à une tâche conditionnelle en aval.
Quel mécanisme ?

- **A.** Une variable globale
- **B.** Un fichier dans le volume
- **C.** `dbutils.jobs.taskValues`
- **D.** Une variable d'environnement

**5.** `databricks bundle deploy` est lancé deux fois sans changement. Résultat ?

- **A.** Ressources dupliquées
- **B.** Workspace inchangé, le déploiement est idempotent
- **C.** Une version empilée par ressource
- **D.** Erreur de conflit

**6.** Un stage montre une médiane de tâche à 20 s et un maximum à 12 min, avec un
*shuffle read* max vingt fois la médiane. Diagnostic ?

- **A.** Trop de partitions
- **B.** Réseau saturé
- **C.** *Data skew*
- **D.** Manque de mémoire driver

**7.** Un utilisateur a `GRANT SELECT ON SCHEMA` mais ne voit aucune table. Cause la plus
probable ?

- **A.** Il faut `MODIFY`
- **B.** Les tables sont externes
- **C.** Propagation en cours
- **D.** `USE CATALOG` ou `USE SCHEMA` manquant

**8.** Auto Loader en `schemaEvolutionMode = "none"` reçoit un fichier avec une colonne
supplémentaire. Que devient cette colonne ?

- **A.** Le flux échoue
- **B.** Placée dans `_rescued_data`
- **C.** Ignorée silencieusement et définitivement
- **D.** Ajoutée à la table

**9.** Quel objet gold garantit d'être toujours à jour sans aucun rafraîchissement ?

- **A.** Une table
- **B.** Une vue
- **C.** Une vue matérialisée
- **D.** Une table de streaming

**10.** Un job doit démarrer dès qu'une table Delta est mise à jour par un autre job.
Quel déclencheur ?

- **A.** Mise à jour de table
- **B.** Programmé
- **C.** Continu
- **D.** Arrivée de fichier

**11.** Dans un bundle, à quoi sert le bloc `targets` ?

- **A.** Déclarer les tables cibles
- **B.** Lister les destinataires de notifications
- **C.** Définir les environnements et leurs surcharges de variables
- **D.** Configurer les clusters

**12.** Une fonction de masquage interroge une table de correspondance. L'appelant en est
absent. Comportement d'une fonction correctement écrite ?

- **A.** Erreur
- **B.** Valeur masquée
- **C.** Ligne exclue
- **D.** Valeur en clair

**13.** `count("*")` renvoie 1 000, `count("email")` renvoie 850. Que peut-on en déduire ?

- **A.** 150 valeurs nulles dans `email`
- **B.** 850 lignes valides et 150 en quarantaine
- **C.** 150 doublons
- **D.** Une erreur de type

**14.** Quelle méthode d'ingestion se dégrade en premier sur un répertoire d'un million de
fichiers ?

- **A.** `COPY INTO`
- **B.** Auto Loader en mode notification
- **C.** Auto Loader en *directory listing*
- **D.** Les trois de la même façon

**15.** Une table de faits de 50 millions de lignes est jointe à une dimension de 300
lignes. Quelle stratégie évite le *shuffle* du gros côté ?

- **A.** Trier les deux tables
- **B.** Augmenter `spark.sql.shuffle.partitions`
- **C.** `cross join` filtré
- **D.** Jointure diffusée de la dimension

**16.** Un `DENY SELECT` est posé sur une table par son propriétaire, à un groupe dont il
est membre. Peut-il lire la table ?

- **A.** Oui, le propriétaire n'y est pas soumis
- **B.** Non
- **C.** Seulement en SQL
- **D.** Seulement après `REVOKE`

**17.** Que fait `databricks bundle validate` ?

- **A.** Supprime les ressources orphelines
- **B.** Lance les tests unitaires
- **C.** Déploie en mode simulation
- **D.** Résout les variables et vérifie la configuration, sans rien modifier

**18.** Une tâche conditionnelle est fausse. Les tâches de la branche `true` sont…

- **A.** exécutées quand même
- **B.** en échec
- **C.** relancées
- **D.** *skipped*, et le job peut finir en succès

**19.** Quelle commande revient à une version antérieure d'une table Delta ?

- **A.** `RESTORE TABLE t TO VERSION AS OF n`
- **B.** `ROLLBACK`
- **C.** `ALTER TABLE t UNDO`
- **D.** `REVERT TABLE t`

**20.** `explode` contre `explode_outer` : quelle différence ?

- **A.** Aucune
- **B.** `explode` ne marche que sur les `MAP`
- **C.** `explode_outer` conserve les lignes à tableau vide ou nul
- **D.** `explode_outer` conserve l'indice

**21.** Un connecteur managé Lakeflow Connect apporte quoi qu'un script JDBC n'apporte pas ?

- **A.** De meilleures performances de lecture
- **B.** La gestion de l'état de progression, du CDC et du schéma, sans code à maintenir
- **C.** Un format de stockage propriétaire
- **D.** Le chiffrement des données

**22.** Sur quel objet est-il impossible de poser un filtre de lignes ?

- **A.** Une vue
- **B.** Une table managée
- **C.** Une table de streaming
- **D.** Une table externe

**23.** Quelle recommandation actuelle pour optimiser les filtres sur une nouvelle table
volumineuse ?

- **A.** Un index
- **B.** `CLUSTER BY`
- **C.** `PARTITIONED BY`
- **D.** `OPTIMIZE ... ZORDER BY`

**24.** Un `TRUNCATE TABLE` est suivi d'un `COPY INTO` identique au précédent. Combien de
lignes sont chargées ?

- **A.** La commande échoue
- **B.** Toutes
- **C.** Aucune
- **D.** Seulement les nouvelles

**25.** Une jointure gauche avec une table de droite ayant deux lignes pour une clé…

- **A.** duplique la ligne de gauche concernée
- **B.** échoue
- **C.** supprime la ligne ambiguë
- **D.** conserve le nombre de lignes de gauche

**26.** En mode de déploiement `development`, que fait le bundle automatiquement ?

- **A.** Désactive les notifications
- **B.** Réduit la taille des clusters
- **C.** Active le mode debug
- **D.** Préfixe les ressources et suspend les planifications

**27.** Un `collect()` sur 80 millions de lignes échoue. Où est la saturation ?

- **A.** Stockage
- **B.** Driver
- **C.** Exécuteurs
- **D.** Metastore

**28.** Quel privilège manque pour créer une table dans un schéma sur lequel on a déjà
`USE SCHEMA` ?

- **A.** `CREATE TABLE`
- **B.** `SELECT`
- **C.** `MODIFY`
- **D.** `USE CATALOG`

**29.** Un fichier JSON contient des objets imbriqués. Que fait une couche bronze fidèle ?

- **A.** Les sérialise en chaîne
- **B.** Les conserve en `STRUCT` et `ARRAY`
- **C.** Les rejette
- **D.** Les aplatit

**30.** Quel type de tâche de job rafraîchit un tableau de bord AI/BI ?

- **A.** Tâche de requête SQL
- **B.** Tâche de pipeline
- **C.** Notebook avec appel API
- **D.** Tâche de type tableau de bord

**31.** `approx_count_distinct` par rapport à `countDistinct` :

- **A.** ne fonctionne que sur les entiers
- **B.** plus précis
- **C.** identique
- **D.** approximatif, avec un coût nettement inférieur

**32.** Une extraction incrémentale filtre `updated_at > watermark`. Quel risque ?

- **A.** Perte silencieuse des lignes validées à un horodatage ≤ watermark après la lecture
- **B.** Watermark qui recule
- **C.** Duplication à chaque exécution
- **D.** Aucun

**33.** Où vit l'état de progression de `COPY INTO` ?

- **A.** Un répertoire de checkpoint désigné par l'utilisateur
- **B.** Le metastore
- **C.** Nulle part
- **D.** Les métadonnées de la table cible

**34.** Un job sans timeout a une tâche bloquée. Conséquence ?

- **A.** Échec au bout de dix minutes
- **B.** Relance automatique
- **C.** Arrêt après une heure par défaut
- **D.** Consommation de compute jusqu'à intervention

**35.** Que signifient des colonnes *Spill (disk)* non nulles dans un stage ?

- **A.** Des données corrompues
- **B.** Une partition qui ne tenait pas en mémoire, passée par le disque — ralentissement silencieux
- **C.** Une erreur mémoire
- **D.** Un checkpoint écrit

**36.** Une politique ABAC s'attache à quoi ?

- **A.** Un utilisateur
- **B.** Une colonne
- **C.** Un catalog ou un schéma, et s'applique par étiquette
- **D.** Un job

**37.** Deux développeurs déploient le même bundle en `development` dans le même
workspace. Résultat ?

- **A.** Conflit et échec
- **B.** Chacun a ses ressources, préfixées par son identité
- **C.** Le second écrase le premier
- **D.** Fusion des ressources

**38.** Quel objet gold pour une requête coûteuse, relue souvent, avec une fraîcheur à la
journée suffisante ?

- **A.** Table recréée à chaque lecture
- **B.** Vue
- **C.** Table de streaming
- **D.** Vue matérialisée

**39.** Une source supprime physiquement ses lignes. Conséquence pour un watermark sur
`updated_at` ?

- **A.** Lignes supprimées jamais remontées, conservées indéfiniment en cible
- **B.** Suppressions détectées
- **C.** Échec de l'extraction
- **D.** Watermark négatif

**40.** `DROP TABLE` sur une table externe supprime…

- **A.** uniquement les fichiers
- **B.** uniquement l'entrée du catalogue
- **C.** rien sans `PURGE`
- **D.** les fichiers et l'entrée du catalogue

**41.** Le meilleur levier face à une tâche lente, en rapport coût/bénéfice ?

- **A.** Agrandir le cluster
- **B.** Passer en continu
- **C.** Réduire le volume lu
- **D.** Ajouter des nouvelles tentatives

**42.** Une tâche `for each` avec une concurrence de 4 sur une liste de 12 éléments : combien
de tâches simultanées ?

- **A.** Variable
- **B.** 1
- **C.** 4
- **D.** 12

**43.** Où stocker un mot de passe utilisé par un job déployé par bundle ?

- **A.** Variable de bundle
- **B.** Fichier `.env` commité
- **C.** `base_parameters`
- **D.** Scope de secrets, référencé dans le YAML

**44.** Quelle affirmation sur le compute serverless est exacte ?

- **A.** On choisit le type d'instance
- **B.** Réservé au machine learning
- **C.** Démarrage rapide, facturation à l'usage, aucune inactivité facturée
- **D.** Exige un cluster préexistant

**45.** Un masque est posé sur `gold.dim_customer.email`, mais `silver.customer_scd2` reste
lisible par les mêmes utilisateurs. Portée réelle de la protection ?

- **A.** Nulle en pratique : la donnée est lisible une couche plus haut
- **B.** Partielle, les données sont chiffrées
- **C.** Complète
- **D.** Complète après `VACUUM`

---

<details>
<summary><b>Corrigé — ne déplier qu'après avoir répondu aux 45 questions</b></summary>

| # | Rép. | Section | Pourquoi |
|---|---|---|---|
| 1 | B | 2 | Peu de fichiers, quotidien, équipe SQL : `COPY INTO` en tâche de requête SQL, sans état à gérer |
| 2 | C | 1 | `metastore → catalog → schema → table` |
| 3 | A | 3 | `union` aligne par position ; types compatibles = pas d'erreur, résultat faux |
| 4 | C | 4 | Les valeurs de tâches sont le seul mécanisme prévu entre tâches |
| 5 | B | 5 | Le bundle décrit un état souhaité : le déploiement est idempotent |
| 6 | C | 6 | Médiane basse, max très élevé, shuffle read déséquilibré = *skew* |
| 7 | D | 7 | Il faut `USE` sur tous les niveaux supérieurs |
| 8 | C | 2 | `none` ignore silencieusement — le seul mode qui détruit sans trace |
| 9 | B | 3 | Une vue est recalculée à la lecture, donc toujours à jour |
| 10 | A | 4 | Le déclencheur de mise à jour de table chaîne deux jobs sans horloge |
| 11 | C | 5 | `targets` = environnements et surcharges |
| 12 | B | 7 | Fermeture par défaut : un principal inconnu ne voit rien |
| 13 | A | 3 | `count("colonne")` ignore les nulls ; l'écart les compte |
| 14 | A | 2 | `COPY INTO` liste le répertoire à chaque exécution |
| 15 | D | 3 | Diffuser la petite table évite de mélanger la grosse |
| 16 | A | 7 | Le propriétaire n'est pas soumis aux `DENY` sur ses objets |
| 17 | D | 5 | `validate` résout et vérifie sans rien modifier |
| 18 | D | 4 | *Skipped*, pas *failed* : le job peut finir en vert. Piège classique |
| 19 | A | 1 | `RESTORE TABLE ... TO VERSION AS OF` |
| 20 | C | 3 | `explode_outer` garde les tableaux vides ou nuls |
| 21 | B | 2 | État de progression, CDC et schéma gérés, zéro code à maintenir |
| 22 | A | 7 | Ni masque ni filtre sur une vue |
| 23 | B | 6 | Liquid clustering ; le Z-ordering n'est plus le choix par défaut |
| 24 | C | 2 | `TRUNCATE` ne remet pas l'historique des fichiers chargés |
| 25 | A | 3 | Une jointure gauche garantit *au moins* une ligne, pas exactement une |
| 26 | D | 5 | Préfixage et suspension des planifications |
| 27 | B | 6 | `collect()` rapatrie sur le driver |
| 28 | A | 7 | `CREATE TABLE` est un privilège distinct de `USE SCHEMA` |
| 29 | B | 2 | Bronze ne détruit pas les types portés par le format |
| 30 | D | 4 | Le type de tâche tableau de bord existe pour ça |
| 31 | D | 3 | HyperLogLog : approximatif, beaucoup moins cher |
| 32 | A | 2 | Le piège de bordure du watermark |
| 33 | D | 2 | Dans les métadonnées de la table cible, pas dans un checkpoint séparé |
| 34 | D | 4 | Sans timeout, rien n'arrête une tâche bloquée |
| 35 | B | 6 | Le *spill* ralentit sans lever d'erreur |
| 36 | C | 7 | ABAC s'attache au catalog ou au schéma, par étiquette |
| 37 | B | 5 | Le mode `development` isole par utilisateur |
| 38 | D | 3 | Coûteuse, relue souvent, fraîcheur non critique = vue matérialisée |
| 39 | A | 2 | Une ligne supprimée n'a plus d'`updated_at` à comparer |
| 40 | B | 7 | Les fichiers d'une table externe survivent au `DROP` |
| 41 | C | 6 | Moins de données : gain d'un ordre de grandeur, coût nul |
| 42 | C | 4 | La concurrence configurée détermine le parallélisme |
| 43 | D | 5 | Scope de secrets ; jamais dans un fichier versionné |
| 44 | C | 1 | Démarrage en secondes, facturation à l'usage |
| 45 | A | 7 | Un masque dont l'amont est ouvert donne une illusion de conformité |

### Grille de lecture

| Score | Interprétation |
|---|---|
| 38+ / 45 | Bien placé. Travaille les sections où tu as perdu des points |
| 32–37 | Correct. Identifie les 2 sections les plus faibles et approfondis-les |
| < 32 | Reprends les modules correspondants avant de refaire un examen blanc |

Ces seuils sont indicatifs. Le guide officiel le rappelle : un examen blanc généré n'est
pas une mesure calibrée. Ton vrai signal reste la couverture complète des objectifs et
l'aisance sur les dix gestes.

</details>
