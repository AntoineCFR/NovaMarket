# QCM — Section 6 : Diagnostic, monitoring et optimisation

Réalisation le 29/07/2026
Début à 17h19
Fin à 17h25

**10 % de l'examen · ~5 questions · 12 questions ici**

Objectifs couverts : tendances de performance des jobs · interface Lakeflow Jobs ·
goulots d'étranglement dans le Spark UI (*skew*, *shuffle*, *spill*) · Liquid Clustering
et predictive optimization · diagnostic de compute.

---

**1.** La durée d'un job batch a doublé après l'arrivée d'une nouvelle source. Dans le
Spark UI, le stage le plus long montre la plupart des tâches sous 30 secondes et **une**
tâche à plus de 10 minutes. Le résumé indique un *shuffle read* médian d'environ 400 Mo,
et un maximum au-delà de 5 Go.

Quelle solution réduit la durée ?

- **A.** Augmenter la taille du cluster pour ajouter des exécuteurs
- **B.** Vérifier que l'*adaptive query execution* et son traitement du *skew join* sont actifs, pour découper la partition surdimensionnée à l'exécution
- **C.** Réduire `spark.sql.shuffle.partitions` pour regrouper le travail en moins de tâches
- **D.** Repartitionner manuellement avec une clé salée avant la jointure

Réponse : B

---

**2.** Comment reconnaît-on un *disk spill* dans le Spark UI ?

- **A.** Une exception `OutOfMemoryError` dans les journaux
- **B.** Des colonnes *Spill (memory)* et *Spill (disk)* non nulles au niveau du stage
- **C.** Un nombre de tâches supérieur au nombre d'exécuteurs
- **D.** Un temps de garbage collection élevé

Réponse : A

---

**3.** Quelle est la conséquence la plus importante d'un *spill* ?

- **A.** Le job échoue
- **B.** Le job ralentit **sans lever d'erreur** — c'est un problème silencieux
- **C.** Les données sont corrompues
- **D.** Le checkpoint est invalidé

Réponse : D

---

**4.** Un `collect()` sur un DataFrame de 50 millions de lignes provoque une erreur. Où se
produit la saturation mémoire ?

- **A.** Sur les exécuteurs
- **B.** Sur le driver
- **C.** Dans le stockage objet
- **D.** Dans le metastore

Réponse : A

---

**5.** Pour une nouvelle table volumineuse fréquemment filtrée sur deux colonnes, quelle
approche recommande Databricks aujourd'hui ?

- **A.** `PARTITIONED BY` sur les deux colonnes
- **B.** `OPTIMIZE ... ZORDER BY` après chaque chargement
- **C.** `CLUSTER BY` — le liquid clustering
- **D.** Un index secondaire

Réponse : D

---

**6.** Qu'apporte le liquid clustering par rapport au partitionnement classique ?

- **A.** Il compresse mieux les données
- **B.** Il ne fige pas d'arborescence de répertoires et évite les partitions déséquilibrées ou minuscules
- **C.** Il supprime le besoin d'`OPTIMIZE`
- **D.** Il accélère les écritures de 50 %

Réponse : B

---

**7.** Sur quoi faut-il mesurer le gain d'un regroupement plutôt que sur la durée
d'exécution ?

- **A.** Le nombre de partitions
- **B.** Le nombre de fichiers réellement lus par la requête
- **C.** La taille de la table sur disque
- **D.** Le nombre d'exécuteurs

Réponse : D

---

**8.** Qu'est-ce que la *predictive optimization* dans Unity Catalog ?

- **A.** Un moteur de recommandation de requêtes
- **B.** Le déclenchement automatique d'`OPTIMIZE` et de `VACUUM` sur les tables managées quand c'est rentable
- **C.** Une prédiction du coût d'une requête avant exécution
- **D.** Un cache de résultats

Réponse : C

---

**9.** Une jointure diffuse une table de 2 Go vers tous les exécuteurs. Quel risque ?

- **A.** Aucun, plus la table diffusée est grosse, mieux c'est
- **B.** Une saturation mémoire des exécuteurs — un *shuffle* lent est préférable à un OOM
- **C.** Une perte de données
- **D.** Un ralentissement des écritures

Réponse : B

---

**10.** Comment comparer la durée d'exécution actuelle d'un job à un historique de
référence ?

- **A.** L'historique des exécutions dans l'interface Lakeflow Jobs
- **B.** Le Spark UI de la dernière exécution
- **C.** `DESCRIBE HISTORY` sur la table cible
- **D.** Les journaux du cluster

Réponse : A

---

**11.** Face à une tâche lente, quel levier offre le meilleur rapport coût/bénéfice ?

- **A.** Augmenter la taille du cluster
- **B.** Réduire le volume de données lu — filtrer plus tôt, élaguer, ne lire que les colonnes utiles
- **C.** Augmenter le nombre de nouvelles tentatives
- **D.** Passer en mode continu

Réponse : B

---

**12.** Un cluster ne démarre pas et l'interface affiche `BOOTSTRAP_TIMEOUT`. Où chercher
la cause ?

- **A.** Dans le Spark UI
- **B.** Dans le journal d'événements du cluster, qui dit **pourquoi** et non seulement **que** ça a échoué
- **C.** Dans `DESCRIBE HISTORY`
- **D.** Dans le catalog

Réponse : B

---

Réponses : B, A, D, A, D, B, D, C, B, A, B, B

---

<details>
<summary><b>Corrigé — ne déplier qu'après avoir répondu aux 12 questions</b></summary>

**1 — B.** C'est la question 1 du guide officiel. Le profil décrit — médiane basse,
maximum très élevé, *shuffle read* max dix fois la médiane — est la signature d'un *data
skew*. L'AQE le corrige à l'exécution en découpant la partition surdimensionnée. Ajouter
des exécuteurs (A) n'aide pas : c'est **une seule** tâche qui traîne. Réduire les
partitions (C) aggrave. Le salage manuel (D) fonctionne mais reste le dernier recours,
après avoir vérifié que l'AQE fait son travail.

**2 — B.** Le *spill* se lit dans les métriques de stage, pas dans les journaux. C'est
précisément pour ça qu'il passe inaperçu.

**3 — B.** Le *spill* n'est pas une erreur : la partition ne tenait pas en mémoire, elle
est passée par le disque. Le job réussit, en étant beaucoup plus lent. Un ralentissement
silencieux est plus difficile à diagnostiquer qu'un échec.

**4 — B.** `collect()` rapatrie les données **sur le driver**. C'est la cause n°1 des OOM
de driver. Le remède n'est pas d'agrandir le driver, c'est d'agréger avant de ramener —
ou de ne pas ramener du tout.

**5 — C.** Le liquid clustering est la recommandation actuelle pour les nouvelles tables.
Le Z-ordering reste supporté mais n'est plus le choix par défaut — c'est un des pièges où
une IA entraînée avant le changement se trompe systématiquement.

**6 — B.** Le partitionnement fige une arborescence : trop de partitions produit une
myriade de petits fichiers, trop peu ne filtre rien, et un choix de colonne malheureux
est coûteux à corriger. Le liquid clustering évite ce piège et se modifie sans réécrire
la table.

**7 — B.** Le gain vient de l'**élagage de fichiers**. Sur un petit jeu de test, la durée
est dominée par les coûts fixes et ne bougera pas, alors que le mécanisme fonctionne. Le
ratio fichiers lus / fichiers totaux est la vraie mesure.

**8 — B.** La *predictive optimization* prend en charge la maintenance des tables managées
d'Unity Catalog : elle décide quand `OPTIMIZE` et `VACUUM` sont rentables et les
déclenche. C'est un argument de plus en faveur des tables managées.

**9 — B.** La table diffusée est copiée **intégralement sur chaque exécuteur**. À 2 Go,
c'est une saturation mémoire probable. Un *shuffle* est lent ; un OOM fait tout échouer.

**10 — A.** L'historique des exécutions donne les durées successives et permet de repérer
une dérive. Le Spark UI (B) détaille **une** exécution sans point de comparaison.

**11 — B.** Moins de données : gain souvent d'un ordre de grandeur, coût nul, bénéfice
permanent. Augmenter le cluster (A) donne un gain au mieux linéaire pour un coût
récurrent à chaque exécution — et masque le problème au lieu de le corriger.

**12 — B.** Le journal d'événements du cluster donne la cause. Le Spark UI ne sert à rien
ici : il n'y a pas eu d'application Spark, le cluster n'a jamais démarré.

</details>
