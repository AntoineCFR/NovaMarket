# QCM — Section 4 : Lakeflow Jobs

Réalisation le 29/07/2026
Début à 17h01
Fin à 17h08

**16 % de l'examen · ~7 questions · 10 questions ici**

Objectifs couverts : flux de contrôle (nouvelles tentatives, conditions, boucles) ·
types de tâches et dépendances dans le DAG · planification et types de déclencheurs ·
arbitrage entre déclencheurs temporels et pilotés par la donnée.

---

**1.** Une tâche d'ingestion Auto Loader échoue environ une fois par mois, quand la source
ajoute une colonne. La relance manuelle réussit toujours. Quelle configuration évite
l'intervention ?

- **A.** Passer le mode d'évolution de schéma à `none`
- **B.** Configurer des nouvelles tentatives sur la tâche
- **C.** Doubler le timeout du job
- **D.** Supprimer le checkpoint avant chaque exécution

Réponse : B

---

**2.** Comment une tâche transmet-elle une valeur calculée à une tâche suivante du même job ?

- **A.** Une variable globale Python
- **B.** Une valeur de tâche, avec `dbutils.jobs.taskValues.set()`
- **C.** Un fichier temporaire dans DBFS
- **D.** Une table de passage dans le catalog

Réponse : B

---

**3.** Une tâche conditionnelle évalue `{{tasks.controle.values.statut}} == 'PASS'`. La
condition est fausse. Que deviennent les tâches de la branche `true` ?

- **A.** Elles échouent
- **B.** Elles sont marquées *skipped*, et le job peut se terminer en succès
- **C.** Elles s'exécutent quand même
- **D.** Le job est annulé

Réponse : A

---

**4.** Quel type de tâche permet de déclencher une mise à jour d'un pipeline déclaratif
depuis un job ?

- **A.** Une tâche notebook qui appelle l'API
- **B.** Une tâche de type pipeline
- **C.** Une tâche `Spark Submit`
- **D.** Ce n'est pas possible, il faut planifier le pipeline séparément

Réponse : B

---

**5.** Une équipe veut rafraîchir un agrégat avec une requête SQL sauvegardée, sans écrire
de notebook. Quel type de tâche ?

- **A.** Notebook
- **B.** Requête SQL
- **C.** Tableau de bord
- **D.** Fichier Python

Réponse : B

---

**6.** Un partenaire dépose des fichiers à des heures imprévisibles, et la donnée doit
être traitée dans les quinze minutes. Quel déclencheur ?

- **A.** Programmé toutes les quinze minutes
- **B.** Arrivée de fichier
- **C.** Mise à jour de table
- **D.** Manuel

Réponse : B

---

**7.** Un job B doit tourner dès que le job A a fini d'écrire dans une table Delta, sans
coordination d'horloge. Quel déclencheur ?

- **A.** Programmé, dix minutes après A
- **B.** Mise à jour de table sur la table écrite par A
- **C.** Arrivée de fichier
- **D.** Continu

Réponse : A

---

**8.** Quel risque présente un déclencheur d'arrivée de fichier quand un partenaire dépose
200 fichiers d'un coup ?

- **A.** Le job ignore les fichiers au-delà du premier
- **B.** Une cascade d'exécutions, à plafonner par le nombre d'exécutions concurrentes
- **C.** Le déclencheur se désactive
- **D.** Aucun : les dépôts sont regroupés automatiquement

Réponse : B

---

**9.** Une tâche `for each` itère sur une liste de dix éléments avec une concurrence de 5.
Combien de tâches sont prêtes à démarrer simultanément ?

- **A.** 1
- **B.** 5
- **C.** 10
- **D.** Cela dépend du cluster

Réponse : B

---

**10.** Un job n'a pas de timeout configuré et une tâche se bloque. Quelle conséquence ?

- **A.** Le job s'arrête après une heure par défaut
- **B.** Le job continue de consommer du compute jusqu'à intervention manuelle
- **C.** La tâche est relancée automatiquement
- **D.** Le job passe en échec au bout de dix minutes

Réponse : A

---

Réponses : B, B, A, B, B, B, A, B, B, A

---

<details>
<summary><b>Corrigé — ne déplier qu'après avoir répondu aux 10 questions</b></summary>

**1 — B.** Le comportement décrit est celui d'Auto Loader en `addNewColumns` : il échoue
volontairement, enregistre le nouveau schéma, et la relance passe. Des nouvelles
tentatives absorbent l'incident sans réveiller personne. A détruirait la donnée des
colonnes nouvelles ; C et D sont hors sujet.

**2 — B.** Les valeurs de tâches sont le mécanisme prévu. Une variable globale (A) ne
survit pas d'une tâche à l'autre — chacune a son propre contexte d'exécution. C et D
fonctionneraient mais introduisent un état à nettoyer et à gérer en cas d'échec.

**3 — B.** Une tâche non exécutée pour cause de condition est *skipped*, pas *failed*. Un
job dont toutes les tâches critiques ont été sautées **se termine en succès** et
n'envoie aucune notification d'échec. C'est le piège à connaître : il faut une tâche sur
la branche `false` qui lève une exception.

**4 — B.** Le type de tâche pipeline existe précisément pour ça. A fonctionnerait mais
réinvente ce que la plateforme fournit, sans la gestion des dépendances ni l'affichage
dans le DAG.

**5 — B.** La tâche de requête SQL exécute une requête sauvegardée sur un SQL warehouse.
Pas de notebook à maintenir, et c'est accessible à une équipe qui travaille en SQL.

**6 — B.** Dépôts imprévisibles + latence courte = arrivée de fichier. Un déclencheur
programmé toutes les quinze minutes (A) tournerait à vide la plupart du temps, et
consommerait du compute pour rien.

**7 — B.** Le déclencheur de mise à jour de table est fait pour chaîner deux jobs sans se
caler sur l'horloge. L'option A est le motif fragile classique : si A prend onze minutes
un jour, B travaille sur des données incomplètes.

**8 — B.** Chaque dépôt peut déclencher une exécution. Le nombre maximal d'exécutions
concurrentes est le garde-fou — en Free Edition, avec 5 tâches concurrentes au total, il
faut le fixer à 1.

**9 — B.** La concurrence configurée du `for each` détermine combien d'itérations
démarrent en parallèle. C'est un point d'attention en Free Edition, où le plafond est de
5 tâches concurrentes par compte.

**10 — B.** Sans timeout, rien n'arrête une tâche bloquée. Sur une offre à quota, c'est le
budget d'une journée qui part. Un timeout par **tâche**, plus court que celui du job,
attrape le problème plus tôt et désigne le coupable.

</details>
