# QCM — Section 1 : Plateforme Databricks

Réalisation le 29/07/2026
Début à 13h16
Fin à 13h23

**6 % de l'examen · ~3 questions · 10 questions ici**

Objectifs couverts : composants du socle (architecture, Delta Lake, Unity Catalog) ·
services de compute, leurs caractéristiques, limites et modèles de coût.

---

**1.** Une équipe doit itérer rapidement sur ses pipelines, revenir en arrière de façon
fiable après une mauvaise ingestion, conserver une piste d'audit à des fins
réglementaires, et servir une source unique de vérité à la fois pour la BI et l'IA.

Quelle approche répond à l'ensemble de ces exigences ?

- **A.** Stocker des CSV sur DBFS avec un versionnement manuel et des copies nocturnes
- **B.** Delta Lake pour les transactions ACID et le *time travel*, gouverné par Unity Catalog
- **C.** Le stockage objet seul, avec des requêtes SQL ponctuelles pour la reprise
- **D.** Des DataFrames en mémoire, recalculés à la demande

Réponse : B

---

**2.** Des analystes lancent des requêtes SQL ad hoc toute la journée sur des tables Delta
curées. L'équipe veut de bonnes performances, un démarrage rapide, plusieurs utilisateurs
simultanés, et éviter de payer une mise à l'échelle inutile.

Quelle configuration de compute convient ?

- **A.** Un job cluster avec mise à l'échelle automatique
- **B.** Un all-purpose cluster à nombre de workers fixe
- **C.** Un SQL warehouse serverless
- **D.** Un cluster mono-nœud

Réponse : A

---

**3.** Un ETL planifié tourne chaque nuit sur un all-purpose cluster. Un ingénieur propose
de le basculer sur du job compute.

Quel est l'argument principal ?

- **A.** Le job compute est plus rapide à l'exécution
- **B.** Le job compute est facturé à un tarif nettement inférieur pour une charge éphémère
- **C.** Le job compute supporte plus de bibliothèques
- **D.** L'all-purpose cluster ne peut pas être planifié

Réponse : B

---

**4.** Dans Unity Catalog, quel est l'ordre correct de la hiérarchie des objets sécurisables ?

- **A.** metastore → catalog → schema → table
- **B.** workspace → catalog → database → table
- **C.** catalog → metastore → schema → table
- **D.** account → workspace → schema → table

Réponse : B

---

**5.** Une table Delta a subi une écriture erronée il y a deux heures. Quelle commande
permet de revenir à l'état antérieur ?

- **A.** `ROLLBACK TRANSACTION`
- **B.** `RESTORE TABLE ma_table TO VERSION AS OF 12`
- **C.** `UNDO LAST WRITE ON ma_table`
- **D.** `ALTER TABLE ma_table REVERT`

Réponse : C

---

**6.** Qu'est-ce qui distingue un **volume** d'une **table** dans Unity Catalog ?

- **A.** Le volume est en lecture seule, la table est modifiable
- **B.** Le volume gouverne des fichiers, la table gouverne des lignes structurées
- **C.** Le volume ne peut contenir que du CSV
- **D.** Le volume n'apparaît pas dans le lineage

Réponse : B

---

**7.** Un ingénieur affirme : « Delta Lake garantit l'ACID, donc deux jobs peuvent écrire
dans la même table sans risque. » Que faut-il nuancer ?

- **A.** Rien, l'affirmation est exacte en toutes circonstances
- **B.** Delta gère la concurrence par contrôle optimiste : les écritures conflictuelles échouent et doivent être rejouées
- **C.** Delta ne gère pas les transactions, seulement le versionnement
- **D.** Les écritures concurrentes sont sérialisées et attendent leur tour indéfiniment

Réponse : B

---

**8.** Quelle affirmation décrit correctement le compute **serverless** ?

- **A.** Il permet de choisir le type d'instance et le nombre de workers
- **B.** Il démarre en quelques secondes et ne facture pas les périodes d'inactivité
- **C.** Il est réservé aux charges de machine learning
- **D.** Il exige un cluster préalablement démarré

Réponse : A

---

**9.** Une équipe veut partager des tables avec un partenaire externe qui n'utilise pas
Databricks. Quelle fonctionnalité vise cet usage ?

- **A.** Unity Catalog Volumes
- **B.** Delta Sharing
- **C.** Databricks Git Folders
- **D.** Lakeflow Connect

Réponse : B

---

**10.** Un job produit des résultats corrects en développement et échoue en production
avec `ClassNotFoundException`. Quelle cause faut-il examiner en premier ?

- **A.** Un manque de mémoire sur le driver
- **B.** Une différence de version de runtime ou de bibliothèques entre les deux environnements
- **C.** Une table absente du catalog de production
- **D.** Un quota de stockage dépassé

Réponse : B

---

Réponses: B, A, B, B, C, B, B, A, B, B

---


<details>
<summary><b>Corrigé — ne déplier qu'après avoir répondu aux 10 questions</b></summary>

**1 — B.** C'est la question 2 du guide officiel. Delta Lake apporte les transactions
ACID et le *time travel* (donc le retour arrière et la piste d'audit), Unity Catalog la
gouvernance et le lineage. Les trois autres options n'offrent ni transaction ni
versionnement fiable.

**2 — C.** Interactif + concurrent + démarrage rapide = SQL warehouse. Le job cluster (A)
vise les charges planifiées ; l'all-purpose à taille fixe (B) ne s'adapte pas à la charge
et coûte cher ; le mono-nœud (D) ne supporte pas la concurrence.
*Note : la question 4 du guide officiel pose le même scénario avec un jeu de réponses où
le high-concurrency cluster est la bonne réponse. Retiens le raisonnement — interactif et
concurrent — plutôt qu'un nom de produit.*

**3 — B.** Le tarif DBU du job compute est nettement inférieur à celui de l'all-purpose,
pour une charge qui n'a pas besoin d'être interactive. La vitesse d'exécution (A) est
comparable ; C et D sont faux.

**4 — A.** `metastore → catalog → schema → table/volume/function`. Retiens qu'il faut
`USE` sur tous les niveaux supérieurs pour atteindre un objet — c'est la cause n°1 des
accès qui ne fonctionnent pas malgré un `GRANT SELECT`.

**5 — B.** `RESTORE TABLE ... TO VERSION AS OF` ou `TO TIMESTAMP AS OF`. Les trois autres
commandes n'existent pas. `DESCRIBE HISTORY` donne le numéro de version à viser.

**6 — B.** Un volume gouverne des **fichiers** (`/Volumes/catalog/schema/volume/`), une
table gouverne des **lignes**. Les deux existent en versions managée et externe, et les
deux apparaissent dans le lineage.

**7 — B.** Delta utilise un contrôle de concurrence **optimiste** : deux écritures
conflictuelles ne s'attendent pas, la seconde échoue avec une exception de concurrence et
doit être rejouée. C'est fondamental pour concevoir des jobs concurrents.

**8 — B.** Démarrage en secondes, facturation à l'usage, aucun temps d'inactivité facturé.
A décrit le compute classique ; C et D sont faux.

**9 — B.** Delta Sharing est le protocole ouvert de partage, y compris vers des
consommateurs hors Databricks. Les autres réponses désignent des fonctionnalités internes.

**10 — B.** `ClassNotFoundException` est la signature d'un conflit ou d'une absence de
bibliothèque. L'ordre de résolution est notebook → cluster → runtime : un environnement
qui n'a pas les mêmes bibliothèques installées produit exactement ce symptôme.

</details>
