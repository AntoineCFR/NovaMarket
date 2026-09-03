# QCM — Section 2 : Ingestion et chargement

Réalisation le 29/07/2026
Début à 13h29
Fin à 13h41

**21 % de l'examen · ~9 questions · 12 questions ici**

Objectifs couverts : motifs batch / streaming / incrémental · `COPY INTO` · Auto Loader
avec application et évolution de schéma · Lakeflow Connect · clients JDBC et REST ·
arbitrage entre méthodes · données semi-structurées et imbriquées.

---

**1.** Un répertoire de stockage objet reçoit environ 50 000 nouveaux fichiers JSON par
jour. Le pipeline doit les ingérer en continu, sans relire ceux déjà traités.

Quelle méthode convient ?

- **A.** `COPY INTO` planifié toutes les cinq minutes
- **B.** Auto Loader en mode notification de fichier
- **C.** `INSERT INTO ... SELECT * FROM json.` avec un filtre sur la date
- **D.** Un `spark.read.json()` complet suivi d'un `overwrite`

Réponse : B

---

**2.** Quel mécanisme `COPY INTO` utilise-t-il pour ne pas recharger deux fois le même
fichier ?

- **A.** Un checkpoint dans un répertoire désigné par l'utilisateur
- **B.** L'historique des fichiers chargés, conservé dans les métadonnées de la table cible
- **C.** Une comparaison des sommes de contrôle à chaque exécution
- **D.** Aucun : il recharge tout à chaque fois

Réponse : B

---

**3.** Une table a été chargée par `COPY INTO`. Un ingénieur exécute `TRUNCATE TABLE`,
puis relance exactement la même commande `COPY INTO`. Que se passe-t-il ?

- **A.** Tous les fichiers sont rechargés, la table retrouve son contenu
- **B.** Aucune ligne n'est chargée : les fichiers sont toujours considérés comme traités
- **C.** La commande échoue car la table est vide
- **D.** Seuls les fichiers modifiés depuis le `TRUNCATE` sont chargés

Réponse : D

---

**4.** Auto Loader est configuré avec `cloudFiles.schemaEvolutionMode = "addNewColumns"`.
Un fichier arrive avec deux colonnes supplémentaires. Que se passe-t-il ?

- **A.** Les colonnes sont ignorées silencieusement
- **B.** Le flux échoue, met à jour son schéma, et la relance suivante les intègre
- **C.** Les colonnes partent dans `_rescued_data`
- **D.** Le flux s'arrête définitivement et exige une intervention manuelle

Réponse : B

---

**5.** Quel mode d'évolution de schéma faut-il **éviter** parce qu'il détruit de la donnée
sans le signaler ?

- **A.** `addNewColumns`
- **B.** `rescue`
- **C.** `none`
- **D.** `failOnNewColumns`

Réponse : C

---

**6.** À quoi sert `rescuedDataColumn` dans Auto Loader ?

- **A.** À stocker les lignes rejetées dans une table séparée
- **B.** À conserver, dans une colonne, les données qui ne correspondent pas au schéma
- **C.** À réessayer automatiquement les fichiers en échec
- **D.** À restaurer une version antérieure de la table

Réponse : B

---

**7.** Une équipe doit ingérer des objets Salesforce vers Unity Catalog, avec capture des
changements, sans écrire ni maintenir de code d'extraction.

Quelle approche ?

- **A.** Un notebook avec un client REST, orchestré par un job
- **B.** Un connecteur managé Lakeflow Connect
- **C.** Auto Loader sur un export CSV déposé quotidiennement
- **D.** Une lecture JDBC avec extraction par watermark

Réponse : B

---

**8.** Quelle affirmation distingue correctement Lakeflow Connect de Lakeflow Declarative
Pipelines ?

- **A.** Connect transforme la donnée, les pipelines l'ingèrent
- **B.** Connect ingère la donnée jusqu'au lakehouse, les pipelines la transforment ensuite
- **C.** Ce sont deux noms du même produit
- **D.** Connect est réservé aux fichiers, les pipelines aux bases de données

Réponse : B

---

**9.** Un ingénieur extrait une table Postgres avec un filtre `WHERE updated_at >
:watermark`, puis avance le watermark au maximum observé. Quel risque prend-il ?

- **A.** Aucun, c'est le motif standard
- **B.** Les lignes validées à un horodatage inférieur ou égal au watermark après la lecture sont perdues définitivement
- **C.** Les lignes sont dupliquées à chaque exécution
- **D.** Le watermark recule à chaque exécution vide

Réponse : D

---

**10.** Une source relationnelle supprime physiquement ses lignes (`DELETE`). Quelle
conséquence pour une extraction incrémentale par watermark sur `updated_at` ?

- **A.** Les suppressions sont détectées automatiquement
- **B.** Les lignes supprimées ne remontent dans aucun delta et restent indéfiniment en cible
- **C.** L'extraction échoue
- **D.** Le watermark devient négatif

Réponse : B

---

**11.** Un fichier JSON contient des objets imbriqués et des tableaux. Dans une couche
bronze fidèle à la source, que faut-il faire de ces structures ?

- **A.** Les aplatir immédiatement pour simplifier la lecture
- **B.** Les conserver telles quelles, en `STRUCT` et `ARRAY`
- **C.** Les sérialiser en chaîne JSON
- **D.** Les rejeter, bronze n'accepte que du plat

Réponse : B

---

**12.** Dans quel cas `COPY INTO` reste-t-il préférable à Auto Loader ?

- **A.** Quand le répertoire contient des millions de fichiers
- **B.** Quand il faut du streaming continu
- **C.** Pour un chargement ponctuel de quelques fichiers, sans état à gérer
- **D.** Quand le schéma évolue fréquemment

Réponse : C

---

Réponses : B, B, D, B, C, B, B, B, D, B, B, C

---

<details>
<summary><b>Corrigé — ne déplier qu'après avoir répondu aux 12 questions</b></summary>

**1 — B.** Volume élevé de fichiers + continu = Auto Loader, et le mode notification
évite de lister le répertoire à chaque passage. `COPY INTO` (A) listerait 50 000 fichiers
puis de plus en plus. C et D rechargent ou dupliquent.

**2 — B.** L'état de `COPY INTO` vit dans les métadonnées de la **table cible**. C'est la
différence essentielle avec Auto Loader, dont le checkpoint est un répertoire séparé — que
l'on peut perdre, et que `RESTORE` ne remet pas en cohérence.

**3 — B.** Piège classique. `TRUNCATE` vide les données mais **ne remet pas** l'historique
des fichiers chargés. Pour tout recharger, il faut `COPY_OPTIONS ('force' = 'true')` ou
recréer la table.

**4 — B.** `addNewColumns` fait échouer le flux **volontairement** : il enregistre le
nouveau schéma puis s'arrête. La relance repart du schéma mis à jour. C'est pourquoi la
tâche d'ingestion doit avoir des nouvelles tentatives.

**5 — C.** `none` ignore les colonnes inconnues, silencieusement et définitivement. C'est
la seule option qui détruit de la donnée sans laisser de trace. `rescue` (B) les conserve
dans la colonne de sauvetage.

**6 — B.** La colonne de sauvetage capture ce qui ne correspond pas au schéma : colonnes
en trop, conflits de type, enregistrements illisibles. Elle ne crée pas de table séparée
— c'est le rôle d'une quarantaine, qu'on construit soi-même.

**7 — B.** Application d'entreprise + CDC + zéro code à maintenir = connecteur managé. A
et D fonctionneraient mais imposent de gérer pagination, jetons, schéma et suppressions.

**8 — B.** Connect **amène** la donnée, les pipelines déclaratifs la **transforment**. Les
deux commencent par « Lakeflow » et font des choses différentes.

**9 — B.** C'est le piège de bordure. Une transaction ouverte avant la lecture et validée
après porte un `updated_at` ≤ watermark : le `>` strict ne la verra jamais, et la perte
est définitive et silencieuse. On utilise `>=`, ou un watermark reculé d'une marge de
sécurité.

**10 — B.** Une ligne supprimée n'a plus d'`updated_at` à comparer. D'où l'intérêt des
suppressions douces côté source, ou d'un CDC natif, ou d'une réconciliation périodique
des clés.

**11 — B.** La règle bronze n'est pas « tout en string », c'est « ne rien ajouter, ne rien
perdre ». Les types du JSON sont portés par le format lui-même : les détruire, c'est
perdre de l'information réelle. L'aplatissement est une transformation, donc silver.

**12 — C.** Chargement ponctuel, peu de fichiers, ou équipe qui travaille en SQL :
`COPY INTO` tient en trois lignes et n'a aucun état à créer ni à nettoyer. A, B et D sont
tous des arguments **en faveur** d'Auto Loader.

</details>
