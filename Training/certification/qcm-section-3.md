# QCM — Section 3 : Transformation et modélisation

Réalisation le 29/07/2026
Début à 13h44
Fin à 13h54

**22 % de l'examen · ~10 questions · 12 questions ici** — la section la plus lourde.

Objectifs couverts : nettoyage bronze → silver · jointures · manipulation de colonnes et
de lignes · déduplication et agrégations · paramètres de tuning · objets de la couche
gold · contrôles qualité.

---

**1.** Dans l'API DataFrame de PySpark, quelle est la différence entre `df1.union(df2)` et
`df1.unionAll(df2)` ?

- **A.** `union` déduplique, `unionAll` non
- **B.** Aucune : `unionAll` est un alias, et **ni l'un ni l'autre** ne déduplique
- **C.** `unionAll` échoue si les schémas diffèrent
- **D.** `union` aligne les colonnes par nom, `unionAll` par position

Réponse : A

---

**2.** `dfA` a les colonnes `(id, montant)`. `dfB` a `(montant, id)`, avec des types
compatibles. Que produit `dfA.union(dfB)` ?

- **A.** Une erreur de schéma
- **B.** Un résultat correct, Spark aligne sur les noms
- **C.** Un résultat **faux et silencieux** : l'alignement se fait par position
- **D.** Un DataFrame vide

Réponse : B

---

**3.** Une table de faits de 10 millions de lignes est jointe à une dimension de 500
lignes. Quelle stratégie évite un *shuffle* du gros côté ?

- **A.** Augmenter `spark.sql.shuffle.partitions`
- **B.** Une jointure diffusée (*broadcast*) de la dimension
- **C.** Un `cross join` suivi d'un filtre
- **D.** Trier les deux tables sur la clé avant de joindre

Réponse : B

---

**4.** Une jointure gauche entre une table de 1 000 lignes et une table de droite qui
contient **deux lignes pour une même clé** produit combien de lignes ?

- **A.** Toujours 1 000
- **B.** Plus de 1 000 : la ligne de gauche est dupliquée pour chaque correspondance
- **C.** 999
- **D.** 2 000

Réponse : D

---

**5.** Quelle jointure permet de compter les lignes de gauche **sans** correspondance à
droite, sans ramener les colonnes de droite ?

- **A.** `left_semi`
- **B.** `left_anti`
- **C.** `inner`
- **D.** `full_outer`

Réponse : B

---

**6.** Quelle différence entre `count("*")` et `count("ma_colonne")` ?

- **A.** Aucune
- **B.** `count("*")` compte les lignes, `count("colonne")` compte les valeurs non nulles
- **C.** `count("colonne")` est plus rapide mais approximatif
- **D.** `count("*")` échoue s'il y a des valeurs nulles

Réponse : B

---

**7.** Un tableau de bord affiche « environ 25 000 clients actifs », relu des centaines de
fois par jour. Quelle fonction privilégier ?

- **A.** `countDistinct` — la précision prime toujours
- **B.** `approx_count_distinct` — l'erreur de quelques pour cent est acceptable et le coût bien moindre
- **C.** `count` — c'est la même chose
- **D.** `collect_set` puis `len()`

Réponse : B

---

**8.** Quelle commande donne le nombre de lignes, la moyenne, l'écart-type, le minimum, le
maximum **et les quartiles** d'un DataFrame en une fois ?

- **A.** `df.describe()`
- **B.** `df.summary()`
- **C.** `df.stats()`
- **D.** `df.profile()`

Réponse : A

---

**9.** `explode` et `explode_outer` diffèrent sur quel point ?

- **A.** `explode_outer` conserve les lignes dont le tableau est vide ou nul
- **B.** `explode_outer` conserve l'indice de position
- **C.** `explode` ne fonctionne que sur les `MAP`
- **D.** Aucune différence

Réponse : A

---

**10.** Une requête est relue plusieurs fois par heure, elle est coûteuse, et une fraîcheur
à la journée suffit. Quel objet gold choisir ?

- **A.** Une vue
- **B.** Une vue matérialisée
- **C.** Une table de streaming
- **D.** Une table recréée par le pipeline

Réponse : B

---

**11.** Un rapport doit toujours refléter les 90 derniers jours glissants. Quel objet gold ?

- **A.** Une table rafraîchie chaque nuit
- **B.** Une vue, dont la borne est calculée à la lecture
- **C.** Une vue matérialisée rafraîchie une fois par semaine
- **D.** Une table de streaming

Réponse : B

---

**12.** `spark.sql.autoBroadcastJoinThreshold` est fixé à `-1`. Quel effet ?

- **A.** Toutes les jointures deviennent diffusées
- **B.** La diffusion automatique est désactivée : les jointures passent par un *shuffle*
- **C.** Spark choisit la stratégie au hasard
- **D.** Les jointures échouent

Réponse : A

---

Réponses : A, B, B, D, B, B, B, A, A, B, B, A

---

<details>
<summary><b>Corrigé — ne déplier qu'après avoir répondu aux 12 questions</b></summary>

**1 — B.** Piège majeur. En SQL ANSI, `UNION` déduplique et `UNION ALL` non. Dans l'API
DataFrame de Spark, `unionAll` est un **alias historique** de `union`, et **aucun des
deux** ne déduplique. Il faut un `.distinct()` explicite. En SQL Spark
(`spark.sql("... UNION ...")`), la sémantique ANSI s'applique bien : deux comportements
pour le même mot selon l'API.

**2 — C.** `union` aligne par **position**, pas par nom. Ici les types sont compatibles,
donc aucune erreur n'est levée : le résultat est simplement faux. Utilise `unionByName`
par défaut.

**3 — B.** Une dimension de 500 lignes se diffuse à tous les exécuteurs, ce qui évite de
mélanger les 10 millions de lignes. A augmenterait le parallélisme du *shuffle* sans
l'éviter. C est absurde, D ne change pas la stratégie de jointure.

**4 — B.** Une jointure gauche garantit **au moins** une ligne par ligne de gauche, pas
exactement une. Une clé en double à droite duplique la ligne de gauche. C'est la cause la
plus fréquente des faits qui gonflent après ajout d'une dimension.

**5 — B.** `left_anti` renvoie les lignes de gauche sans correspondance, sans ramener les
colonnes de droite — donc sans coût de transfert. `left_semi` (A) fait l'inverse : celles
**avec** correspondance.

**6 — B.** `count("*")` compte les lignes ; `count("colonne")` ignore les nulls. L'écart
entre les deux est un compteur de valeurs manquantes gratuit.

**7 — B.** `approx_count_distinct` utilise HyperLogLog : erreur par défaut d'environ 5 %,
coût très inférieur à un `countDistinct` qui exige un *shuffle* complet. Sur un affichage
arrondi relu en continu, c'est le bon arbitrage. Sur un décompte réglementaire, non.

**8 — B.** `summary()` ajoute les quartiles à ce que donne `describe()`, et accepte des
percentiles personnalisés. `describe()` (A) s'arrête à count/mean/stddev/min/max.

**9 — A.** `explode` supprime les lignes dont le tableau est vide ou nul,
`explode_outer` les conserve avec un `null`. L'indice de position s'obtient avec
`posexplode` / `posexplode_outer`.

**10 — B.** Coûteuse + relue souvent + fraîcheur non critique = vue matérialisée. Le
moteur sait la rafraîchir incrémentalement quand la requête s'y prête, ce qu'un
`CREATE OR REPLACE TABLE AS SELECT` ne fera jamais.

**11 — B.** Une fenêtre glissante matérialisée est périmée le lendemain — et périmée
**sans erreur**, ce qui est pire. En vue, la borne est recalculée à chaque lecture, donc
le résultat est juste par construction.

**12 — B.** `-1` désactive la diffusion automatique et force le *shuffle*. C'est utile
pour mesurer ce que la diffusion apporte, ou pour éviter une saturation mémoire quand
l'estimation de taille est fausse.

</details>
