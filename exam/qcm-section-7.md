# QCM — Section 7 : Gouvernance et sécurité

Réalisation le 31/07/2026
Début à 08h11
Fin à 08h16

**15 % de l'examen · ~7 questions · 12 questions ici** — la section la mieux payée après
transformation et ingestion.

Objectifs couverts : tables managées et externes · `GRANT`, `REVOKE`, `DENY` · masquage
de colonnes et sécurité au niveau des lignes · politiques ABAC.

---

**1.** Quelle différence entre une table managée et une table externe dans Unity Catalog ?

- **A.** La table externe ne peut pas être requêtée en SQL
- **B.** `DROP TABLE` supprime les fichiers d'une table managée, mais pas ceux d'une table externe
- **C.** La table managée ne supporte pas Delta
- **D.** La table externe n'apparaît pas dans le lineage

Réponse : B

---

**2.** Une table externe est supprimée par `DROP TABLE`, puis recréée au même emplacement.
Que contient-elle ?

- **A.** Rien, elle est vide
- **B.** Les données d'avant : les fichiers n'ont jamais été supprimés
- **C.** La commande échoue
- **D.** Une copie partielle

Réponse : B

---

**3.** Quel avantage les tables **managées** offrent-elles qui n'existe pas pour les
tables externes ?

- **A.** Le support du format Parquet
- **B.** L'optimisation automatique — compactage et `VACUUM` pris en charge
- **C.** La possibilité d'être partagées
- **D.** L'accès en SQL

Réponse : B

---

**4.** Un utilisateur a reçu `GRANT SELECT ON SCHEMA mon_catalog.gold`. Il ne voit
toujours pas les tables. Quelle est la cause la plus probable ?

- **A.** Le privilège met 24 heures à se propager
- **B.** Il lui manque `USE CATALOG` sur le catalog, et/ou `USE SCHEMA` sur le schéma
- **C.** `SELECT` ne s'applique pas aux schémas
- **D.** Les tables sont externes

Réponse : B

---

**5.** Quelle est la différence entre ne pas accorder un privilège et poser un `DENY` ?

- **A.** Aucune
- **B.** Le `DENY` est explicite, s'affiche dans `SHOW GRANTS`, et **l'emporte sur tout `GRANT`**, y compris hérité
- **C.** Le `DENY` est temporaire
- **D.** Le `DENY` ne fonctionne que sur les catalogs

Réponse : B

---

**6.** Un propriétaire de table pose un `DENY SELECT` sur sa propre table, à un groupe
dont il fait partie. Peut-il encore la lire ?

- **A.** Non, le `DENY` s'applique à tous
- **B.** Oui : le propriétaire n'est pas soumis aux `DENY` sur ses propres objets
- **C.** Seulement en lecture seule
- **D.** Seulement via un SQL warehouse

Réponse : B

---

**7.** Qu'est-ce qu'un masque de colonne dans Unity Catalog ?

- **A.** Une copie anonymisée de la table
- **B.** Une fonction appliquée **à la lecture**, qui renvoie une valeur différente selon l'appelant, sans modifier la donnée stockée
- **C.** Un chiffrement au repos
- **D.** Une vue filtrée

Réponse : B

---

**8.** Sur quel type d'objet est-il **impossible** de poser un masque de colonne ou un
filtre de lignes ?

- **A.** Une table managée
- **B.** Une table externe
- **C.** Une vue
- **D.** Une table de streaming

Réponse : B

---

**9.** Une fonction de masquage interroge une table de correspondance pour savoir si
l'appelant a le droit de voir la valeur en clair. Que se passe-t-il si l'appelant est
absent de cette table ?

- **A.** Cela dépend de l'écriture de la fonction — et une fonction bien écrite masque par défaut
- **B.** La requête échoue
- **C.** La valeur est toujours affichée en clair
- **D.** L'utilisateur est ajouté automatiquement

Réponse : A

---

**10.** Une table alimentée par `MERGE` reçoit une politique de masquage. Quel risque ?

- **A.** Aucun
- **B.** Le `MERGE` peut ne plus être supporté sur cette table, ce qui casse le pipeline d'alimentation
- **C.** Le masque est ignoré pendant le `MERGE`
- **D.** La table devient en lecture seule

Réponse : A

---

**11.** Que permettent les politiques ABAC par rapport à un masque posé colonne par
colonne ?

- **A.** Un chiffrement plus fort
- **B.** S'attacher à un catalog ou un schéma et s'appliquer à toute colonne portant une étiquette donnée, y compris dans les tables créées plus tard
- **C.** De masquer les lignes plutôt que les colonnes
- **D.** De se passer de fonction de masquage

Réponse : B

---

**12.** Une équipe masque `email` dans la couche gold, mais les couches bronze et silver
restent lisibles par les mêmes utilisateurs. Quelle est la portée réelle de la protection ?

- **A.** Complète : le masque suffit
- **B.** Nulle en pratique : il suffit de remonter d'une couche pour lire la donnée en clair
- **C.** Partielle : les données sont chiffrées en amont
- **D.** Complète après un `VACUUM`

Réponse : B

---

Réponses : B, B, B, B, B, B, B, B, A, A, B, B

---

<details>
<summary><b>Corrigé — ne déplier qu'après avoir répondu aux 12 questions</b></summary>

**1 — B.** C'est la différence structurante : Unity Catalog gère les métadonnées dans les
deux cas, mais possède les **fichiers** uniquement pour les tables managées. `DROP` sur
une externe ne supprime que l'entrée du catalogue.

**2 — B.** Corollaire direct de la question 1, et question d'examen classique. Les
fichiers n'ont jamais été supprimés : recréer la table au même chemin ressuscite les
données. Parfois voulu, souvent surprenant.

**3 — B.** Les tables managées bénéficient de la *predictive optimization* : compactage et
`VACUUM` pris en charge par la plateforme. Sur une externe, la maintenance est à ta
charge. C'est la raison principale pour laquelle Databricks recommande le managé par
défaut.

**4 — B.** Cause n°1 des accès qui échouent malgré un `GRANT`. La hiérarchie exige `USE`
sur tous les niveaux supérieurs : sans `USE CATALOG`, on ne peut même pas atteindre le
schéma.

**5 — B.** Le `DENY` est une interdiction explicite, visible dans `SHOW GRANTS`, et il
l'emporte sur tout `GRANT`. Il sert à percer un trou dans un octroi large, à neutraliser
un héritage, et à documenter une interdiction — « n'a pas reçu l'accès » ne se lit nulle
part le jour de l'audit.

**6 — B.** Un propriétaire n'est pas soumis aux `DENY` sur ses propres objets : il ne peut
pas se verrouiller dehors sans recours. Le `DENY` s'applique aux autres membres du
principal visé.

**7 — B.** Le masque est une fonction appliquée **au moment de la lecture**. La donnée en
base n'est pas modifiée, et il n'y a aucune copie. C'est ce qui le distingue d'une table
anonymisée.

**8 — C.** On ne peut poser ni masque ni filtre de lignes sur une **vue**. Cela oriente
toute la conception : les politiques vont sur les tables, les vues en héritent par
transitivité.

**9 — A.** Le comportement dépend entièrement de l'écriture. Une fonction correcte
**ferme par défaut** : un principal inconnu ne voit rien. Écrite dans l'autre sens
(`WHEN NOT autorise THEN masquer ELSE valeur`), un principal absent voit tout en clair —
la sécurité ne doit pas dépendre du sens dans lequel on a écrit un `CASE`.

**10 — B.** Les tables portant certaines politiques de masquage ou de filtrage ne
supportent pas le `MERGE` dans plusieurs cas documentés. Poser un masque sur une table
alimentée par `MERGE` casse le pipeline au passage suivant. On les pose donc sur les
tables reconstruites en `overwrite`.

**11 — B.** L'ABAC s'attache au catalog ou au schéma et s'applique à toute colonne portant
l'étiquette visée, **y compris dans les tables créées demain**. On passe d'un travail par
colonne à un travail par règle. La fonction de masquage reste nécessaire (D est faux).

**12 — B.** Un masque sur une couche dont l'amont est ouvert donne une **illusion de
conformité**, ce qui est pire que pas de masque du tout. Le masquage n'est pas une
politique de sécurité, c'est un mécanisme : la politique consiste à décider qui accède à
quel schéma, et le masque n'affine qu'à l'intérieur de ce qui est déjà autorisé.

</details>
