# M10 — Gouvernance et sécurité

**Section 7 de l'examen · 15 % · ~7 questions** — la section la mieux payée du guide.

**Durée estimée** : 3 h · **Prérequis** : M6 validé (les étiquettes `pii` y sont posées)

> 🧰 **Commence par `OUTILLAGE.md`** — les bibliothèques et fonctions dont tu auras besoin,
> sans les réponses. Et `docs/07-python-pour-le-parcours.md` si c'est ton premier passage.

---

## Objectif

Rendre les données personnelles inexploitables pour qui n'a pas à les voir — **sans
dupliquer une seule table** et **sans casser le pipeline**.

La solution paresseuse consiste à créer une copie anonymisée pour les uns et à garder
l'originale pour les autres. Elle double le stockage, double les traitements, et garantit
que les deux versions divergeront. Unity Catalog permet de faire mieux : une seule table,
et le résultat de la requête dépend de qui la pose.

---

## Ce que tu dois produire

| Objet | Rôle |
|---|---|
| `novamarket.ops.access_policy` | Qui a le droit de voir quoi. Une table, pas du code en dur |
| `novamarket.gold.mask_pii` | Fonction de masquage de colonne |
| `novamarket.gold.filter_by_country` | Fonction de filtrage de lignes |
| Masques posés sur `gold.dim_customer` | `first_name`, `last_name`, `email`, `zip_code` |
| Filtre posé sur `gold.fact_order_line` | Sur `shipping_country` |
| `novamarket.ops.privilege_audit` | Photographie des privilèges accordés |

---

## Partie 1 — La hiérarchie des privilèges

Unity Catalog est hiérarchique : `metastore → catalog → schema → table/volume/function`.
Un privilège accordé à un niveau se propage vers le bas, et **il faut `USE` sur tous les
niveaux au-dessus** pour atteindre un objet. C'est la cause n°1 des « je ne vois pas la
table alors qu'on m'a donné les droits ».

Exerce, dans cet ordre, en observant `SHOW GRANTS` après chaque opération :

1. `GRANT USE CATALOG` sur le catalog, puis `GRANT USE SCHEMA` et `GRANT SELECT` sur le
   schéma `gold`, au groupe intégré `account users`.
2. `REVOKE` le `SELECT`, et constate ce qui reste.
3. `DENY SELECT` sur `silver.customer_scd2`.

**Trois questions à trancher en le faisant :**

- Quelle est la différence entre ne pas accorder un privilège et le **refuser**
  explicitement ? Dans quel cas le second sert-il à quelque chose ?
- Le `DENY` que tu viens de poser t'empêche-t-il, **toi**, de lire la table ? Pourquoi ?
- `GRANT SELECT ON SCHEMA` couvre-t-il les tables créées **après** l'octroi ?

> En Free Edition, il n'y a ni SCIM, ni console de compte, donc pas de groupes
> personnalisés. Tu travailleras avec le groupe intégré `account users` et ton propre
> utilisateur. Le mécanisme évalué à l'examen est identique — seule la variété des
> principaux change.

### `ops.privilege_audit`

Capture l'état final. Schéma imposé :

| Colonne | Type |
|---|---|
| `captured_at` | `timestamp` |
| `securable_type` | `string` — `CATALOG`, `SCHEMA`, `TABLE` |
| `securable_name` | `string` |
| `principal` | `string` |
| `privilege` | `string` |
| `action_type` | `string` — `GRANT` ou `DENY` |

Une photographie des privilèges est le premier livrable qu'on te demandera en audit, et
`SHOW GRANTS` ne se conserve pas tout seul.

---

## Partie 2 — Masquage de colonnes

Une fonction de masquage reçoit la valeur de la colonne et renvoie ce que l'appelant a le
droit de voir. Elle est **appliquée à la lecture**, la donnée en base n'est pas modifiée.

Le pilotage ne doit pas être codé en dur dans la fonction. Utilise `ops.access_policy` :

| Colonne | Type |
|---|---|
| `principal` | `string` — le résultat de `current_user()` |
| `can_see_pii` | `boolean` |
| `allowed_country` | `string` — `NULL` signifie « tous les pays » |
| `updated_at` | `timestamp` |

Trois exigences :

1. **Fermeture par défaut.** Un principal absent de la table ne doit **rien** voir en
   clair. Réfléchis à ce que renvoie une sous-requête qui ne trouve aucune ligne, et à ce
   que fait un `CASE WHEN NULL`.
2. Le masque s'applique aux quatre colonnes étiquetées `pii` en M6.
3. La donnée masquée doit rester **jointable** : masquer un identifiant de jointure casse
   les usages légitimes. Regarde bien quelles colonnes tu masques.

### Le piège à connaître

Tu vas être tenté de poser le masque sur `silver.customer_scd2`, qui est la source de
vérité. **Ne le fais pas**, et sache dire pourquoi : cette table est alimentée par un
`MERGE` en M4, et un `MERGE` ne supporte pas les tables portant certaines politiques de
masquage ou de filtrage. Poser un masque là casserait ton pipeline d'historisation au
prochain passage.

Deuxième contrainte de la même famille : **on ne peut pas poser de masque ni de filtre
sur une vue**. Cela oriente la conception — les politiques vont sur les tables, les vues
en héritent.

---

## Partie 3 — Filtrage de lignes

Même principe, appliqué aux lignes : la fonction reçoit la valeur d'une colonne et
renvoie un booléen. Ici, un utilisateur ne voit que les commandes de son pays.

`allowed_country` à `NULL` doit signifier « tous les pays » — sinon un administrateur ne
peut plus rien voir.

### ⚠️ Conséquence opérationnelle

Une fois le filtre posé sur `gold.fact_order_line`, **toutes** les requêtes sur cette
table sont filtrées, y compris celles de tes graders précédents. Si tu laisses
`allowed_country = 'FR'`, le grader de M5 échouera et il aura raison.

Repasse la politique en permissive à la fin du module. C'est exactement le genre d'effet
de bord qu'une politique de sécurité produit en production, et le connaître vaut mieux que
le découvrir.

---

## Partie 4 — ABAC : la même chose, en une fois

Poser un masque colonne par colonne ne passe pas à l'échelle. Les politiques ABAC
s'attachent au **catalog** ou au **schema** et s'appliquent automatiquement à toute
colonne portant une étiquette donnée — y compris aux tables créées demain.

C'est le point de convergence avec M6 : les étiquettes `pii` que tu as posées deviennent
le critère de la politique. Le travail de gouvernance déjà fait sert directement.

Écris la politique. Si la fonctionnalité n'est pas disponible sur ton workspace, le
grader passe le critère en avertissement — mais lis la fiche de décision : c'est un
objectif explicite du guide, donc un sujet de QCM probable.

---

## Partie 5 — Fiche : tables managées et externes

📖 **Non reproductible en Free Edition** — les tables externes exigent un
*external location*, donc un compte cloud. Traité en fiche de décision dans
`modules/M10-gouvernance/FICHE-tables-managees-externes.md`, avec QCM.

---

## Critères d'acceptation

Les critères sont **comportementaux** : le grader ne se contente pas de vérifier que la
politique est déclarée, il bascule `ops.access_policy` et observe le résultat des
requêtes. Ils ne dépendent donc d'aucun comptage, et restent valables quelle que soit la
vague de données ingérée.

| # | Critère |
|---|---|
| 1 | `ops.access_policy` existe avec le schéma imposé |
| 2 | Les fonctions `mask_pii` et `filter_by_country` existent |
| 3 | Les 4 colonnes de `gold.dim_customer` portent un masque |
| 4 | `gold.fact_order_line` porte un filtre de lignes |
| 5 | **Comportement** : `can_see_pii = false` → aucun e-mail lisible |
| 6 | **Comportement** : `can_see_pii = true` → e-mails lisibles |
| 7 | **Fermeture par défaut** : principal absent de la table → aucun e-mail lisible |
| 8 | **Comportement** : `allowed_country = 'FR'` → uniquement des lignes `FR`, et strictement moins que le total |
| 9 | **Comportement** : `allowed_country = NULL` → tous les pays reviennent |
| 10 | Aucun masque sur `silver.customer_scd2` |
| 11 | `ops.privilege_audit` : schéma exact, au moins 3 privilèges capturés dont un `DENY` |
| 12 | La politique est laissée en état permissif à la fin |
| 13 | Politique ABAC déclarée *(avertissement si la fonctionnalité est indisponible)* |

---

## Questions

1. Masque de colonne, vue filtrée, table dupliquée anonymisée : trois façons de répondre
   au même besoin. Compare-les sur quatre axes — coût de stockage, coût de maintenance,
   risque de divergence, granularité du contrôle.
2. Ta fonction de masquage interroge `ops.access_policy` à chaque ligne lue. Qu'est-ce
   que ça coûte, et à partir de quel volume ça devient un problème ?
3. Qui peut modifier `ops.access_policy` ? Si la réponse est « tous ceux qui ont accès au
   schéma `ops` », tu as construit une porte blindée avec la clé sur la serrure. Corrige.
4. Un `DENY` posé sur une table ne t'empêche pas de la lire. Dans quelles situations
   réelles le `DENY` sert-il donc à quelque chose ?
5. Tu as masqué `email` sur `gold.dim_customer`. La même adresse est-elle encore lisible
   ailleurs dans ton catalog ? Cherche vraiment avant de répondre.
