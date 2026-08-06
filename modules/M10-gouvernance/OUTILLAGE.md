# 🧰 Outillage — M10

*Cette fiche dit **avec quoi**, pas **comment**.*

> **Section 7 — 15 % de l'examen.** Tu y as fait 83 % au diagnostic **sans avoir rien
> travaillé**. C'est ta meilleure section : ce module vise donc les deux points précis que
> tu as ratés — les restrictions (`MERGE`, vues) plutôt que les bases.

---

## Ce que tu vas faire

Décider qui voit quoi. Pas au niveau de la table — au niveau de la **colonne** et de la
**ligne**, et sans dupliquer la donnée.

Tout ce module s'écrit en SQL.

---

## 1. Les privilèges

| Commande | Ce qu'elle fait |
|---|---|
| `GRANT <privilege> ON <objet> TO <principal>` | accorder |
| `REVOKE <privilege> ON <objet> FROM <principal>` | retirer un octroi |
| `DENY <privilege> ON <objet> TO <principal>` | **interdire explicitement** |
| `SHOW GRANTS ON <objet>` | lister |

Privilèges usuels : `USE CATALOG`, `USE SCHEMA`, `SELECT`, `MODIFY`, `CREATE TABLE`,
`EXECUTE`, `ALL PRIVILEGES`.

> **La cause n°1 des accès qui échouent malgré un `GRANT SELECT`** : il manque `USE
> CATALOG` et `USE SCHEMA`. La hiérarchie exige un droit de traversée à **chaque** niveau
> supérieur. Tu as eu cette question juste au diagnostic — elle revient à l'examen.

`DENY` n'est pas « ne pas accorder ». Il est **explicite**, visible dans `SHOW GRANTS`, et
il **l'emporte sur tout `GRANT`**, y compris hérité. Il sert à percer un trou dans un
octroi large — et à documenter une interdiction, ce que « n'a jamais reçu l'accès » ne
fait pas le jour de l'audit.

## 2. Masquer une colonne

| Élément | Ce qu'il fait |
|---|---|
| `CREATE FUNCTION nom(param TYPE) RETURNS TYPE RETURN <expression>` | la fonction de masquage |
| `ALTER TABLE ... ALTER COLUMN col SET MASK nom` | l'appliquer |
| `ALTER TABLE ... ALTER COLUMN col DROP MASK` | la retirer |
| `is_account_group_member('groupe')` | tester l'appelant |
| `current_user()` | l'identité de l'appelant |

Le masque est une fonction évaluée **à la lecture**. La donnée en base n'est pas modifiée
et il n'y a aucune copie — c'est ce qui le distingue d'une table anonymisée.

> **Écris-la fermée par défaut.** `CASE WHEN autorise THEN valeur ELSE masque END` et
> `CASE WHEN NOT autorise THEN masque ELSE valeur END` se comportent différemment quand
> l'appelant est **inconnu**. La sécurité ne doit pas dépendre du sens dans lequel tu as
> écrit un `CASE`.

## 3. Filtrer des lignes

| Élément | Ce qu'il fait |
|---|---|
| `CREATE FUNCTION nom(col TYPE) RETURNS BOOLEAN RETURN <predicat>` | le filtre |
| `ALTER TABLE ... SET ROW FILTER nom ON (colonne)` | l'appliquer |
| `ALTER TABLE ... DROP ROW FILTER` | le retirer |

## 4. ABAC

| Élément | Ce qu'il fait |
|---|---|
| étiquettes gouvernées | posées en M6 sur les colonnes `pii` |
| politique attachée à un catalog ou un schéma | s'applique à **toute** colonne portant l'étiquette |

L'intérêt : les tables créées **demain** sont couvertes sans intervention. On passe d'un
travail par colonne à un travail par règle. La fonction de masquage reste nécessaire.

> Disponibilité variable selon les workspaces. Le grader traite ce point en avertissement,
> pas en échec — si ça ne marche pas chez toi, note-le et passe.

## 5. Vérifier

| Outil | Ce qu'il fait |
|---|---|
| `information_schema.column_masks` | les masques posés |
| `information_schema.row_filters` | les filtres posés |
| `SHOW GRANTS ON ...` | les droits effectifs |

---

## Les deux restrictions que tu as ratées au diagnostic

**On ne pose ni masque ni filtre sur une vue.** Les politiques vont sur les **tables** ;
les vues en héritent par transitivité. Ça oriente toute la conception.

**Une table portant certaines politiques ne supporte plus `MERGE`** dans plusieurs cas
documentés. Poser un masque sur une table alimentée par `MERGE` casse le pipeline au
passage suivant. On les pose donc sur les tables reconstruites en `overwrite` — ce qui,
dans ton pipeline, exclut les tables SCD2 de M4.

## La question qui compte

Un masque sur `gold.dim_customer.email` ne protège rien si `silver.customer_scd2` reste
lisible par les mêmes personnes. **Le masquage n'est pas une politique de sécurité, c'est
un mécanisme.** La politique consiste à décider qui accède à quel schéma ; le masque
n'affine qu'à l'intérieur de ce qui est déjà autorisé.

## Le vocabulaire à retenir

**`GRANT` / `REVOKE` / `DENY`** · **traversée `USE`** · **masque de colonne** · **filtre
de lignes** · **fermé par défaut** · **ABAC** · **propriétaire d'objet**.

Section 7 — 15 %.
