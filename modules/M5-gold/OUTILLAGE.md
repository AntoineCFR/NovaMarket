# 🧰 Outillage — M5

*Cette fiche dit **avec quoi**, pas **comment**.*

---

## Ce que tu vas faire

Passer d'une couche silver propre mais brute à un socle **que quelqu'un peut interroger
sans te demander comment ça marche**. Modèle en étoile : des dimensions, une table de
faits, et au-dessus les objets qui répondent aux six questions métier de
`docs/02-sources-et-modele.md`.

C'est le premier module où le SQL prend le pas sur PySpark, et c'est voulu : la couche
gold s'écrit dans le langage de ceux qui la consomment.

---

## 1. La dimension temps

Elle ne vient d'aucune source : elle se fabrique.

| Outil | Ce qu'il fait |
|---|---|
| `F.sequence(debut, fin, F.expr("interval 1 day"))` | une suite de dates |
| `F.explode(colonne)` | un tableau → une ligne par élément |
| `F.year` · `F.quarter` · `F.month` · `F.dayofmonth` | découper une date |
| `F.date_format(c, "yyyy-MM")` · `"EEEE"` | formater — motif **Java** |
| `F.expr("...")` | glisser une expression SQL dans du PySpark |

## 2. Les dimensions historisées

Elles viennent de M4. La question à trancher : **la table de faits pointe-t-elle vers la
version courante ou vers celle qui était valide au moment de la commande ?**

| Outil | Ce qu'il fait |
|---|---|
| `.filter("is_current")` | la photo d'aujourd'hui |
| `.join(dim, (cle) & (ts >= valid_from) & (ts < valid_to))` | la version d'alors |
| `F.row_number().over(w)` | fabriquer une clé de substitution |
| `F.broadcast(dim)` | diffuser une dimension petite |

Les deux jointures donnent des chiffres différents. L'écart entre les deux est une des
six questions métier — ce n'est pas un détail technique, c'est le sujet.

## 3. La table de faits

| Outil | Ce qu'il fait |
|---|---|
| `.join(..., how="left")` | rattacher sans perdre de fait |
| `F.coalesce(cle, F.lit(-1))` | un membre « inconnu » plutôt qu'un `NULL` |
| `F.round(c, 2)` · `.cast("decimal(12,2)")` | montants |
| `F.when(...).otherwise(...)` | drapeaux dérivés |

> Une table de faits ne doit **jamais** grossir en joignant une dimension. Compte les
> lignes avant et après chaque jointure. Si le nombre bouge, une clé de dimension n'est
> pas unique — et c'est exactement le piège que tu as déjà croisé en M3 avec la table de
> réparation.

## 4. Les quatre objets gold

Le choix entre eux est un objectif d'examen à part entière.

| Objet | Créé par | Se rafraîchit |
|---|---|---|
| Vue | `CREATE VIEW` | à chaque lecture, jamais périmée, coût à chaque fois |
| Vue matérialisée | `CREATE MATERIALIZED VIEW` | sur commande ou par pipeline, incrémentalement quand la requête s'y prête |
| Table de streaming | `CREATE STREAMING TABLE` | en continu, par ajouts |
| Table classique | `CREATE TABLE AS SELECT` | seulement si tu la recrées |

Trois questions décident : **coût de la requête**, **fréquence de lecture**, **fraîcheur
exigée**. Une fenêtre glissante (« les 90 derniers jours ») matérialisée est périmée le
lendemain — et périmée *sans erreur*, ce qui est pire.

Le complément `COMPLEMENT-objets-gold.md` détaille le tableau.

## 5. Écrire en SQL

| Outil | Ce qu'il fait |
|---|---|
| `spark.sql("""...""")` | requête multi-lignes |
| `.createOrReplaceTempView(nom)` | exposer un DataFrame au SQL |
| `CREATE OR REPLACE TABLE ... AS SELECT` | matérialiser |
| `COMMENT ON TABLE ... IS '...'` | documenter — attendu par le grader |

## 6. Contrôler

| Outil | Ce qu'il fait |
|---|---|
| `.count()` avant / après jointure | détecter le gonflement |
| `.filter(cle.isNull()).count()` | les faits orphelins |
| `.groupBy(...).agg(F.sum(...))` | recouper un total contre silver |

Le CA total de gold doit être **exactement** celui de silver. Un écart d'un centime est un
bug, pas un arrondi.

---

## Les questions auxquelles l'outillage ne répond pas

- Une commission calculée au taux **actuel** du vendeur ou au taux **d'alors** : laquelle
  est juste, et pour qui ?
- Que met-on dans une dimension quand la clé étrangère est orpheline — on jette, on
  invente un membre inconnu, on laisse `NULL` ?

## Le vocabulaire à retenir

**Modèle en étoile** · **clé de substitution** · **dimension à variation lente** ·
**membre inconnu** · **vue / vue matérialisée / table de streaming** · **granularité**.

Section 3 de l'examen — 22 %.
