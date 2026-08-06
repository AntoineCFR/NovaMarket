# 🧰 Outillage — M3

*À lire avant d'ouvrir le notebook. Cette fiche dit **avec quoi**, pas **comment**.*

> **Le module le plus lourd du parcours, et ta priorité n°1.** La section 3 de l'examen
> pèse 22 % et tu y as obtenu 58 % au diagnostic — le plus mauvais rapport poids/maîtrise
> des sept. C'est ici que du temps supplémentaire se justifie par la mesure.

---

## Ce que tu vas faire

Transformer une couche bronze fidèle mais inexploitable en une couche silver typée, où
chaque valeur est celle qu'elle prétend être. Ce qui ne passe pas les règles ne disparaît
pas : il part en quarantaine, avec son motif.

Puis recoller les adresses réparées en M1, et détecter les clés étrangères orphelines
sans les jeter.

---

## 1. Dédupliquer par fenêtre

Le seul concept vraiment nouveau du module. Motif : « garder une seule version par clé ».

```python
from pyspark.sql import Window
```

| Outil | Ce qu'il fait |
|---|---|
| `Window.partitionBy(cles)` | découpe en groupes indépendants |
| `.orderBy(F.col(x).desc())` | ordonne **à l'intérieur** de chaque groupe |
| `F.row_number().over(w)` | numérote 1, 2, 3… dans chaque groupe |
| `.filter("rang = 1")` | ne garde que le premier |

`row_number` contre `rank` contre `dense_rank` : les trois numérotent différemment les
ex æquo. Le choix compte, et l'examen le teste.

## 2. Nettoyer les chaînes

Les prix et quantités arrivent pollués. Il faut décider quoi retirer, et surtout **quoi
ne pas retirer**.

| Outil | Ce qu'il fait |
|---|---|
| `F.trim(c)` · `F.upper(c)` · `F.lower(c)` | espaces, casse |
| `F.regexp_replace(c, motif, remplacement)` | retirer ou substituer par motif |
| `F.length(c)` · `F.ascii(c)` | mesurer, inspecter un caractère |
| `.rlike("^[0-9]+$")` | tester une forme complète |
| `F.split` · `F.size` · `F.array_join` | déjà vus en M1 |

> `F.trim` ne retire **pas** l'espace insécable (U+00A0). Tes prix en contiennent. C'est
> exactement le genre de détail que `repr()` révèle et qu'un `trim` optimiste laisse
> passer — la valeur reste alors non convertible, et ton `cast` la met à `NULL` sans rien
> dire.

## 3. Typer

| Outil | Ce qu'il fait |
|---|---|
| ~~`.cast("int" / "decimal(10,2)")`~~ | ⚠️ **lève une exception** si la conversion échoue — voir ci-dessous |
| `.try_cast("int" / "decimal(10,2)")` | convertir — `NULL` si impossible |
| `F.try_to_timestamp(c, F.lit("yyyy-MM-dd HH:mm:ss"))` | parser une date, **motif Java** — `NULL` si l'entrée ne convient pas |

> ⚠️ **Le mode ANSI est actif sur ce compute.** Contrairement à ce que dit la documentation
> générique de Spark, un `cast` qui échoue ne rend **pas** `NULL` : il lève
> `CAST_INVALID_INPUT` et arrête le notebook — sur la première valeur sale, c'est-à-dire
> exactement celle que ta quarantaine devait attraper.
>
> **La règle est simple : sur de la donnée venant de bronze, jamais `cast`, toujours
> `try_cast`.** Et `try_to_timestamp` au lieu de `to_timestamp`.
>
> Détail dans `docs/01-contraintes-free-edition.md`. Constaté le 5 août 2026.

`yyyy` et non `%Y` · `MM` = mois, `mm` = minutes · `HH` = 24 h, `hh` = 12 h. Une erreur de
casse ici donne des dates fausses, pas une exception.

Le format contractuel est dans `docs/02-sources-et-modele.md`. Une chaîne qui ne le
respecte pas doit partir en quarantaine — **pas** être rattrapée par un second motif.

## 4. Conditions et quarantaine

| Outil | Ce qu'il fait |
|---|---|
| `F.when(cond, val).otherwise(val)` | branchement — sans `otherwise`, c'est `NULL` |
| `F.coalesce(a, b, ...)` | la première valeur non nulle |
| `.isNull()` · `.isNotNull()` · `.isin(...)` | tester |
| `F.array(...)` | construire un tableau de motifs |
| `F.array_compact(...)` | en retirer les `NULL` |
| `F.size(tableau) == 0` | « aucun motif » = ligne saine |

Le motif recommandé : une colonne `quarantine_reasons` de type `array<string>`, calculée
**une fois**, qui sert ensuite à séparer les deux tables. Une ligne peut cumuler plusieurs
motifs — le grader le vérifie.

## 5. Jointures

| Outil | Ce qu'il fait |
|---|---|
| `.join(autre, on="cle", how="left")` | garde toutes les lignes de gauche |
| `how="left_anti"` | les lignes de gauche **sans** correspondance |
| `how="left_semi"` | celles **avec**, sans ramener les colonnes de droite |
| `F.broadcast(petit_df)` | diffuse la petite table, évite un shuffle |
| `.distinct()` | **avant** de joindre sur une clé non unique |

> **Trois pièges, tous vus au diagnostic** :
> 1. Une jointure **interne** pour détecter les orphelins écarterait justement les lignes
>    à signaler. Le CA fondrait de 0,6 % sans un message d'erreur.
> 2. `bronze.orders_address_repair` a **1 087 lignes pour 1 073 clés**. Sans `.distinct()`
>    au préalable, ta table de faits grossit de quatorze lignes.
> 3. Une jointure gauche garantit *au moins* une ligne par ligne de gauche, **jamais
>    exactement une**.

## 6. Agrégations et contrôles

| Outil | Ce qu'il fait |
|---|---|
| `.groupBy(...).agg(...)` | agréger |
| `F.count("*")` contre `F.count("col")` | lignes contre valeurs non nulles — l'écart compte les manquants |
| `F.sum` · `F.avg` · `F.min` · `F.max` | classiques |
| `F.countDistinct` contre `F.approx_count_distinct` | exact et cher, ou approché et rapide |
| ~~`.cache()` / `.unpersist()`~~ | **indisponible sur serverless**. Si un DataFrame sert deux fois, soit tu laisses recalculer, soit tu ecris une table intermediaire |

## 7. Écrire

`.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(nom)` pour les deux
tables. La quarantaine est **recalculée** à chaque passage, donc écrasée — pas ajoutée.

---

## L'invariant du module

`silver.order_line` + `ops.quarantine_order_line` = le nombre de lignes distinctes de
bronze. **Aucune ligne ne disparaît sans laisser d'adresse.** Le grader le vérifie, et
c'est la propriété qui distingue une couche silver d'un `WHERE` silencieux.

## Le vocabulaire à retenir

**Quarantaine explicite** · **fenêtre de déduplication** · **six formes de jointure** ·
**diffusion (*broadcast*)** · **cast silencieux** · **motif de date Java**.

Section 3 — 22 % de l'examen, le plus gros bloc.
