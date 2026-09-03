# 5 · Agrégation et objets de publication

Deux sujets, un même fil : **que promet-on à celui qui lira ?**

---

## Réduire ou annoter

| | Agrégation | Fenêtre |
|---|---|---|
| Ce qu'elle fait | plusieurs lignes → **une** | chaque ligne reçoit un calcul sur son groupe |
| Le détail | **détruit** | conservé |
| Quand | un total par catégorie | un cumul, un rang, une part |

```python
# réduire
(cmd.groupBy("region", "mois")
    .agg(F.sum("montant").alias("ca"),
         F.count("*").alias("lignes"),
         F.count("montant").alias("renseignes"),      # ignore les absences
         F.countDistinct("id_cli").alias("clients"))
    .filter(F.col("ca") > 100_000))                   # remplace HAVING

# annoter
w = Window.partitionBy("id_cli").orderBy("jour")
cumul = w.rowsBetween(Window.unboundedPreceding, 0)

cmd.withColumns({
  "rang":      F.row_number().over(w),
  "veille":    F.lag("montant", 1, 0).over(w),
  "ca_cumule": F.sum("montant").over(cumul),
  "part":      F.col("montant") / F.sum("montant").over(Window.partitionBy("id_cli")),
})
```

### Toujours nommer

Sans `alias`, tes colonnes s'appellent `sum(montant)`. Six mois plus tard, le tableau de
bord qui s'appuyait dessus tombe au premier changement, et personne ne relie la panne.
Un agrégat nommé **se filtre ensuite comme une colonne ordinaire**.

### Ce que comptent les fonctions de comptage

```python
F.count("*")            # les lignes
F.count("montant")      # les valeurs RENSEIGNÉES — ignore les absences
```

**L'écart entre les deux est exactement le nombre d'absences** — la mesure de complétude
la plus simple qui existe :

```python
(F.count("montant") / F.count("*")).alias("completude")
```

`sum`, `avg`, `min`, `max` ignorent tous les absences. La moyenne de trois valeurs dont
une est absente se calcule sur deux.

| Fonction | À savoir |
|---|---|
| `F.count_if(cond)` | plus lisible qu'un `when` imbriqué |
| `F.countDistinct(a, b)` | brassage complet, coûteux |
| `F.approx_count_distinct(c, 0.01)` | second argument : l'erreur admise |
| `F.percentile_approx(c, 0.5, 10000)` | troisième argument : la précision |
| `F.collect_set` · `F.collect_list` | **tout le groupe en mémoire** : dangereux |

### Le cadre d'une fenêtre — la surprise durable

```python
w = Window.partitionBy("id").orderBy("jour")
F.last_value("m").over(w)     # rend la LIGNE COURANTE, pas la dernière du groupe
```

Une fenêtre **triée** a un cadre implicite qui va du début du groupe **jusqu'à la ligne
courante**. Sans tri, le cadre couvre toute la partition. Dès qu'une fonction est
sensible aux bornes, **écris le cadre** :

```python
partition = w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
cumul     = w.rowsBetween(Window.unboundedPreceding, 0)
sept_obs  = w.rowsBetween(-6, 0)      # six voisines : compte de lignes
sept_jours = w.rangeBetween(-6, 0)    # ex æquo compris : valeur de la colonne de tri
```

| Fonction | Sur ex æquo |
|---|---|
| `F.row_number()` | 1, 2, 3 — **une seule survivante** |
| `F.rank()` | 1, 1, 3 — « les 3 premiers » peut en rendre 5 |
| `F.dense_rank()` | 1, 1, 2 |

Et jamais de fenêtre **sans `partitionBy`** sur un gros volume : tout se rassemble sur un
seul exécuteur.

### Le grain

Avant d'agréger, écris en une phrase ce que représentera une ligne du résultat. Une table
au grain pays-mois se somme par pays sans problème — mais en calculer la moyenne donne
**la moyenne des mois**, qui n'est pas la moyenne des ventes.

---

## Les quatre objets de publication

Ils ne diffèrent que par **le moment où le calcul a lieu**.

| Objet | Calcul | Lecture | Fraîcheur |
|---|---|---|---|
| **Table** | lors d'un traitement explicite | minimale | celle du dernier traitement |
| **Vue** | à chaque lecture | le coût de la requête, à chaque fois | toujours à jour |
| **Vue matérialisée** | au rafraîchissement | minimale | celle du dernier rafraîchissement |
| **Table de flux** | en continu, à l'arrivée | minimale | quelques minutes |

```sql
CREATE OR REPLACE VIEW gold.v_ca AS
  SELECT region, sum(montant) AS ca FROM gold.faits GROUP BY region;

CREATE MATERIALIZED VIEW gold.mv_ca AS
  SELECT region, sum(montant) AS ca FROM gold.faits GROUP BY region;

CREATE OR REFRESH STREAMING TABLE gold.st_evt AS
  SELECT * FROM STREAM(silver.evenement);

CREATE OR REPLACE TABLE gold.t_ca AS SELECT * FROM gold.v_ca;
```

### Trancher : coût × fréquence

> **On matérialise ce qui est lu bien plus souvent qu'il n'est écrit, et jamais l'inverse.**

Une agrégation de 40 secondes lue **12 fois par jour** : 8 minutes de calcul quotidien en
vue simple, contre 2 heures de rafraîchissement en vue matérialisée. **La vue gagne.**

La même lue **200 fois par jour** : 2 h 20 en vue simple contre 2 h fixes, et une réponse
en une seconde au lieu de quarante. **La matérialisée gagne.**

La question à poser au demandeur n'est pas *« voulez-vous que ce soit rapide »* mais
**« combien de fois par jour cette requête sera-t-elle exécutée »**.

### Table de flux : un seul critère

**Les lignes anciennes peuvent-elles être modifiées en source ?** Si oui, une table de
flux ne le verra **jamais** — elle traite chaque enregistrement une fois. Il faut une vue
matérialisée, qui recalcule.

### Le piège de la définition relative

```sql
-- PIÈGE : la borne est figée au calcul, la fenêtre glisse en silence
CREATE MATERIALIZED VIEW gold.ca_90j AS
  SELECT sum(montant) AS ca FROM gold.faits
  WHERE date_vente >= current_date() - INTERVAL 90 DAYS;

-- JUSTE : matérialiser à un grain ABSOLU, laisser le lecteur choisir sa fenêtre
CREATE MATERIALIZED VIEW gold.ca_jour AS
  SELECT date_vente, sum(montant) AS ca FROM gold.faits GROUP BY date_vente;
```

Aucune erreur, aucune valeur aberrante, aucun écart de volume. L'indicateur porte
simplement sur une période vieille de plusieurs mois.

---

## Encapsuler une règle

Tant qu'une règle est recopiable, elle sera recopiée — et corrigée dans onze tableaux de
bord sur quatorze.

```sql
CREATE OR REPLACE FUNCTION gold.ttc(ht DECIMAL(12,2))
  RETURNS DECIMAL(12,2) RETURN ht * 1.20;

SELECT gold.ttc(montant) FROM gold.faits;

-- la vue paramétrée : une requête réutilisable dont on fixe l'argument à l'appel
CREATE OR REPLACE FUNCTION gold.cmd_du_mois(m STRING)
  RETURNS TABLE (id STRING, montant DECIMAL(12,2))
  RETURN SELECT id_cmd, montant FROM gold.faits
         WHERE date_format(jour, 'yyyy-MM') = m;

SELECT * FROM gold.cmd_du_mois('2026-03');
```

Une fonction **SQL** est optimisée comme du SQL ordinaire ; une fonction Python force le
moteur à sortir de son mode efficace.

---

## Publier, c'est promettre

Sur la **structure** — renommer ou supprimer une colonne casse quelque chose chez
quelqu'un. Consulte le lignage **avant**, pas après.

Sur la **sémantique** — changer la règle de calcul d'une colonne sans changer son nom ne
casse rien techniquement, et fausse tout ce qui en dépend. Personne ne le verra.

Sur la **fraîcheur** — à quelle heure la donnée de la veille est-elle disponible ? En
l'absence de promesse affichée, chacun se construit une croyance optimiste.

**Traite une table publiée comme une interface** : ajouter sans casser, ne jamais retirer
sans préavis, publier une nouvelle version à côté de l'ancienne quand le fond change.
