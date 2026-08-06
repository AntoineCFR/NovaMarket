# M6 — Qualité de données, métadonnées et observabilité

**Durée estimée** : 3 h 30 · **Prérequis** : M5 validé

> 🧰 **Commence par `OUTILLAGE.md`** — les bibliothèques et fonctions dont tu auras besoin,
> sans les réponses. Et `docs/07-python-pour-le-parcours.md` si c'est ton premier passage.

---

## Objectif

Tu as construit cinq couches. Aucune ne sait dire si elle va bien.

Ce module ne produit aucune donnée métier nouvelle. Il produit ce qui permet de
**faire confiance** aux données existantes : des mesures, des seuils, une trace, et des
métadonnées qui survivent à ton départ.

Trois livrables, trois idées différentes :

| Livrable | Répond à |
|---|---|
| `ops.dq_metrics` | « Mon pipeline va-t-il bien **aujourd'hui** ? » |
| `ops.contract_violations` | « La source respecte-t-elle ce qu'elle a promis ? » |
| `ops.dq_rescued_summary` | « Qu'est-ce que j'ai sauvé, et pourquoi ? » |

Plus la documentation Unity Catalog, qui répond à « quelqu'un d'autre peut-il utiliser
ces tables sans moi ? ».

---

## Partie 1 — `ops.dq_metrics`

Un contrôle de qualité, c'est quatre choses : une mesure, un seuil, une comparaison, un
verdict. Si l'une manque, ce n'est pas un contrôle — c'est un affichage.

### Schéma imposé

| Colonne | Type |
|---|---|
| `run_id` | `string` |
| `measured_at` | `timestamp` |
| `layer` | `string` (`bronze`, `silver`, `gold`, `ops`) |
| `table_name` | `string` |
| `check_name` | `string` |
| `metric_value` | `double` |
| `threshold` | `double` |
| `comparison` | `string` — `<=`, `>=` ou `==` |
| `status` | `string` — `PASS`, `WARN` ou `FAIL` |

`status` est **calculé** à partir de `metric_value`, `threshold` et `comparison`. Pas
écrit à la main.

### Les 16 contrôles imposés

| `table_name` | `check_name` | `metric_value` |
|---|---|---|
| `bronze.orders_raw` | `row_count` | 287 785 |
| `bronze.orders_raw` | `truncated_rows` | 1 087 |
| `bronze.events_raw` | `row_count` | 131 068 |
| `bronze.events_raw` | `malformed_rows` | 389 |
| `bronze.ref_products_raw` | `row_count` | 8 000 |
| `bronze.app_customers_raw` | `row_count` | 25 971 |
| `silver.order_line` | `row_count` | 282 104 |
| `silver.order_line` | `duplicate_keys` | 0 |
| `silver.order_line` | `null_unit_price` | 0 |
| `silver.order_line` | `orphan_customer_rows` | 1 721 |
| `silver.order_line` | `orphan_product_rows` | 545 |
| `ops.quarantine_order_line` | `row_count` | 2 229 |
| `silver.seller_scd2` | `multiple_current_versions` | 0 |
| `silver.seller_scd2` | `chain_breaks` | 0 |
| `gold.fact_order_line` | `row_count` | 282 104 |
| `gold.fact_order_line` | `orphan_seller_sk` | 0 |

`malformed_rows` se définit comme `event_id IS NULL` : un enregistrement dont rien n'a pu
être lu. **Ne le conditionne pas à `_rescued_data`** — cette colonne est vide sur les
événements aussi. Le texte d'origine de ces lignes vit dans `_corrupt_record` (M1,
étape 2), et un contrôle bâti sur la mauvaise colonne vaudrait 0 en passant au vert.

`truncated_rows` se mesure sur **`bronze.orders_raw`**, avant réparation : c'est le
volume de dégâts causés par le lecteur CSV, et il ne peut pas se lire dans
`_rescued_data`, qui est vide sur les commandes (M1, étape 5). La seule trace est
l'adresse amputée de sa virgule.

Deux raisons de le suivre alors que M1 le répare déjà :

- **Une réparation se surveille.** Si le volume de lignes tronquées grimpe d'un coup, la
  source a changé et la passe de réparation ne suffira peut-être plus.
- **La métrique n'existe que parce que quelqu'un a trouvé comment rendre le défaut
  visible.** C'est le vrai travail d'une couche qualité — pas de compter ce qui se compte
  tout seul.

Contrôle-miroir à ajouter côté silver, où la réparation doit avoir tout recollé. Attention
au prédicat : une adresse **réparée** ne ressemble pas à une adresse saine.

| Cas | Exemple | Reconnaissable à |
|---|---|---|
| saine | `130 quai des Chartrons, 28001 Madrid` | une **virgule** |
| réparée | `38 quai des Chartrons; Batiment D; 80331 Munich` | un **point-virgule** |
| tronquée | `38 quai des Chartrons` | **ni l'un ni l'autre** |

`silver.order_line` ne doit contenir aucune adresse du troisième type. Un contrôle qui
cherche seulement la virgule signalerait les 1 087 lignes réparées comme défectueuses —
et tu passerais la journée à chercher un bug qui n'existe pas.

### Écris-les une fois

Seize contrôles écrits à la main, c'est seize occasions de se tromper et zéro
réutilisation. Structure-les : une description déclarative (table, nom, requête, seuil,
comparaison) et **une** boucle qui exécute et écrit. Tu ajouteras le dix-septième en une
ligne.

### Sur les seuils

Les six contrôles à zéro (`duplicate_keys`, `null_unit_price`, `chain_breaks`…) sont des
**invariants** : leur seuil est 0, et un dépassement est un `FAIL`.

Les autres sont des **observations**. `orphan_customer_rows = 1 721` n'est ni bon ni
mauvais dans l'absolu. Choisis des seuils et **assume-les par écrit** dans le notebook :
un seuil qu'on ne sait pas justifier est un seuil qu'on désactivera à la première alerte.

---

## Partie 2 — `ops.contract_violations`

Depuis M1, je t'ai demandé trois fois de noter les écarts entre `docs/02-sources-et-modele.md`
et la réalité. C'est le moment de les matérialiser.

La différence avec `dq_metrics` est de nature : `dq_metrics` surveille **ton** pipeline,
`contract_violations` documente ce que **la source** t'impose. L'un se corrige en
changeant ton code, l'autre en allant voir l'équipe d'en face.

### Schéma imposé

`rule_code` (`string`), `source_name` (`string`), `rule_text` (`string`),
`scope_rows` (`bigint`), `violation_rows` (`bigint`), `violation_rate` (`double`),
`status` (`string`), `checked_at` (`timestamp`).

### Les 6 règles imposées

Portée des règles de valeur : les lignes bronze **dédupliquées** (284 333 pour les
commandes, 130 025 pour les événements). Seule la règle d'unicité se mesure sur le brut,
par définition.

| `rule_code` | Ce que le contrat promet | `violation_rows` |
|---|---|---|
| `ORDER_LINE_ID_UNIQUE` | `order_line_id` est une clé unique | **3 452** |
| `ORDER_TS_PARSABLE` | `order_ts` au format `yyyy-MM-dd HH:mm:ss` | **1 422** |
| `QUANTITY_POSITIVE` | `quantity` est un entier > 0 | **813** |
| `UNIT_PRICE_NUMERIC` | `unit_price` est un décimal à virgule | **1 102** |
| `CURRENCY_ALWAYS_EUR` | `currency` vaut toujours `EUR` | **0** |
| `EVENT_TS_ISO8601` | `event_ts` est une chaîne ISO 8601 | **1 267** |

`CURRENCY_ALWAYS_EUR` à zéro n'est pas une ligne inutile. C'est la seule promesse que la
source tient, et la seule sur laquelle tu peux t'appuyer sans filet. Un contrôle qui
passe est une information ; un contrôle absent n'en est pas une.

> `EVENT_TS_ISO8601` vaut 1 267 sur les événements dédupliqués, et 1 269 si on compte
> les lignes brutes. Deux chiffres justes, deux portées différentes — d'où l'obligation
> d'écrire `scope_rows` à côté de `violation_rows`.

---

## Partie 3 — `ops.dq_rescued_summary`

Le bilan de ce que l'ingestion a abîmé, par table et par cause — que le mécanisme l'ait
rattrapé ou non.

Colonnes : `table_name`, `rescue_reason`, `n_rows`, `example_value`, `summarized_at`.

Deux lignes au minimum, avec ces valeurs exactes :

| `table_name` | `rescue_reason` | `n_rows` | D'où vient le compte |
|---|---|---|---|
| `bronze.orders_raw` | `EXTRA_COLUMNS` | **1 087** | adresse sans virgule — **pas** `_rescued_data`, qui est vide |
| `bronze.events_raw` | `MALFORMED_JSON` | **389** | `event_id IS NULL` · texte brut dans `_corrupt_record` |

Cette table est le seul endroit du parcours où les deux mondes se retrouvent : un défaut
que la plateforme a su capturer, et un défaut qu'elle a laissé passer en silence. Les
deux méritent la même ligne dans un bilan de qualité — c'est même la raison d'être d'un
bilan de qualité.

Un bilan honnête distingue par ailleurs le défaut **subi** du défaut **résiduel**. Ces
1 087 lignes sont réparées depuis M1 : elles disent ce que la source coûte à traiter, pas
ce qui manque en aval. Si tu ajoutes une colonne à cette table, `is_repaired` est celle
qui a le plus de valeur pour quelqu'un qui la lira sans connaître l'histoire.

Si ton bronze a envoyé les horodatages epoch au rescue (voir M1), ajoute la ligne
correspondante — le grader ne l'exige pas, mais ton bilan serait faux sans elle.

---

## Partie 4 — Métadonnées Unity Catalog

### Commentaires

**Toutes** les tables managées de `silver` et de `gold` doivent porter un commentaire non
vide. Le grader les énumère via `information_schema.tables` et vérifie. Les vues ne sont
pas exigées — mais `COMMENT ON VIEW` existe, et une vue exposée aux analystes sans
commentaire est un aussi mauvais livrable qu'une table.

Un commentaire utile dit ce que la table **n'est pas** et quels pièges elle contient.
« Table des lignes de commande » n'apprend rien à personne. « Une ligne = un produit
dans une commande ; le CA ne se somme que sur `is_revenue`; 1 721 lignes pointent vers
un client absent du référentiel » évite trois erreurs.

### Étiquettes

Étiquette les colonnes personnelles avec `pii = true` :

- `silver.customer_scd2` : `first_name`, `last_name`, `email`, `zip_code`
- `gold.dim_customer` : les mêmes

```sql
ALTER TABLE novamarket.gold.dim_customer
ALTER COLUMN email SET TAGS ('pii' = 'true')
```

Puis vérifie ton travail :

```sql
SELECT * FROM novamarket.information_schema.column_tags WHERE tag_name = 'pii'
```

> Selon ton workspace, `information_schema.column_tags` peut ne pas être exposé. Le
> grader traite ce critère en avertissement, pas en échec.

### Lineage

Non évalué, mais fais-le : ouvre `gold.fact_order_line` dans Catalog Explorer, onglet
**Lineage**. Tu dois voir remonter la chaîne jusqu'aux fichiers du volume.

Demande-toi ensuite : ce graphe montre-t-il que `commission_rate` vient de
`bronze.app_sellers_raw` via une jointure temporelle ? Le lineage automatique s'arrête
au niveau des tables. La sémantique reste dans tes commentaires — c'est pour ça qu'ils
ne sont pas optionnels.

---

## Critères d'acceptation

| # | Critère | Attendu |
|---|---|---|
| 1 | `ops.dq_metrics` : schéma exact | voir tableau |
| 2 | 16 contrôles sur la dernière exécution | **16** |
| 3 | Chaque contrôle a la `metric_value` attendue | 16/16 |
| 4 | `status` cohérent avec `metric_value`, `threshold` et `comparison` | **0 incohérence** |
| 5 | Les 6 invariants à zéro sont en `PASS` | **6** |
| 6 | `ops.contract_violations` : schéma exact | — |
| 7 | Les 6 règles présentes avec le bon `violation_rows` | 6/6 |
| 8 | `CURRENCY_ALWAYS_EUR` présente et à 0 | — |
| 9 | `violation_rate` cohérent avec `violation_rows / scope_rows` | **0 écart** |
| 10 | `ops.dq_rescued_summary` : les 2 lignes imposées | — |
| 11 | Toutes les tables `silver` portent un commentaire | **0 sans** |
| 12 | Toutes les tables `gold` portent un commentaire | **0 sans** |
| 13 | Colonnes étiquetées `pii` | ≥ 8 *(avertissement si `information_schema` indisponible)* |

---

## Questions

1. `dq_metrics` mesure l'état courant. Comment détecterais-tu une **dérive** — un taux
   d'orphelins qui passe de 0,6 % à 0,9 % — que ce modèle ne voit pas ?
2. Quel est le bon moment pour exécuter ces contrôles : avant l'écriture de la table,
   après, ou les deux ? Qu'est-ce que chaque choix rend possible ou impossible ?
3. Un contrôle en `FAIL` doit-il arrêter le pipeline ? Distingue les cas — tu en as au
   moins trois dans ce projet.
4. `contract_violations` compte 3 452 violations d'unicité. En pratique, qu'est-ce que
   tu envoies à l'équipe source, et sous quelle forme ?
5. Étiqueter `email` en `pii` ne protège rien par soi-même. Qu'est-ce que cette
   étiquette permet ensuite, concrètement, sur Unity Catalog ?
