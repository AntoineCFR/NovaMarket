# M5 — Silver → Gold : modèle en étoile et données business-ready

**Durée estimée** : 4 h 30 · **Prérequis** : M3 et M4 validés

> 🧰 **Commence par `OUTILLAGE.md`** — les bibliothèques et fonctions dont tu auras besoin,
> sans les réponses. Et `docs/07-python-pour-le-parcours.md` si c'est ton premier passage.

---

## Objectif

Livrer un socle sur lequel un analyste répond aux six questions métier **sans te
demander d'aide** et **sans pouvoir se tromper**.

C'est le critère qui distingue une couche gold d'un tas de tables agrégées : si
répondre correctement suppose de connaître une subtilité que tu es le seul à connaître,
la subtilité n'est pas dans le modèle, elle est dans ta tête — et le jour où quelqu'un
d'autre écrit la requête, le chiffre est faux.

La subtilité en question, ici, tu la connais déjà : le taux de commission dépend du plan
du vendeur **à la date de la commande**. Un analyste qui joint naïvement sur le vendeur
courant se trompe de **45 765,97 €**. Ton modèle doit rendre cette erreur impossible.

---

## Le modèle

```
                 dim_date          dim_customer
                     \                  /
                      \                /
   dim_product ---- fact_order_line ---- dim_seller  (SCD2, clé de version)
                            |
                    ref_commission_plan
```

### `gold.dim_date`

Calendrier du 2025-12-01 au 2026-06-30. **212 lignes.**

| Colonne | Type |
|---|---|
| `date_key` | `date` |
| `year` | `int` |
| `month` | `int` |
| `year_month` | `string` (`yyyy-MM`) |
| `day_of_month` | `int` |
| `day_of_week` | `int` (1 = lundi) |
| `day_name` | `string` |
| `is_weekend` | `boolean` |
| `quarter` | `int` |

### `gold.dim_customer` — SCD1, état courant

Depuis `silver.customer_scd2` filtré sur `is_current`. **25 080 lignes.**

`customer_id`, `first_name`, `last_name`, `email`, `country`, `city`, `zip_code`,
`segment` (`string`), `is_opt_in`, `is_deleted` (`boolean`), `created_at` (`timestamp`),
`_processed_at` (`timestamp`).

### `gold.dim_seller` — SCD2, toutes les versions

Depuis `silver.seller_scd2`, **toutes** les lignes. **665 lignes.**

Colonnes du SCD2, plus en première position :

| Colonne | Type | Définition |
|---|---|---|
| `seller_sk` | `string` | `concat(seller_id, '#', date_format(valid_from, 'yyyyMMddHHmmss'))` |

Cette clé de substitution est imposée et **déterministe** : elle identifie une *version*
de vendeur, pas un vendeur. C'est elle qui porte l'historisation jusque dans le fait.

> Un `monotonically_increasing_id()` ferait aussi l'affaire fonctionnellement, mais
> changerait à chaque reconstruction de la dimension — et casserait tous les faits déjà
> écrits. Une clé de substitution doit être stable ou gérée par un registre persistant.
> Ici, on la dérive du contenu : plus simple, et rejouable.

### `gold.dim_product`

Depuis `bronze.ref_products_raw` joint aux catégories. **8 000 lignes.**

`product_id`, `product_name`, `brand`, `category_id`, `category_label`,
`top_category_code`, `top_category_label`, `seller_id` (`string`),
`list_price` (`decimal(10,2)`), `is_discontinued` (`boolean`), `_processed_at`.

### `gold.ref_commission_plan`

3 lignes : `plan_code` (`string`), `commission_rate` (`decimal(5,3)`) —
`BASIC` 0.150, `PLUS` 0.115, `PREMIUM` 0.085.

### `gold.fact_order_line`

**282 104 lignes**, grain identique à `silver.order_line`.

| Colonne | Type |
|---|---|
| `order_line_id`, `order_id` | `string` |
| `order_date` | `date` — FK vers `dim_date` |
| `order_ts` | `timestamp` |
| `customer_id` | `string` — FK vers `dim_customer` |
| `seller_sk` | `string` — **FK vers la version de vendeur en vigueur à `order_ts`** |
| `seller_id`, `product_id` | `string` |
| `quantity` | `int` |
| `unit_price`, `discount_amount` | `decimal(10,2)` |
| `gross_amount`, `net_amount` | `decimal(12,2)` |
| `commission_rate` | `decimal(5,3)` |
| `commission_amount` | `decimal(12,2)` |
| `order_status`, `payment_method`, `shipping_country` | `string` |
| `is_revenue`, `is_orphan_customer`, `is_orphan_product` | `boolean` |
| `_processed_at` | `timestamp` |

**Règle de commission** : `commission_amount = round(net_amount * commission_rate, 2)`
si `is_revenue`, sinon `0.00`. Jamais `null` — une commande annulée génère zéro
commission, ce n'est pas une commission inconnue.

La jointure temporelle respecte la convention `[valid_from, valid_to)` de M4 :

```sql
ON  f.seller_id = d.seller_id
AND f.order_ts >= d.valid_from
AND (d.valid_to IS NULL OR f.order_ts < d.valid_to)
```

> Aucune ligne ne doit perdre son vendeur. Si ton fait passe sous 282 104 lignes, ta
> jointure temporelle a un trou — vérifie le côté `valid_to IS NULL`.

---

## Les agrégats

### `gold.agg_revenue_monthly` — questions 1

Grain : `year_month` × `top_category_code` × `seller_id`. **13 333 lignes.**

Construit **uniquement sur les lignes de chiffre d'affaires** (`is_revenue`). Les
produits orphelins sont regroupés sous `top_category_code = 'UNKNOWN'`.

Colonnes : `year_month`, `top_category_code`, `seller_id`, `net_amount` (`decimal(18,2)`),
`commission_amount` (`decimal(18,2)`), `n_lines` (`bigint`), `n_orders` (`bigint`).

### `gold.agg_funnel_source` — question 5

Grain : `utm_source`. **7 lignes.** Une session est comptée dans une étape si elle
contient au moins un événement de ce type.

Colonnes : `utm_source`, `sessions`, `sessions_product_view`, `sessions_add_to_cart`,
`sessions_checkout_start`, `sessions_purchase` (tous `bigint`), plus les taux de
conversion que tu juges utiles.

### `gold.v_top_products_90d` — question 6

Vue. Top 20 produits par `net_amount` sur les 90 derniers jours, avec leur taux de
retour. Fenêtre : `order_date > date_sub(<date de commande la plus récente>, 90)`,
soit strictement après le **2026-03-04**.

Colonnes minimales : `product_id`, `product_name`, `top_category_code`, `net_amount`,
`n_lines`, `n_returned_lines`, `return_rate`.

### Trois vues pour les questions 2, 3 et 4

Le grader vérifie qu'elles existent et renvoient des lignes ; leur contenu, c'est toi
qui le défends.

| Vue | Question |
|---|---|
| `gold.v_seller_quality_monthly` | Taux d'annulation et de retour par vendeur et par mois |
| `gold.v_basket_by_segment` | Panier moyen et nombre de commandes par segment client et par pays |
| `gold.v_customer_cohort` | Rétention par mois de première commande |

---

## Documentation

Une table gold non documentée n'est pas business-ready. Le grader vérifie que
**`fact_order_line` et ses colonnes portent des commentaires Unity Catalog** :

- un `COMMENT` sur la table ;
- un `COMMENT` sur chacune des colonnes `seller_sk`, `commission_rate`,
  `commission_amount`, `is_revenue`, `net_amount`.

Le commentaire de `seller_sk` doit expliquer que c'est une clé de **version**, pas de
vendeur. C'est précisément l'information qui évite l'erreur à 45 765,97 €.

---

## Critères d'acceptation

| # | Critère | Attendu |
|---|---|---|
| 1 | `dim_date` : lignes | **212** |
| 2 | `dim_customer` : lignes, `customer_id` unique | **25 080** |
| 3 | `dim_seller` : lignes | **665** |
| 4 | `dim_seller` : `seller_sk` unique | **665** |
| 5 | `dim_product` : lignes | **8 000** |
| 6 | `ref_commission_plan` : lignes | **3** |
| 7 | `fact_order_line` : schéma exact | voir tableau |
| 8 | `fact_order_line` : lignes | **282 104** |
| 9 | Aucun `seller_sk` nul | **0** |
| 10 | Tous les `seller_sk` du fait existent dans `dim_seller` | **0 orphelin** |
| 11 | `sum(commission_amount)` | **3 164 343,53** |
| 12 | Commission nulle sur les lignes hors CA | **0 ligne à `commission_amount <> 0`** |
| 13 | Aucun `commission_amount` nul | **0** |
| 14 | `agg_revenue_monthly` : lignes | **13 333** |
| 15 | `agg_revenue_monthly` : `sum(net_amount)` | **24 049 792,86** |
| 16 | `agg_revenue_monthly` : `sum(commission_amount)` | **3 164 343,53** |
| 17 | Décembre 2025 : `net_amount` / `n_lines` | **4 840 958,20** / **47 515** |
| 18 | Juin 2026 : `net_amount` / `n_lines` | **260 610,22** / **2 529** |
| 19 | `agg_funnel_source` : lignes | **7** |
| 20 | Total des sessions | **31 867** |
| 21 | Total des sessions avec achat | **1 873** |
| 22 | `v_top_products_90d` : lignes | **20** |
| 23 | Produit en tête | **P007960**, `net_amount` **9 571,30** |
| 24 | Les trois vues des questions 2, 3, 4 existent et ne sont pas vides | — |
| 25 | `fact_order_line` porte un commentaire de table | — |
| 26 | Les 5 colonnes clés portent un commentaire | — |

---

## Le test qui compte

Avant de lancer le grader, exécute cette requête :

```sql
SELECT
    round(sum(f.commission_amount), 2)                     AS commission_historisee,
    round(sum(f.net_amount * c.commission_rate), 2)        AS commission_plan_courant
FROM novamarket.gold.fact_order_line f
JOIN novamarket.gold.dim_seller d ON f.seller_id = d.seller_id AND d.is_current
JOIN novamarket.gold.ref_commission_plan c ON d.plan_code = c.plan_code
WHERE f.is_revenue
```

Les deux colonnes doivent afficher **3 164 343,53** et **3 118 577,56**.

Si elles sont égales, ton `seller_sk` ne résout pas la bonne version et tout le module
est à refaire. Si elles diffèrent de 45 765,97 €, ton modèle fait son travail : il rend
l'erreur *visible* pour qui la cherche, et *impossible* pour qui utilise `seller_sk`.

---

## Questions

1. Pourquoi `dim_customer` en SCD1 et `dim_seller` en SCD2, alors que les deux sources
   sont historisées en silver ? Qu'est-ce qui justifie ce traitement asymétrique, et
   qu'est-ce qu'on perd ?
2. 1 721 lignes du fait pointent vers un client absent de `dim_customer`. Comment un
   modèle en étoile gère-t-il ça proprement ? Compare deux ou trois approches.
3. `agg_revenue_monthly` compte 13 333 lignes pour 282 104 lignes de fait — un rapport
   de 1 à 21. Cet agrégat vaut-il le coût de sa maintenance ? À partir de quel rapport
   la question se pose-t-elle vraiment ?
4. Le taux de commission est figé dans une table de référence sans historisation. Que se
   passe-t-il le jour où NovaMarket change ses taux ? Que faudrait-il changer ?
5. `v_top_products_90d` est une vue, `agg_revenue_monthly` une table. Sur quel critère
   as-tu tranché, et est-ce le bon ?
