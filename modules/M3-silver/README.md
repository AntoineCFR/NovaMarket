# M3 — Bronze → Silver : typage, déduplication, quarantaine

**Durée estimée** : 4 h 15 · **Prérequis** : M1 et M2 validés

> 🧰 **Commence par `OUTILLAGE.md`** — les bibliothèques et fonctions dont tu auras besoin,
> sans les réponses. Et `docs/07-python-pour-le-parcours.md` si c'est ton premier passage.

---

## Objectif

Bronze enregistrait sans juger. Silver juge — et doit rendre des comptes sur ses
jugements.

La règle du module :

> **Aucune ligne ne disparaît sans laisser d'adresse.** Toute ligne bronze finit soit
> en silver, soit en quarantaine avec un motif explicite. Un `WHERE` qui écarte
> silencieusement des lignes est un bug, même quand il produit une jolie table.

L'invariant que le grader vérifie :

```
count(silver.order_line) + count(ops.quarantine_order_line) = 284 333
```

284 333, c'est le nombre de `order_line_id` distincts en bronze. Ni 287 785 (le nombre
de lignes brutes), ni le nombre qui t'arrangerait.

---

## Partie 1 — `silver.order_line`

### Ordre des opérations imposé

1. **Déduplication** sur `order_line_id`, sur l'intégralité de bronze.
2. **Nettoyage** des valeurs.
3. **Validation** → quarantaine ou silver.
4. **Enrichissement** (montants calculés, drapeaux).

L'ordre compte : dédupliquer *après* validation gonflerait artificiellement la
quarantaine avec des copies du même problème.

### Règles de nettoyage

| Champ | Règle |
|---|---|
| `order_status` | `trim` puis `upper`. Les variantes de casse ne sont pas des erreurs |
| `unit_price`, `discount_amount` | Retirer tout caractère qui n'est ni un chiffre, ni `,`, ni `.`, ni `-`. Puis, s'il reste une virgule, la traiter comme séparateur décimal. **Attention** : le bruit ne se limite pas à `€` et `EUR` |
| `discount_amount` absent ou vide | vaut `0.00` |
| `order_ts` | Parser avec le format **exact** du contrat : `yyyy-MM-dd HH:mm:ss`. Pas de parsing permissif — on veut savoir ce qui ne respecte pas le contrat |

> Le nettoyage des montants récupère **100 %** des valeurs polluées. Si ta quarantaine
> contient des `INVALID_PRICE`, ta règle est trop timide. Regarde les octets, pas les
> caractères.

### Règles de validation

Une ligne part en quarantaine si elle déclenche au moins un motif. Une même ligne peut
en déclencher plusieurs.

| Motif | Condition |
|---|---|
| `INVALID_TIMESTAMP` | `order_ts` ne parse pas au format contractuel |
| `INVALID_QUANTITY` | `quantity` n'est pas un entier strictement positif |
| `INVALID_PRICE` | `unit_price` reste inexploitable ou ≤ 0 après nettoyage |
| `UNKNOWN_STATUS` | `order_status` normalisé hors de la liste contractuelle |

### Ce qui n'est *pas* un motif de quarantaine

Les clés étrangères orphelines. Une commande passée par un client absent du référentiel
reste une commande : le chiffre d'affaires est bien réel, l'argent a bien été encaissé.
La jeter fausserait le CA ; l'accepter en silence masquerait un problème d'intégrité.

On la garde donc, **signalée** :

- `is_orphan_customer` : `customer_id` absent de `bronze.app_customers_raw`
- `is_orphan_product` : `product_id` absent de `bronze.ref_products_raw`

C'est un arbitrage métier, pas une évidence technique. Assume-le et documente-le.

### Schéma imposé de `silver.order_line`

| Colonne | Type | Note |
|---|---|---|
| `order_line_id` | `string` | clé unique |
| `order_id` | `string` | |
| `order_ts` | `timestamp` | |
| `order_date` | `date` | dérivée |
| `customer_id`, `seller_id`, `product_id` | `string` | |
| `quantity` | `int` | |
| `unit_price`, `discount_amount` | `decimal(10,2)` | |
| `gross_amount` | `decimal(12,2)` | `quantity * unit_price` |
| `net_amount` | `decimal(12,2)` | `gross_amount - discount_amount` |
| `currency`, `shipping_country`, `payment_method`, `order_status` | `string` | |
| `is_revenue` | `boolean` | faux si statut `CANCELLED` ou `RETURNED` |
| `is_orphan_customer`, `is_orphan_product` | `boolean` | |
| `shipping_address` | `string` | **réparée** — voir ci-dessous |
| `_source_file` | `string` | |
| `_silver_processed_at` | `timestamp` | |

### `shipping_address` : recoller la réparation de M1

Environ 1 087 adresses ont été tronquées par le lecteur CSV et récupérées en M1 dans
`bronze.orders_address_repair`. Silver est la couche où on les remet en place :

```
coalesce(repair.shipping_address_full, bronze.shipping_address)
```

> ⚠️ **Le piège est dans la jointure, pas dans le `coalesce`.** La table de réparation
> compte 1 087 lignes pour seulement **1 073 clés distinctes** : quatorze lignes
> défectueuses étaient elles-mêmes dupliquées dans les fichiers, et bronze ne déduplique
> pas.
>
> Une jointure gauche naïve sur `order_line_id` fera donc **grossir ta table de faits**,
> silencieusement, de quelques lignes. Ton `silver.order_line` ne tombera plus sur le
> compte attendu et tu chercheras longtemps.
>
> Déduplique la table de réparation **avant** de joindre. C'est la même mécanique que
> celle du QCM de la section 3 : une jointure gauche garantit *au moins* une ligne par
> ligne de gauche, jamais exactement une.

### Schéma imposé de `ops.quarantine_order_line`

Les 14 colonnes source **telles quelles** (en `string`), plus `_rescued_data`,
`_source_file`, plus :

| Colonne | Type |
|---|---|
| `quarantine_reasons` | `array<string>` |
| `quarantined_at` | `timestamp` |

Une table de quarantaine qui ne conserve pas la donnée brute ne sert à rien : son
utilité, c'est de permettre le rejeu après correction.

---

## Partie 2 — `silver.event` et `silver.event_item`

### Règles

1. Les lignes JSON illisibles partent en `ops.quarantine_event` avec le motif
   `MALFORMED_JSON` et **leur texte d'origine intact**. Elles n'ont pas d'`event_id`, et
   leur contenu brut est dans `_corrupt_record` — pas dans `_rescued_data`, qui est vide
   ici (M1, étape 2). Repère-les par `event_id IS NULL`.
2. Déduplication sur `event_id`.
3. Aplatissement des structures imbriquées (voir schéma ci-dessous).
4. `items` est explosé dans une table fille `silver.event_item`, avec l'indice de
   position conservé.
5. `event_ts` doit finir en `timestamp` **pour la totalité des événements**.

> Point 5, attention : `event_ts` arrive sous deux formes — chaîne ISO `yyyy-MM-dd'T'HH:mm:ss'Z'`
> et entier epoch en millisecondes (1 269 occurrences). L'inférence d'Auto Loader retient
> `string` : les entiers sont donc **dans la colonne**, convertis en chaînes de chiffres.
> Tu dois traiter les deux formes dans la même expression, et distinguer l'une de l'autre
> sans te tromper — une chaîne de treize chiffres n'est pas une date ISO.
>
> Vérifie le type obtenu avant de commencer :
> `dict(spark.table(f"{CATALOG}.bronze.events_raw").dtypes)["event_ts"]`

### Schéma imposé de `silver.event`

`event_id` (string), `event_ts` (timestamp), `event_date` (date), `event_type` (string),
`customer_id` (string), `session_id` (string), `segment` (string), `os` (string),
`app_version` (string), `is_mobile` (boolean), `page` (string), `referrer` (string),
`utm_source`, `utm_medium`, `utm_campaign` (string), `search_term` (string),
`order_id` (string), `n_items` (int), `_source_file` (string), `_silver_processed_at` (timestamp)

### Schéma imposé de `silver.event_item`

`event_id` (string), `item_index` (int), `product_id` (string), `qty` (int),
`price` (decimal(10,2)), `_silver_processed_at` (timestamp)

> `qty` et `price` sont eux aussi parfois sérialisés en chaîne dans le JSON source.
> Applique la même rigueur qu'aux montants des commandes.

---

## Critères d'acceptation

### Commandes

| # | Critère | Attendu |
|---|---|---|
| 1 | `silver.order_line` : schéma exact | voir tableau |
| 2 | Lignes | **282 104** |
| 3 | `order_line_id` unique | **282 104** |
| 4 | `ops.quarantine_order_line` : lignes | **2 229** |
| 5 | Invariant silver + quarantaine | **284 333** |
| 6 | Motif `INVALID_TIMESTAMP` | **1 422** |
| 7 | Motif `INVALID_QUANTITY` | **813** |
| 8 | Motif `INVALID_PRICE` | **0** |
| 9 | Motif `UNKNOWN_STATUS` | **0** |
| 10 | Commandes distinctes | **136 824** |
| 11 | `sum(net_amount)` | **28 983 772,55** |
| 12 | `sum(net_amount)` sur les lignes de CA | **24 049 792,86** |
| 13 | Lignes de CA (`is_revenue`) | **234 272** |
| 14 | `is_orphan_customer` | **1 721** |
| 15 | `is_orphan_product` | **545** |
| 16 | Aucun `unit_price` ni `order_ts` nul | **0** |
| 17 | Plage de dates | 2025-12-01 → 2026-06-02 |

Les critères 6 et 7 totalisent 2 235 alors que la quarantaine en compte 2 229 : six
lignes cumulent les deux motifs. Si tu trouves 2 235, tu comptes des lignes deux fois.

### Événements

| # | Critère | Attendu |
|---|---|---|
| 18 | `silver.event` : lignes | **130 025** |
| 19 | `event_id` unique | **130 025** |
| 20 | Aucun `event_ts` nul | **0** |
| 21 | `ops.quarantine_event` : lignes | **389** |
| 22 | `silver.event_item` : lignes | **26 183** |
| 23 | Événements portant au moins un item | **15 755** |
| 24 | Événements avec `customer_id` renseigné | **114 396** |
| 25 | Répartition par `event_type` | voir `graders/expected/M3.json` |
| 26 | Aucun `qty` ni `price` nul dans `event_item` | **0** |

---

## Vérification croisée

`generator/reference_stats.py` est une seconde implémentation de ces règles, en Python
pur, sans Spark. Elle produit `graders/expected/M3.json`.

Si ton résultat diverge, l'un des deux se trompe — et la comparaison des deux te dira
lequel bien plus vite que la lecture de ton propre code. C'est aussi une bonne habitude
en soi : une règle métier qui ne peut être vérifiée que par le code qui l'implémente
n'est pas une règle, c'est une opinion.

---

## Questions

À traiter dans le notebook :

1. Six lignes cumulent deux motifs. Retrouve-les et explique ce qui a pu produire ça.
2. `INVALID_PRICE` et `UNKNOWN_STATUS` ne se déclenchent jamais. Fallait-il quand même
   les implémenter ? Argumente.
3. Ta quarantaine contient 2 229 lignes. Quel processus de reprise proposes-tu ? Qui
   décide, à quelle fréquence, et comment les lignes corrigées rentrent-elles en silver
   sans casser l'unicité de `order_line_id` ?
4. `is_orphan_customer` concerne 1 721 lignes, soit 0,6 % du CA. À partir de quel seuil
   ce chiffre devrait-il faire échouer le pipeline plutôt que produire un simple drapeau ?
