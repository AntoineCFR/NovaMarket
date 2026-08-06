# M7 — Pipeline déclaratif Lakeflow

**Durée estimée** : 3 h · **Prérequis** : M3 validé (M6 recommandé)

> 🧰 **Commence par `OUTILLAGE.md`** — les bibliothèques et fonctions dont tu auras besoin,
> sans les réponses. Et `docs/07-python-pour-le-parcours.md` si c'est ton premier passage.

---

## Objectif

Refaire le chemin `landing → bronze → silver → agrégat` **sans écrire une seule ligne
d'orchestration**, et comparer honnêtement avec ce que tu as construit à la main.

Un pipeline déclaratif ne remplace pas ce que tu sais faire : il déplace le curseur.
Tu décris des jeux de données et leurs contraintes ; le moteur déduit le graphe de
dépendances, gère les checkpoints, ordonne les rafraîchissements et publie des métriques
de qualité que tu n'as pas eu à instrumenter.

En échange, tu perds du contrôle. Le module se termine sur l'endroit exact où ça fait mal.

---

## Contrainte Free Edition

**Un seul pipeline actif par type.** Si tu en as déjà un, supprime-le avant de commencer.
Ce module est le seul du parcours à en utiliser un.

Tout se passe dans un schéma dédié, `novamarket.ldp`, en parallèle de ce que tu as déjà
construit. On ne touche pas à `bronze`, `silver` ni `gold` : le but est de **comparer**,
pas de remplacer.

---

## Ce que tu dois produire

Un fichier source de pipeline (`M7_pipeline.py`), plus un notebook d'exploitation du
journal d'événements.

| Dataset | Type | Lignes attendues |
|---|---|---|
| `ldp.orders_bronze` | table de streaming (Auto Loader) | **287 785** |
| `ldp.order_line_silver` | vue matérialisée, avec attentes | **282 104** |
| `ldp.order_line_quarantine` | vue matérialisée | **2 229** |
| `ldp.revenue_by_month_country` | vue matérialisée | **42** |

Plus `novamarket.ops.ldp_expectations`, alimentée depuis le journal d'événements du
pipeline.

---

## Déroulé

### Étape 1 — La table de streaming

Auto Loader sur `/Volumes/novamarket/landing/files/orders`, mêmes options qu'en M1
(séparateur, encodage, colonnes en `STRING`, colonne de sauvetage).

Différence notable : **tu ne déclares aucun checkpoint**. Le pipeline gère son état.
C'est le premier bénéfice concret, et le premier abandon de contrôle.

### Étape 2 — La vue matérialisée silver, avec attentes

Applique les quatre règles de M3, sous forme d'attentes nommées :

| Nom de l'attente | Condition |
|---|---|
| `valid_timestamp` | `order_ts` parse au format contractuel |
| `valid_quantity` | `quantity` est un entier > 0 |
| `valid_price` | `unit_price` est exploitable après nettoyage |
| `known_status` | `order_status` normalisé est dans la liste contractuelle |

**Attention à l'ordre des opérations.** Les attentes s'appliquent au DataFrame que ta
fonction renvoie. Si tu ne dédupliques pas *avant*, elles s'évaluent sur 287 785 lignes
au lieu de 284 333, et les compteurs ne voudront plus rien dire.

Attendu dans le journal d'événements :

| Attente | Enregistrements en échec |
|---|---|
| `valid_timestamp` | **1 422** |
| `valid_quantity` | **813** |
| `valid_price` | **0** |
| `known_status` | **0** |

1 422 + 813 = 2 235, alors que 2 229 lignes seulement sont écartées : six lignes
échouent à deux attentes. Exactement comme en M3 — c'est la même donnée, la même règle,
et c'est un bon signe que les deux implémentations convergent.

### Étape 3 — Le problème que les attentes ne résolvent pas

`expect_or_drop` écarte les lignes. Il ne les **garde** pas.

Tu as maintenant un compteur qui dit « 2 229 lignes écartées » et aucun moyen de savoir
lesquelles, ni de les rejouer après correction. En M3, ces lignes étaient dans
`ops.quarantine_order_line`, avec leur donnée brute et leurs motifs.

Produis `ldp.order_line_quarantine` : **2 229 lignes**, avec les colonnes source et les
motifs. Réfléchis avant de coder — il y a au moins deux façons de s'y prendre, et l'une
duplique la logique de validation, ce qui garantit qu'elle divergera un jour.

### Étape 4 — L'agrégat

`ldp.revenue_by_month_country` : CA net par mois × pays de livraison, sur les lignes de
chiffre d'affaires uniquement.

Attendu : **42 lignes**, **234 272** lignes de CA agrégées, **24 049 792,86 €** de CA net.

### Étape 5 — Le journal d'événements

Le pipeline publie tout ce qu'il fait : flux démarrés, lignes lues, attentes évaluées,
temps passé. C'est de l'observabilité que tu n'as pas eu à écrire — compare avec le
travail de M6.

```sql
SELECT * FROM event_log(TABLE(novamarket.ldp.order_line_silver))
WHERE event_type = 'flow_progress'
```

> Selon ton workspace, tu devras peut-être publier le journal dans Unity Catalog depuis
> les paramètres du pipeline plutôt que d'utiliser la fonction `event_log()`. Les deux
> chemins mènent aux mêmes données.

Les métriques d'attentes sont dans le champ `details`, en JSON, sous
`flow_progress.data_quality.expectations`. Extrais-les vers
`novamarket.ops.ldp_expectations` :

| Colonne | Type |
|---|---|
| `expectation_name` | `string` |
| `dataset` | `string` |
| `passed_records` | `bigint` |
| `failed_records` | `bigint` |
| `update_id` | `string` |
| `extracted_at` | `timestamp` |

---

## Critères d'acceptation

| # | Critère | Attendu |
|---|---|---|
| 1 | `ldp.orders_bronze` : lignes | **287 785** |
| 2 | `ldp.orders_bronze` : colonnes source en `STRING` + `_rescued_data` | — |
| 3 | `ldp.order_line_silver` : lignes | **282 104** |
| 4 | `ldp.order_line_silver` : `order_line_id` unique | **282 104** |
| 5 | `ldp.order_line_silver` : aucun `order_ts` nul | **0** |
| 6 | `ldp.order_line_quarantine` : lignes | **2 229** |
| 7 | Invariant silver + quarantaine | **284 333** |
| 8 | `ldp.revenue_by_month_country` : lignes | **42** |
| 9 | Somme des lignes agrégées | **234 272** |
| 10 | Somme du CA net | **24 049 792,86** |
| 11 | `ops.ldp_expectations` : les 4 attentes | 4 |
| 12 | `valid_timestamp` en échec | **1 422** |
| 13 | `valid_quantity` en échec | **813** |
| 14 | `valid_price` et `known_status` en échec | **0** et **0** |
| 15 | Les résultats coïncident avec ceux de M3 | `silver.order_line` = `ldp.order_line_silver` |

Le critère 15 est le vrai test du module : deux implémentations indépendantes, la même
donnée, le même résultat. Si elles divergent, l'une des deux a tort et tu as maintenant
les moyens de savoir laquelle.

---

## Questions

Ce module ne se juge pas sur le code, qui est court, mais sur ces réponses.

1. Qu'as-tu **gagné** par rapport aux notebooks de M1 et M3 ? Sois précis : liste ce que
   tu n'as pas eu à écrire.
2. Qu'as-tu **perdu** ? Cite au moins un contrôle de M3 ou M6 que ce pipeline ne permet
   pas d'exprimer aussi bien.
3. `expect_or_drop` écarte, `expect` avertit, `expect_or_fail` arrête tout. Sur les
   quatre règles du module, laquelle mériterait un `expect_or_fail` ? Justifie — et
   attention, la réponse évidente n'est pas la bonne.
4. Le pipeline recalcule intégralement `order_line_silver` à chaque exécution, alors que
   `orders_bronze` est incrémental. Pourquoi ? Que faudrait-il pour rendre le silver
   incrémental, et pourquoi ne l'a-t-on pas fait ici ?
5. Sur ce projet, garderais-tu le pipeline déclaratif ou les notebooks ? Pour quelle
   partie du flux, et pourquoi ? Une réponse « ça dépend » sans critère ne compte pas.
