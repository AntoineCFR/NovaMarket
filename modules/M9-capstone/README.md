# M9 — Capstone : incident de production

**Durée estimée** : 4 h · **Prérequis** : M8 validé

> 🧰 **Commence par `OUTILLAGE.md`** — les bibliothèques et fonctions dont tu auras besoin,
> sans les réponses. Et `docs/07-python-pour-le-parcours.md` si c'est ton premier passage.

---

## La situation

Il est 8 h 15. Le job de nuit a tourné. Toutes les tâches sont vertes. La barrière
qualité est passée. Le gold a été publié.

La responsable du contrôle de gestion t'écrit :

> *« Bonjour, le CA de mercredi me paraît bizarre sur le tableau de bord. Tu peux
> vérifier ? Merci. »*

C'est tout ce que tu as.

---

## Mise en place

Téléverse la vague W4, **intégralement** — y compris ce que la source a déposé dans
`ref/`, comme le ferait un job d'ingestion automatique :

```bash
databricks fs cp -r "data/waves/W4/orders" "dbfs:/Volumes/novamarket/landing/files/orders" --overwrite
```

```bash
databricks fs cp -r "data/waves/W4/events" "dbfs:/Volumes/novamarket/landing/files/events" --overwrite
```

```bash
databricks fs cp -r "data/waves/W4/ref" "dbfs:/Volumes/novamarket/landing/files/ref" --overwrite
```

Puis lance ton job de M8. Laisse-le aller au bout.

**N'ouvre pas `data/waves/W4/`.** Tu peux évidemment le faire, personne ne te regarde —
mais tu ne t'entraîneras à rien. En production, personne ne t'ouvrira le fichier source
pour t'expliquer ce qui cloche.

---

## Ton travail

Cinq temps. Traite-les dans l'ordre, en documentant au fur et à mesure : la valeur d'un
post-mortem tient à ce qu'on a noté **pendant**, pas à ce qu'on reconstitue après.

### 1. Détecter

Tes contrôles de M6 sont-ils passés ? Lesquels auraient dû se déclencher et ne l'ont pas
fait ? Un contrôle qui laisse passer un incident est un problème en soi — commence par
identifier lesquels, tu en auras besoin au temps 5.

Compare aussi la journée du 2026-06-04 aux précédentes. Pas le total : la **série**.

### 2. Diagnostiquer

Il y a **plus d'une** anomalie. Elles n'ont ni la même cause, ni la même gravité, ni le
même mode de détection : l'une saute aux yeux dès qu'on regarde le bon indicateur,
l'autre est parfaitement invisible pour tout contrôle technique.

Pour chacune, réponds à : quoi, combien, depuis quand, quelle portée, quel impact chiffré.

Un diagnostic qui s'arrête à « il y a un problème sur les prix » ne permet aucune
décision. « 55 lignes de commande émises par 25 vendeurs le 2026-06-04 portent un prix
unitaire multiplié par 100, ce qui gonfle le CA de 439 592 € » permet d'agir dans la
minute.

### 3. Contenir

Le gold est publié et faux. Qu'est-ce que tu fais **maintenant**, avant même de
comprendre la cause ?

Delta te donne plusieurs options — `RESTORE`, *time travel*, republication. Choisis, et
sache dire pourquoi tu n'as pas choisi les autres.

### 4. Réparer

Le pipeline doit produire un résultat juste, et le refaire tout seul demain.

Deux points de méthode :

- **L'ordre des opérations compte.** Une des deux anomalies rend la seconde plus
  difficile à traiter tant qu'elle n'est pas corrigée. Trouve laquelle.
- Les lignes fautives ne doivent pas être supprimées en silence. Tu as une table de
  quarantaine depuis M3 : elle est faite pour ça. Ajoute le motif
  **`SUSPECTED_UNIT_SCALE`**, défini ainsi :

  > Une ligne est suspecte si son `unit_price` nettoyé dépasse **10 fois** le
  > `list_price` du produit au catalogue. La règle ne s'applique qu'aux produits
  > présents dans le référentiel.

  Ce seuil de 10 n'est pas arbitraire : les prix de vente légitimes oscillent entre 0,80
  et 1,05 fois le prix catalogue. Un facteur 10 ne laisse aucune place au doute, et un
  facteur 100 encore moins.

### 5. Prévenir

Ajoute à `ops.dq_metrics` les contrôles qui auraient détecté chaque anomalie **avant**
la publication du gold. Deux au minimum, nommés exactement :

| `check_name` | Ce qu'il doit attraper |
|---|---|
| `daily_revenue_anomaly` | Une journée dont le CA s'écarte anormalement de la tendance |
| `reference_volume_drop` | Un référentiel qui perd massivement des lignes d'une livraison à l'autre |

Le second est instructif : tu avais déjà un garde-fou sur ce référentiel en M6. Il est
passé au vert. Comprends pourquoi avant d'écrire le nouveau.

---

## Livrables

### `ops.incident_log`

| Colonne | Type |
|---|---|
| `incident_id` | `string` |
| `detected_at` | `timestamp` |
| `detected_by` | `string` — contrôle automatique, alerte métier, hasard… |
| `severity` | `string` — `LOW`, `MEDIUM`, `HIGH`, `CRITICAL` |
| `title` | `string` |
| `symptom` | `string` — ce qu'on a vu |
| `root_cause` | `string` — pourquoi |
| `affected_rows` | `bigint` |
| `impact_amount` | `decimal(18,2)` — impact chiffré en euros, `0.00` si non financier |
| `containment` | `string` — ce qui a été fait tout de suite |
| `remediation` | `string` — la correction durable |
| `prevention` | `string` — le contrôle ajouté |
| `status` | `string` — `OPEN`, `MITIGATED`, `RESOLVED` |

Une ligne par anomalie distincte. Pas une ligne fourre-tout.

### Le reste

- Les nouveaux contrôles dans `ops.dq_metrics`.
- Le motif `SUSPECTED_UNIT_SCALE` opérationnel dans `ops.quarantine_order_line`.
- Un pipeline qui, relancé demain, refait tout ça sans toi.

---

## ⚠️ Critères d'acceptation

**Ne lis pas cette section avant d'avoir terminé les temps 1 et 2.** Les chiffres
attendus répondent à la moitié des questions de l'enquête.

<details>
<summary>Déplier une fois le diagnostic posé</summary>

État attendu **après réparation complète**.

| # | Critère | Attendu |
|---|---|---|
| 1 | `bronze.orders_raw` : lignes | **290 711** |
| 2 | `bronze.orders_raw` : fichiers sources distincts | **10** |
| 3 | `bronze.ref_products_raw` : lignes | **8 000** |
| 4 | `silver.order_line` : lignes | **284 909** |
| 5 | `ops.quarantine_order_line` : lignes | **2 305** |
| 6 | Invariant silver + quarantaine | **287 214** |
| 7 | Motif `SUSPECTED_UNIT_SCALE` | **55** |
| 8 | Motif `INVALID_TIMESTAMP` | **1 435** |
| 9 | Motif `INVALID_QUANTITY` | **821** |
| 10 | Motifs `INVALID_PRICE` et `UNKNOWN_STATUS` | **1** et **1** |
| 11 | `sum(net_amount)` sur les lignes de CA | **24 295 870,03** |
| 12 | Lignes à produit orphelin | **548** |
| 13 | Dernière date de commande | **2026-06-04** |
| 14 | `ops.incident_log` : au moins 2 incidents distincts | — |
| 15 | Un incident chiffré à **439 591,92 €** d'impact | — |
| 16 | Tous les incidents sont en `RESOLVED` ou `MITIGATED` | — |
| 17 | `ops.dq_metrics` contient `daily_revenue_anomaly` | — |
| 18 | `ops.dq_metrics` contient `reference_volume_drop` | — |
| 19 | `gold.fact_order_line` : lignes | **284 909** |
| 20 | Aucune ligne de fait avec `net_amount` > 100 000 € | **0** |

Les critères 8 à 10 méritent un regard : `INVALID_PRICE` et `UNKNOWN_STATUS`, restés à
zéro depuis M3, valent maintenant 1 chacun. **La même ligne** les déclenche tous les
deux, plus deux autres motifs. Elle est le troisième symptôme de la journée — le plus
discret, et le seul que ton pipeline avait déjà attrapé tout seul.

</details>

---

## Questions du post-mortem

Un post-mortem ne cherche pas un coupable, il cherche ce qui a rendu l'erreur possible
et ce qui l'a rendue invisible.

1. Combien de temps entre l'arrivée de la donnée fausse et sa détection ? Qu'est-ce qui
   aurait raccourci ce délai, et à quel coût ?
2. Ton garde-fou sur le référentiel produit, écrit en M6, est passé au vert pendant que
   le référentiel perdait 94 % de ses lignes. Qu'est-ce que ça t'apprend sur la façon
   dont tu as écrit tes seuils ?
3. L'anomalie de prix ne déclenche **aucun** contrôle technique : la donnée est bien
   typée, bien formée, dans les clous. Quelle famille de contrôles manque à ton
   dispositif, et pourquoi est-elle systématiquement la dernière qu'on écrit ?
4. Tu as quarantiné 55 lignes de commandes réelles, passées par de vrais clients. Le CA
   de ces commandes est maintenant absent du gold. Est-ce le bon arbitrage ? Qu'aurais-tu
   fait si la source avait confirmé qu'il fallait diviser par 100 ?
5. Écris la version courte du post-mortem : cinq lignes, destinées à quelqu'un qui n'a
   pas d'expertise data et qui doit décider s'il faut alerter les vendeurs concernés.
