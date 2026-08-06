# M4 — Historisation : SCD2, MERGE, Change Data Feed

**Durée estimée** : 4 h · **Prérequis** : M2 et M3 validés

> 🧰 **Commence par `OUTILLAGE.md`** — les bibliothèques et fonctions dont tu auras besoin,
> sans les réponses. Et `docs/07-python-pour-le-parcours.md` si c'est ton premier passage.

---

## Objectif

Le journal bronze de M2 contient toutes les versions successives des clients et des
vendeurs. Il ne dit pas *quand* chaque version était en vigueur. C'est ce que tu vas
construire.

L'enjeu est concret. Le taux de commission de NovaMarket dépend du plan du vendeur :

| Plan | Taux |
|---|---|
| `BASIC` | 15,0 % |
| `PLUS` | 11,5 % |
| `PREMIUM` | 8,5 % |

65 vendeurs ont changé de plan. Si tu calcules la commission d'une commande de janvier
avec le plan que le vendeur a aujourd'hui, tu te trompes de **45 765,97 €** sur
l'historique. Ce chiffre n'est pas une hypothèse : le grader de M5 le vérifie.

---

## Ce que tu dois produire

| Table | Contenu |
|---|---|
| `novamarket.silver.customer_scd2` | Historique des versions client |
| `novamarket.silver.seller_scd2` | Historique des versions vendeur |
| `novamarket.ops.scd2_change_log` | Ce que le `MERGE` a réellement fait, capturé par CDF |

### Schéma imposé de `silver.seller_scd2`

| Colonne | Type |
|---|---|
| `seller_id` | `string` |
| `seller_name`, `seller_country`, `seller_city`, `main_top_category`, `plan_code` | `string` |
| `is_active` | `boolean` |
| `onboarded_at` | `date` |
| `valid_from` | `timestamp` |
| `valid_to` | `timestamp` — `null` pour la version courante |
| `is_current` | `boolean` |
| `_scd_hash` | `string` |
| `_processed_at` | `timestamp` |

### Schéma imposé de `silver.customer_scd2`

`customer_id`, `first_name`, `last_name`, `email`, `country`, `city`, `zip_code`,
`segment` (`string`), `is_opt_in`, `is_deleted` (`boolean`), `created_at` (`timestamp`),
puis `valid_from`, `valid_to`, `is_current`, `_scd_hash`, `_processed_at` comme ci-dessus.

---

## Les règles

### Attributs suivis

Seuls ces attributs déclenchent une nouvelle version :

- **Client** : `first_name`, `last_name`, `email`, `country`, `city`, `zip_code`,
  `segment`, `is_opt_in`, `is_deleted`
- **Vendeur** : `seller_name`, `seller_country`, `seller_city`, `main_top_category`,
  `plan_code`, `is_active`

`created_at` et `onboarded_at` sont transportés mais **non suivis** : ils ne changent
jamais, et les inclure dans la détection de changement n'apporterait rien.

`_scd_hash` est l'empreinte des attributs suivis. C'est elle qui décide s'il y a
changement — pas une comparaison colonne à colonne, qui deviendrait illisible à
neuf colonnes et fausse au premier `null`.

### Ordre et départage

Les versions d'une même clé se trient par `updated_at`. **Ce critère ne suffit pas** :
deux versions peuvent porter le même horodatage (c'est même exactement ce que produit
l'extraction en `>=` de M2). Il faut un second critère de tri déterministe.
`_extracted_at` fait l'affaire.

Un tri non déterministe ici, et ton historique change à chaque exécution.

### Fusion des versions identiques

C'est le cœur du module. Le journal contient des lignes ré-extraites **inchangées** :
l'extraction en `>=` reprend systématiquement les lignes situées pile sur le watermark.

Deux versions consécutives dont le `_scd_hash` est identique doivent être **fusionnées
en une seule**, qui conserve le `valid_from` de la première. Sinon ton SCD2 se remplit
de versions fantômes — 401 côté client, 41 côté vendeur — qui ne correspondent à aucun
changement réel.

### Bornes de validité

- `valid_from` = `updated_at` de la version
- `valid_to` = `valid_from` de la version suivante, ou `null` si c'est la courante
- `is_current` = `true` uniquement pour la dernière version

Convention **[valid_from, valid_to)** : borne basse incluse, borne haute exclue. Une
commande passée exactement à `valid_to` appartient à la version suivante. C'est la seule
convention qui évite le double comptage, et il faut la tenir aussi dans les jointures
temporelles de M5.

---

## Déroulé

### Étape 1 — Reconstruction complète

À partir du journal issu des **deux** extractions de M2.

Attendu : `customer_scd2` = **25 390** lignes dont **25 060** courantes ·
`seller_scd2` = **640** lignes dont **600** courantes.

Si tu obtiens 25 411 et 641, tu n'as pas fusionné les versions identiques.

### Étape 2 — Une nouvelle journée d'activité

- **Voie Lakebase** : exécute `data/lakebase/03_changes_D2.sql`.
- **Voie fichier** : téléverse `app_customers_v3.csv` et `app_sellers_v3.csv`, et fais
  pointer la lecture de M2 dessus.

Puis relance le notebook de M2 : troisième extraction.

Attendu dans le journal bronze : **25 971** lignes clients, **706** lignes vendeurs.

> Le grader de M2 ne vaut plus après cette étape — ses bornes décrivent l'état après la
> deuxième extraction. C'est normal.

### Étape 3 — Application du delta par `MERGE`

Sans tout reconstruire. Un `MERGE` SCD2 fait deux choses en une passe :

1. il **ferme** la version courante des clés dont l'empreinte a changé
   (`valid_to` renseigné, `is_current` à faux) ;
2. il **insère** la nouvelle version.

C'est le motif classique en deux temps : un `MERGE` ne peut pas à la fois mettre à jour
une ligne et en insérer une autre pour la même condition de correspondance. Cherche
comment on contourne ça — l'astuce tient en une union et une clé de jointure nulle.

Attendu : `customer_scd2` = **25 570** lignes dont **25 080** courantes ·
`seller_scd2` = **665** lignes dont **600** courantes.

### Étape 4 — Vérification croisée

Reconstruis intégralement le SCD2 depuis le journal complet, dans des tables temporaires,
et compare avec le résultat du `MERGE`. Ils doivent être **identiques ligne à ligne**.

C'est le seul test qui prouve qu'un pipeline incrémental est correct. Écris-le : tu le
réutiliseras toute ta carrière.

### Étape 5 — Change Data Feed

Active le Change Data Feed sur `silver.seller_scd2`, puis utilise `table_changes()` pour
alimenter `ops.scd2_change_log` : quelles lignes le `MERGE` a-t-il fermées, lesquelles
a-t-il insérées, à quelle version de la table.

Schéma libre, mais il doit contenir au minimum `seller_id`, `_change_type`,
`_commit_version`, `_commit_timestamp`.

---

## Critères d'acceptation

| # | Critère | Attendu |
|---|---|---|
| 1 | `silver.seller_scd2` : schéma exact | voir tableau |
| 2 | `seller_scd2` : lignes | **665** |
| 3 | `seller_scd2` : lignes courantes | **600** |
| 4 | Vendeurs en 2 versions | **65** |
| 5 | `silver.customer_scd2` : lignes | **25 570** |
| 6 | `customer_scd2` : lignes courantes | **25 080** |
| 7 | Clients en 1 / 2 / 3 versions | **24 593** / **484** / **3** |
| 8 | Clients courants marqués supprimés | **35** |
| 9 | Exactement une version courante par clé | **0 écart** |
| 10 | Aucune version courante avec `valid_to` renseigné | **0** |
| 11 | Aucune version fermée avec `valid_to <= valid_from` | **0** |
| 12 | Chaînage : `valid_to` d'une version = `valid_from` de la suivante | **0 rupture** |
| 13 | Aucune empreinte `_scd_hash` identique sur deux versions consécutives | **0** |
| 14 | CDF activé sur `silver.seller_scd2` | `true` |
| 15 | `ops.scd2_change_log` alimentée | ≥ 1 ligne |

Les critères 9 à 13 sont des **contrôles d'intégrité temporelle**. Ils ne dépendent
d'aucun chiffre du jeu de données : ils resteront vrais l'an prochain sur d'autres
données. Ce sont eux qu'on met dans un job de production, pas les comptages.

---

## Questions

1. Pourquoi `valid_to` à `null` plutôt qu'à `9999-12-31` ? Quels sont les arguments de
   chaque camp, et qu'est-ce que ça change dans une jointure temporelle ?
2. 401 versions client et 41 versions vendeur ont été fusionnées parce qu'identiques.
   D'où viennent-elles exactement ? Aurait-on pu les éviter en amont, et à quel prix ?
3. Trois clients ont trois versions. Retrouve-les et raconte leur histoire.
4. Ton `MERGE` est-il rejouable ? Que se passe-t-il si tu l'exécutes deux fois de suite
   sur le même delta ? Démontre-le plutôt que de l'affirmer.
5. Un vendeur supprimé de la base source n'apparaîtrait plus dans aucun delta. Que
   devient sa version courante dans ton SCD2, et est-ce le comportement voulu ?
