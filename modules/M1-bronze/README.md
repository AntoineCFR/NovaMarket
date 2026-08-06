# M1 — Landing → Bronze avec Auto Loader

**Durée estimée** : 3 h 15 · **Prérequis** : M0 validé

> 🧰 **Commence par `OUTILLAGE.md`** — les bibliothèques et fonctions dont tu auras besoin,
> sans les réponses. Et `docs/07-python-pour-le-parcours.md` si c'est ton premier passage.

---

## Objectif

Construire la couche bronze des trois sources fichiers. Une couche bronze réussie
respecte une règle unique et non négociable :

> **Rien ne se perd.** Toute ligne présente dans un fichier source se retrouve dans la
> table bronze, y compris celles qui sont malformées, dupliquées ou incohérentes.
> Une table bronze ne juge pas, elle enregistre — et elle sait dire d'où vient chaque ligne.

Ce module te fait manipuler les trois mécanismes qui rendent ça possible : la colonne
de sauvetage, les colonnes de métadonnées de fichier, et le checkpoint.

Il te fait aussi découvrir **où cette règle se brise malgré toi**, et ce qu'il faut faire
ensuite. La promesse tient au niveau de la ligne — aucune n'est perdue. Elle ne tient pas
au niveau du champ : le lecteur CSV tronque sans le dire, et la colonne de sauvetage ne
rattrape pas tout. Il te reviendra de le détecter (étape 5), puis de le **réparer**
(étape 6). C'est le vrai contenu du module.

---

## Ce que tu dois produire

| Table | Source | Mode |
|---|---|---|
| `novamarket.bronze.orders_raw` | `landing/files/orders/*.csv` | Auto Loader, append incrémental |
| `novamarket.bronze.events_raw` | `landing/files/events/*.jsonl.gz` | Auto Loader, append incrémental |
| `novamarket.bronze.ref_products_raw` | `landing/files/ref/products.csv` | snapshot, overwrite |
| `novamarket.bronze.ref_sellers_raw` | `landing/files/ref/sellers.csv` | snapshot, overwrite |
| `novamarket.bronze.ref_categories_raw` | `landing/files/ref/categories.csv` | snapshot, overwrite |
| `novamarket.bronze.orders_address_repair` | `landing/files/orders/*.csv` | Auto Loader, append — **passe de réparation** |

Plus une ligne dans `ops.pipeline_runs` par exécution de chaque notebook.

---

## Déroulé

### Étape 1 — `bronze.orders_raw`

Notebook : `M1_bronze_orders.py`.

Le CSV des commandes n'est pas un CSV « propre ». Avant d'écrire la moindre ligne de
code, **va regarder le fichier**. Une cellule d'exploration est prévue pour ça au début
du notebook. Tu dois identifier, au minimum : le séparateur, l'encodage, le format des
nombres, et ce qui se passe sur les lignes où l'adresse de livraison contient un point-virgule.

Contraintes :

- **Toutes les colonnes source en `STRING`.** Aucun cast en bronze. Si tu castes ici,
  tu perds silencieusement les lignes malformées et tu échoues à la règle du module.
- Colonne de sauvetage nommée `_rescued_data`.
- Les 4 colonnes de métadonnées de `docs/03-conventions.md`, alimentées depuis
  `_metadata` pour les deux premières.
- Écriture en `append`, déclenchement `availableNow`.

### Étape 2 — `bronze.events_raw`

Notebook : `M1_bronze_events.py`.

Ici l'enjeu est différent : le JSON est imbriqué et compressé.

- Ne décompresse rien à la main.
- **La structure imbriquée doit être préservée** : `user`, `device`, `context`, `items`
  restent des `STRUCT` / `ARRAY` dans la table bronze. Aplatir est une transformation
  métier — donc silver, donc M3.
- Les lignes JSON illisibles ne doivent **pas disparaître**. Elles produisent une ligne à
  champs nuls, et leur texte brut est conservé — mais **pas** dans la colonne que tu crois.
  Va vérifier laquelle, c'est une distinction d'examen :

  | Colonne | Répond à |
  |---|---|
  | `_rescued_data` | la ligne est lisible, mais **s'écarte du schéma** — type inattendu, colonne en trop |
  | `_corrupt_record` | la ligne **n'a pas pu être analysée du tout** |

### Étape 3 — `bronze.ref_*`

Notebook : `M1_bronze_ref.py`.

Les référentiels sont livrés en **snapshot complet**. Auto Loader, qui est un mécanisme
d'ajout incrémental, n'est pas l'outil adapté. Le notebook te demande d'expliquer
pourquoi et d'implémenter le bon motif.

### Étape 4 — Prouver l'incrémentalité

1. Relance les notebooks 1 et 2 **sans rien téléverser**. Le nombre de lignes ne doit pas bouger.
2. Téléverse la vague W2 :

```bash
databricks fs cp -r "data/waves/W2/orders" "dbfs:/Volumes/novamarket/landing/files/orders" --overwrite
```

```bash
databricks fs cp -r "data/waves/W2/events" "dbfs:/Volumes/novamarket/landing/files/events" --overwrite
```

3. Relance. Seules les lignes du nouveau fichier doivent avoir été ajoutées.

### Étape 5 — Analyse des lignes sauvées

Réponds dans la dernière section du notebook `M1_bronze_orders.py` :

1. Combien de lignes ont un `_rescued_data` non nul ? Le résultat va te surprendre.
   Explique-le : qu'est-ce que la colonne de sauvetage peut détecter, sachant que **toutes
   tes colonnes sont en `STRING`** ?
2. Environ 1 087 lignes du fichier contiennent un `;` non échappé dans `shipping_address`
   et comptent donc 16 champs au lieu de 14. Retrouve-les dans ta table bronze. Où sont
   passés les deux champs en trop ? Le nombre de lignes a-t-il bougé ?
3. Un `_rescued_data` nul garantit-il que la ligne est saine ? Donne un contre-exemple
   tiré du fichier.
4. Le fichier de la vague W2 rejoue une partie des lignes de la veille. Ta table bronze
   contient-elle des doublons ? Est-ce un problème **à ce stade** ? Justifie.

### Étape 6 — Réparer

Constater la perte ne suffit pas. Bronze promet que rien ne se perd, et la promesse doit
tenir **au niveau du champ**. Tu vas donc récupérer les fragments d'adresse dans les
fichiers, puisque le lecteur CSV les a abandonnés.

**Contrainte de conception, à comprendre avant de coder** : on ne touche pas au flux
principal. Lui déclarer un schéma explicite avec des colonnes de réserve désactiverait
l'inférence — et la vague W3 fera justement apparaître deux vraies colonnes dans
l'en-tête, qui entreraient en collision avec elles. La réparation se fait donc dans un
**second flux, avec son propre checkpoint**.

C'est aussi le motif réaliste : quand un loader natif abîme une source qu'on ne contrôle
pas, on ne bricole pas le loader, on réconcilie à côté.

Produis `bronze.orders_address_repair` :

| Colonne | Type | Contenu |
|---|---|---|
| `order_line_id` | string | 2ᵉ champ de la ligne — intact, seul le dernier est amputé |
| `shipping_address_full` | string | l'adresse complète, reconstituée |
| `_source_file` | string | nom du fichier |
| `_repaired_at` | timestamp | horodatage de la passe |

**1 087 lignes** après W1 + W2 — pour **1 073 clés distinctes**. L'écart n'est pas une
erreur : quatorze lignes défectueuses sont elles-mêmes dupliquées dans les fichiers, et
bronze ne déduplique pas. Retiens ce chiffre, il te tendra un piège en M3.

Puis réponds aux deux dernières questions du notebook :

5. La table de réparation compte-t-elle exactement autant de lignes que la table bronze
   en compte de tronquées ? Si tu relances la passe sans nouveau fichier, que se
   passe-t-il — et pourquoi ?
6. On aurait pu, à la place, ne garder que la ligne brute en bronze et tout parser en
   silver. Qu'est-ce que cette solution aurait coûté, au regard de ce que W3 fera au schéma ?
7. **Deux tables bronze pour une source : est-ce ainsi qu'une équipe traiterait le
   problème en production ?** Cite au moins trois autres réponses possibles, dis dans quel
   contexte chacune devient le bon choix et ce qu'elle coûte, puis tranche.

Ces réponses sont corrigées dans `solutions/M1/` — à ouvrir seulement après.

> La question 7 n'a pas de bonne réponse unique, et c'est la plus utile du module. Elle
> est traitée à part dans **`FICHE-source-malformee.md`**, qui pose les quatre options et
> assume le choix retenu ici — un choix **pédagogique**, qui n'est pas celui qu'une équipe
> ferait en premier. Ne l'ouvre qu'après avoir écrit ta propre réponse.

---

## Critères d'acceptation

Le grader `graders/M1_grader.py` vérifie :

| # | Critère | Valeur attendue |
|---|---|---|
| 1 | `bronze.orders_raw` existe | — |
| 2 | Ses 14 colonnes source sont présentes et de type `STRING` | — |
| 3 | Elle porte `_rescued_data`, `_source_file`, `_source_file_modification_time`, `_ingested_at`, `_ingest_batch_id` | — |
| 4 | Nombre de lignes après W1 + W2 | **287 785** |
| 5 | Nombre de fichiers sources distincts | **8** |
| 6 | Lignes avec `_rescued_data` non nul | **0** — voir l'étape 5 |
| 6 bis | Lignes dont `shipping_address` a été tronquée par le lecteur | ~**1 087** (tolérance ±15 %) |
| 6 ter | `bronze.orders_address_repair` : lignes, clés distinctes, adresses complètes, clés rattachables à bronze | **1 087** / **1 073** / toutes / toutes |
| 7 | Aucune valeur de `_source_file` nulle ou vide | — |
| 8 | `bronze.events_raw` existe, lignes après W1 + W2 | **131 068** |
| 9 | `user`, `device`, `context` sont des `STRUCT`, `items` un `ARRAY` | — |
| 10 | Enregistrements d'événements illisibles conservés (lignes sans `event_id`) | ~**389** · leur texte brut doit survivre dans `_corrupt_record` |
| 11 | `ref_products_raw` / `ref_sellers_raw` / `ref_categories_raw` | **8 000** / **600** / **39** lignes, et leurs colonnes correctement séparées |
| 12 | Les tables de référence sont idempotentes en snapshot (pas d'accumulation) | — |
| 13 | `ops.pipeline_runs` contient au moins une ligne par source ingérée | — |

---

## Points d'attention

- **Le checkpoint est ton état.** Si tu te trompes de schéma au premier essai, supprimer
  la table ne suffit pas : le checkpoint garde la mémoire des fichiers déjà lus. Une
  cellule de réinitialisation est fournie en fin de notebook. Utilise-la sciemment.
- **Un flux, un checkpoint.** La passe de réparation a le sien, distinct de celui du flux
  principal. Les deux se réinitialisent séparément — et toujours **par paire** avec leur
  table : un flux est idempotent par fichier, pas par ligne. Effacer le checkpoint sans
  vider la table double les données.
- `cloudFiles.schemaLocation` et `checkpointLocation` sont deux choses différentes.
  Comprends laquelle sert à quoi avant de les faire pointer au même endroit.
- Sur serverless, `.trigger(availableNow=True)` est le seul déclencheur disponible.
  `.trigger(processingTime=...)` lèvera une erreur.
- `awaitTermination()` est indispensable si tu veux que la cellule suivante voie les
  données écrites.
- Attention à la différence entre `cloudFiles.inferColumnTypes` et
  `cloudFiles.schemaEvolutionMode`. L'un décide du typage, l'autre décide de ce qui se
  passe quand une colonne inconnue apparaît. La vague W3 te fera revenir sur ce choix.
