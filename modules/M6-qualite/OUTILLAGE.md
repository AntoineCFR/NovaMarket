# 🧰 Outillage — M6

*Cette fiche dit **avec quoi**, pas **comment**.*

---

## Ce que tu vas faire

Jusqu'ici, c'est toi qui sais si le pipeline va bien. Tu vas rendre cette connaissance
**interrogeable** : seize contrôles chiffrés, un bilan des dégâts d'ingestion, et des
métadonnées qui permettent à quelqu'un d'autre de comprendre tes tables sans t'appeler.

Le module le plus léger techniquement, et le plus proche du travail réel.

---

## 1. Un moteur de contrôles

L'essentiel du module tient dans une idée : **ne pas écrire seize requêtes à la main**.
Tu décris les contrôles dans une structure Python, une boucle les exécute.

| Outil | Ce qu'il fait |
|---|---|
| liste de tuples ou de dicts | la description des contrôles |
| `spark.sql(requete)` | exécuter un contrôle |
| `.first()[0]` · `.collect()` | extraire un scalaire du résultat |
| `spark.createDataFrame(lignes, schema)` | fabriquer la table de métriques |
| `.write.mode("overwrite").saveAsTable(...)` | l'écrire |

> C'est **le seul endroit du parcours où une boucle Python est le bon outil** — parce que
> tu boucles sur des *contrôles*, pas sur des lignes de données.

`status` se **déduit** de `metric_value`, `threshold` et `comparison`. Ne l'écris jamais
en dur : un statut saisi à la main est un statut qui ment le jour où le seuil change.

## 2. Mesurer

| Outil | Ce qu'il fait |
|---|---|
| `count(*)` contre `count(colonne)` | lignes contre valeurs non nulles |
| `count(*) - count(DISTINCT cle)` | les doublons |
| `.filter(...).count()` | une population précise |
| `F.col(...)` · `F.lit(...)` | composer les expressions |

> Attention à ce que tu mesures. Sur les commandes, `_rescued_data` est **vide** : un
> contrôle bâti dessus vaudrait 0 et passerait au vert en ne mesurant rien. Les 1 087
> lignes abîmées ne se repèrent qu'à leur adresse tronquée — tu l'as découvert en M1.

## 3. Métadonnées Unity Catalog

| Outil | Ce qu'il fait |
|---|---|
| `COMMENT ON TABLE ... IS '...'` | documenter une table |
| `COMMENT ON COLUMN ... IS '...'` | documenter une colonne |
| `ALTER TABLE ... ALTER COLUMN ... SET TAGS ('pii' = 'true')` | étiqueter |
| `ALTER TABLE ... SET TAGS (...)` | étiqueter la table |

Les étiquettes `pii` posées ici sont le **prérequis de M10** : les politiques ABAC s'y
accrochent. Une étiquette oubliée aujourd'hui est une colonne non masquée dans trois
semaines.

## 4. Interroger le catalogue lui-même

| Vue | Ce qu'elle contient |
|---|---|
| `information_schema.tables` | les tables et leurs commentaires |
| `information_schema.columns` | les colonnes, types, commentaires |
| `information_schema.column_tags` | les étiquettes posées |
| `information_schema.table_tags` | idem au niveau table |

C'est du SQL ordinaire sur des vues système. Sert à produire un rapport de couverture :
« quel pourcentage de mes colonnes est documenté ? ».

## 5. Lineage

Rien à coder. Unity Catalog le capture seul, à l'exécution. Tu vas le **lire** dans
l'interface (onglet *Lineage* d'une table) et constater ce qu'il montre — et ce qu'il ne
montre pas.

---

## Les questions auxquelles l'outillage ne répond pas

- Un contrôle `row_count >= 1` passe au vert quand ta table de référence a perdu 94 % de
  ses lignes. Qu'est-ce qu'un bon seuil ?
- Une métrique de qualité doit-elle compter les défauts **subis** ou les défauts
  **résiduels** ? Les 1 087 lignes tronquées sont réparées depuis M1 — que dit ton bilan ?
- À quoi sert un contrôle dont personne ne regarde le résultat ?

## Le vocabulaire à retenir

**Métrique de qualité** · **seuil et comparaison** · **étiquette de colonne** ·
**`information_schema`** · **lineage** · **commentaire de table**.

Sections 6 et 7 de l'examen.
