# 🧰 Outillage — M7

*Cette fiche dit **avec quoi**, pas **comment**.*

---

## Ce que tu vas faire

Refaire une partie de ce que tu as construit à la main — mais en **déclaratif**. Tu ne
dis plus « lis, transforme, écris, gère le checkpoint » : tu déclares les tables et leurs
dépendances, et le moteur en déduit l'ordre, la parallélisation et la reprise.

Puis tu compares honnêtement : ce que tu gagnes, ce que tu perds.

> ⚠️ **Vocabulaire.** Le produit s'appelle **Lakeflow Declarative Pipelines** (ex-*Delta
> Live Tables*). Le module Python, lui, s'importe toujours `import dlt` — l'ancien nom
> survit dans le code. Toute IA entraînée avant le changement te parlera de « DLT » ;
> l'examen emploie le nouveau nom.

---

## 1. Déclarer des tables

```python
import dlt
```

| Outil | Ce qu'il fait |
|---|---|
| `@dlt.table(name=..., comment=...)` | la fonction décorée **devient** une table |
| `@dlt.table(temporary=True)` | une étape intermédiaire, non publiée |
| `@dlt.view` | une vue du pipeline |
| `dlt.read("nom")` | lire une autre table **du même pipeline** — c'est ce qui crée l'arête du graphe |
| `dlt.read_stream("nom")` | idem, en streaming |
| `spark.readStream.format("cloudFiles")` | l'ingestion, comme en M1 |

La fonction retourne un DataFrame. Son **nom** devient le nom de la table, sauf si tu le
précises. Aucun `.write`, aucun `saveAsTable`, aucun checkpoint à déclarer : c'est le
moteur qui les gère.

Un décorateur, si le mot t'arrête : `@quelque_chose` au-dessus d'une fonction signifie
« passe cette fonction à `quelque_chose` avant de l'enregistrer ». Tu n'as rien à en
comprendre de plus pour ce module — écris-le tel quel.

## 2. Les attentes de qualité

| Outil | Ce qu'il fait |
|---|---|
| `@dlt.expect(nom, condition)` | mesure et **laisse passer** |
| `@dlt.expect_or_drop(nom, condition)` | mesure et **écarte** la ligne |
| `@dlt.expect_or_fail(nom, condition)` | mesure et **arrête** le pipeline |
| `@dlt.expect_all({...})` | plusieurs d'un coup |

La condition est une chaîne SQL, évaluée sur les colonnes **du DataFrame retourné**. Elle
ne peut donc pas référencer une colonne que tu as supprimée par `select` ou `drop` — piège
classique.

## 3. Le journal d'événements

C'est là que se lisent les résultats des attentes.

| Outil | Ce qu'il fait |
|---|---|
| `SELECT * FROM event_log(TABLE(catalog.schema.table))` | le journal du pipeline |
| `F.from_json(colonne, schema)` | ouvrir la colonne `details`, qui est du JSON |
| `F.get_json_object(c, "$.chemin")` | extraire un champ sans déclarer de schéma |
| `F.explode(...)` | une ligne par attente |
| `.groupBy(...).agg(F.sum(...))` | agréger les passages / échecs |

## 4. Piloter le pipeline

Rien à coder : le pipeline se crée dans l'interface, avec un notebook source, un catalog
et un schéma cibles.

En Free Edition, **un seul pipeline actif par type**. Si un pipeline tourne déjà, arrête-le
avant d'en lancer un autre.

---

## Les questions auxquelles l'outillage ne répond pas

Elles sont l'essentiel du module :

- Ce que tu as écrit ici en 40 lignes t'en avait pris 200 en M1 et M3. **Qu'as-tu perdu
  au passage ?**
- Une attente `expect_or_drop` écarte des lignes. Où vont-elles ? Peux-tu les rejouer ?
  Compare avec ta table de quarantaine de M3.
- Le moteur choisit l'ordre d'exécution. Que se passe-t-il quand il se trompe — et
  comment le saurais-tu ?

## Le vocabulaire à retenir

**Lakeflow Declarative Pipelines** (ex-DLT) · **table de streaming** · **vue
matérialisée** · **attente** (*expectation*) · **journal d'événements** · **AUTO CDC**
(ex-`APPLY CHANGES`).

Section 3 de l'examen, et une partie de la 4.
