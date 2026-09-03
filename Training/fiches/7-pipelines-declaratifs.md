# 7 · Pipelines déclaratifs — Lakeflow Spark Declarative Pipelines

**Le renversement** : on ne décrit plus une suite d'opérations, on décrit **les jeux de
données qui doivent exister**. Le moteur en déduit le graphe, l'ordre, l'état et la
reprise.

> **Vocabulaire.** Le produit s'appelle *Lakeflow Declarative Pipelines* — l'ancien
> *Delta Live Tables*. Mais **le module Python est resté `dlt`** : ne « corrige » jamais
> un import pour l'aligner sur le nom commercial.

---

## Le graphe est un effet du code

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(comment="Dépôt fidèle, aucune transformation")
def brut_commandes():
    return (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "csv")
              .option("cloudFiles.schemaLocation", SCHEMA)
              .option("header", "true").option("sep", ";")
              .load(ARRIVEES))

@dlt.table(comment="Commandes typées")
def silver_commandes():
    # CETTE LECTURE EST LA DÉPENDANCE : rien d'autre à déclarer
    return (dlt.read_stream("brut_commandes")
              .withColumn("montant", F.col("montant_txt").try_cast("decimal(12,2)")))
```

**Le piège central** : lire une table du même pipeline par le chemin ordinaire ne crée
**aucune dépendance**.

```python
dlt.read_stream("brut_commandes")     # ✅ crée le lien dans le graphe
dlt.read("brut_commandes")            # ✅ idem, en lecture complète
spark.read.table("brut_commandes")    # ❌ AUCUNE dépendance
```

Avec la troisième forme, les deux tables s'exécutent **en parallèle**, la seconde sur un
état vide ou périmé. Le code s'exécute, ne signale rien, et produit un résultat faux à la
première exécution puis juste aux suivantes — ce qui rend le diagnostic pénible.

En SQL, c'est `STREAM(...)` qui joue ce rôle :

```sql
CREATE OR REFRESH STREAMING TABLE silver_commandes AS
  SELECT * FROM STREAM(LIVE.brut_commandes);
```

---

## Les trois natures d'objet

| Forme | Ce qu'elle fait | Quand la choisir |
|---|---|---|
| **Table de flux** | traite chaque enregistrement **une fois**, l'ajoute | source qui ne fait que grandir |
| **Vue matérialisée** | recalculée, éventuellement de façon incrémentale | tout ce qui peut changer rétroactivement |
| **Vue** | ne stocke rien, étape intermédiaire nommée | découper un calcul long sans matérialiser |

```python
@dlt.table                 # table matérialisée : recalcul complet à chaque exécution
@dlt.view                  # intermédiaire non stocké
```

```sql
CREATE OR REFRESH STREAMING TABLE t AS …    -- alimentée par incréments
CREATE OR REFRESH MATERIALIZED VIEW v AS …  -- recalculée
```

> **Le critère de choix tient en une question : les lignes anciennes peuvent-elles être
> modifiées en source ?** Si oui, une table de flux ne le verra **jamais**. C'est l'erreur
> de conception la plus fréquente, et elle ne se manifeste que des semaines plus tard,
> sous forme de chiffres qui ne se corrigent pas.

Un changement de logique se répercute différemment : une **table matérialisée** refait
tout l'historique, une **table de flux** ne vaut que pour l'avenir.

---

## La qualité attachée à la définition

C'est l'apport principal, et ce n'est pas la brièveté du code : **chaque attente alimente
une mesure, exécution après exécution, sans une ligne de comptage à écrire.**

```python
@dlt.table
@dlt.expect("montant_connu",  "montant IS NOT NULL")        # compte, laisse passer
@dlt.expect_or_drop("qte_positive", "quantite > 0")         # écarte et compte
@dlt.expect_or_fail("cle_presente", "id_cmd IS NOT NULL")   # le LOT entier échoue
def silver_commandes():
    return dlt.read_stream("brut_commandes")
```

La forme groupée, plus lisible dès trois règles :

```python
REGLES = {
    "montant_connu":  "montant IS NOT NULL",
    "qte_positive":   "quantite > 0",
    "date_plausible": "jour >= '2015-01-01'",
}

@dlt.table
@dlt.expect_all(REGLES)                # aussi expect_all_or_drop, expect_all_or_fail
def silver_v2():
    return dlt.read_stream("brut_commandes")
```

```sql
CONSTRAINT observe EXPECT (montant IS NOT NULL)
CONSTRAINT ecarte  EXPECT (qte > 0)      ON VIOLATION DROP ROW
CONSTRAINT arrete  EXPECT (id IS NOT NULL) ON VIOLATION FAIL UPDATE
```

**Trois règles d'usage :**

- **Commence toujours par `expect`.** Poser une règle bloquante dont on n'a pas mesuré le
  taux de violation est la meilleure façon de faire échouer la chaîne la nuit suivante —
  on découvre souvent qu'elle attrape quinze pour cent des lignes.
- **Le nom compte** : c'est lui qu'on lira dans les mesures. `regle_3` n'apprend rien.
- **Une attente ne porte que sur les colonnes du résultat retourné.** Elle ne peut pas
  viser une colonne supprimée en cours de définition — ce qui oblige parfois à réorganiser
  l'ordre des opérations.

### Lire les mesures produites

```sql
SELECT timestamp,
       details:flow_progress.data_quality.expectations
FROM   event_log(TABLE(ventes.ldp.evenements))
WHERE  event_type = 'flow_progress'
ORDER  BY timestamp DESC LIMIT 20;
```

C'est de l'observabilité obtenue sans l'écrire — **à condition d'aller la lire**.

---

## Historiser sans écrire la mécanique

Tout le motif SCD2 — fusion conditionnelle, intervalles de validité, fermeture de la
version précédente — tient ici en dix lignes.

```python
dlt.create_streaming_table("gold.dim_client")     # préalable obligatoire

dlt.apply_changes(
    target      = "gold.dim_client",
    source      = "silver_clients",
    keys        = ["id_cli"],
    sequence_by = F.col("maj_le"),
    apply_as_deletes   = F.expr("_change_type = 'delete'"),
    except_column_list = ["_change_type", "_commit_version"],
    stored_as_scd_type = 2,
)
```

```sql
CREATE FLOW maj_client AS AUTO CDC INTO gold.dim_client
FROM STREAM(LIVE.silver_clients)
KEYS (id_cli) SEQUENCE BY maj_le STORED AS SCD TYPE 2;
```

| Paramètre | Ce qu'il fait |
|---|---|
| `sequence_by` | désigne **lequel de deux changements est le plus récent** — rend l'opération insensible à l'ordre d'arrivée |
| `stored_as_scd_type` | `1` écrase · `2` historise. Les deux s'écrivent pareil : on peut commencer simple et basculer |
| `except_column_list` | **l'oubli le plus fréquent.** Sans lui, chaque changement technique crée une version et la dimension grossit sans raison |

> **Vocabulaire** : `APPLY CHANGES INTO` s'appelle désormais **`AUTO CDC`**.

---

## Les modes d'exécution — choisis sur le pipeline, jamais dans le code

| Mode | Comportement |
|---|---|
| **Déclenché** | traite ce qui attend, puis s'arrête — **le cas courant**, machines libérées |
| **Continu** | ne s'arrête pas — machines allumées en permanence, se chiffre |
| **Développement** | machines réutilisées, pas de reprise — allers-retours rapides |
| **Production** | machines neuves, reprises automatiques |
| **Rafraîchissement complet** | reconstruit **chaque table depuis la source** |

> ⚠️ **Le rafraîchissement complet efface l'historique d'une dimension SCD2.** C'est
> exactement ce qu'on veut après avoir corrigé une logique fautive, et exactement ce qu'on
> ne veut pas sur une table historisée — la source ne contient plus les états passés.
> Une table peut en être **exclue**, et il faut y penser à la déclaration, pas à
> l'incident.

---

## Ce qu'on accepte de perdre

| | Impératif | Déclaratif |
|---|---|---|
| Ce qu'on écrit | une suite d'opérations | des définitions |
| Le graphe | déclaré à côté, à maintenir | **déduit du code** |
| État et reprise | à écrire et surveiller | **pris en charge** |
| Volume de code | élevé | réduit |
| Maîtrise du plan | totale | **faible** |
| Transposabilité | bonne | **faible** |

**Trois coûts rarement présentés :**

- **Le contrôle.** Quand le moteur décide mal, le diagnostic est difficile — le plan n'est
  pas dans le code que tu as écrit, tu ne peux pas relire tes propres décisions puisque tu
  ne les as pas prises.
- **La propriété des objets.** Un pipeline déclaratif **possède ses tables**. Retirer une
  définition peut supprimer la table au déploiement suivant — y compris une table
  intermédiaire qu'une équipe métier avait fini par consulter directement.
- **L'enfermement.** Une chaîne déclarative ne se transpose pas, elle se réécrit.

### Quand choisir l'un ou l'autre

**Déclaratif** pour les chaînes régulières et nombreuses, qui font toutes à peu près la
même chose : ingérer, nettoyer, publier, avec des contrôles. Dix-sept chaînes de six cents
lignes deviennent dix-sept chaînes de quarante.

**Impératif** pour ce qui sort du cadre : appel à un service externe, logique métier
élaborée, ordre imposé pour des raisons que le moteur ne peut pas deviner, optimisation
fine.

**Et une seule discipline dans une organisation mixte : ne pas mélanger les deux à
l'intérieur d'un même flux.** Une chaîne moitié déclarative et moitié impérative cumule
les inconvénients — il faut à la fois maintenir un graphe explicite et subir les décisions
du moteur.
