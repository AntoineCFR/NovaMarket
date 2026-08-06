# Contraintes Databricks Free Edition

Vérifié sur la documentation officielle (juillet 2026). Ces contraintes ont dicté la
conception du parcours : tout ce qui suit est réalisable sans dépenser un centime.

## Ce qui est disponible

| Capacité | Détail |
|---|---|
| Compute | **Serverless uniquement**. Pas de cluster custom, pas de configuration de nœuds |
| `.cache()` / `.persist()` | **Non.** `PERSIST TABLE is not supported on serverless compute` (SQLSTATE 0A000) — voir ci-dessous |
| Mode **ANSI** | **Actif.** Un `cast` qui échoue **lève** au lieu de rendre `NULL` — voir ci-dessous. Conséquence majeure sur toute quarantaine |
| SQL warehouse | 1 seul, taille `2X-Small` |
| Unity Catalog | Oui : catalogs, schemas, tables, **volumes managés**, tags, lineage |
| Lakeflow Jobs | Oui, **max 5 tâches concurrentes** par compte |
| Lakeflow Declarative Pipelines | Oui, **1 pipeline actif par type** |
| Structured Streaming / Auto Loader | **Oui** (voir la nuance ci-dessous) |
| Lakebase (Postgres managé) | Oui, 1 projet, scale-to-zero — c'est notre source OLTP |
| Langages | Python et SQL. **Pas de Scala, pas de R** |
| Dashboards AI/BI, Genie | Oui |

## La mise en cache est indisponible

*Constaté le 4 août 2026, en exécutant M2.*

```
[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute.
SQLSTATE: 0A000
```

`df.cache()` et `df.persist()` lèvent cette erreur. Il n'y a pas de contournement : le
compute serverless ne laisse pas l'utilisateur épingler un DataFrame en mémoire.

**Ce que ça change pour toi** : quand un DataFrame sert plusieurs fois, son plan est
recalculé à chaque action. Sur les volumes de ce parcours — quelques centaines de
milliers de lignes lues depuis des fichiers — l'écart n'est pas mesurable. Si un jour il
le devenait, la parade est d'**écrire une table intermédiaire** plutôt que de mettre en
cache.

**Ce que ça ne change pas pour l'examen** : `cache()` et `persist()` restent au programme
en tant que concepts. Tu dois savoir à quoi ils servent, quand ils sont pertinents, et
pourquoi mettre en cache ce qui ne sert qu'une fois est un gaspillage. Tu ne pourras
simplement pas les pratiquer ici.

---

## Le mode ANSI est actif : un `cast` raté lève au lieu de rendre `NULL`

*Constaté le 5 août 2026, en exécutant M3.*

```
[CANNOT_PARSE_TIMESTAMP] Text '2026-13-45 99:99:99' could not be parsed:
Invalid value for MonthOfYear (valid values 1 - 12): 13.
Use `try_to_timestamp` to tolerate invalid input string and return NULL instead.
SQLSTATE: 22007
```

C'est **le piège le plus coûteux du parcours**, parce qu'il contredit le réflexe qu'on
acquiert en lisant la documentation Spark générique : « un `cast` impossible rend `NULL` ».
Avec `spark.sql.ansi.enabled = true`, il **lève une exception** et arrête le notebook.

| Expression | Sans ANSI | Avec ANSI (ici) |
|---|---|---|
| `F.col("q").cast("int")` sur `"abc"` | `NULL` | **`CAST_INVALID_INPUT`** (22018) |
| `"".cast("decimal(10,2)")` | `NULL` | **lève** |
| `F.to_timestamp(c, fmt)` sur une date invalide | `NULL` | **`CANNOT_PARSE_TIMESTAMP`** (22007) |
| dépassement de capacité (`decimal`, `int`) | `NULL` | **lève** |

**Pourquoi ça casse tout** : toute stratégie de quarantaine repose sur *« ce qui ne se
convertit pas devient `NULL`, et je mets les `NULL` de côté »*. Sous ANSI, le pipeline
s'arrête sur la première valeur sale — exactement celle qu'on voulait attraper.

**La règle, en une phrase : sur de la donnée venant de bronze, jamais `cast`, toujours
`try_cast`.**

| Au lieu de | Écris |
|---|---|
| `c.cast("int")` | `c.try_cast("int")` |
| `c.cast("decimal(10,2)")` | `c.try_cast("decimal(10,2)")` |
| `F.to_timestamp(c, fmt)` | `F.try_to_timestamp(c, F.lit(fmt))` |
| `cast(x AS INT)` *(SQL)* | `try_cast(x AS INT)` |

`.try_cast()` comme méthode de `Column` est récent. S'il manque sur un runtime, la forme
SQL `F.expr("try_cast(x AS INT)")` est disponible partout. Un test d'une ligne tranche :

```python
spark.range(1).select(F.lit("abc").try_cast("int")).show()
```

> **Ne désactive pas ANSI pour t'en sortir.** C'est le défaut de Databricks SQL et des
> runtimes récents, donc le comportement que tu rencontreras en poste — et un `cast`
> silencieux qui fabrique des `NULL` est précisément ce que le mode ANSI existe pour
> empêcher. Écris du code qui déclare ce qu'il accepte.

**Ce que ça change pour l'examen** : le comportement des conversions et l'existence des
variantes `try_*` sont au programme de la section 3. Le vérifier chez toi vaut mieux que
de le supposer :

```python
print(spark.conf.get("spark.sql.ansi.enabled"))
```

---

## La nuance sur le streaming

Le streaming n'est pas interdit — c'est le streaming **continu** qui l'est.

Sur serverless, `Trigger.ProcessingTime(...)` et `Trigger.Continuous(...)` ne sont pas
disponibles dans les notebooks ni dans les jobs. Le mode supporté est :

```python
.trigger(availableNow=True)
```

Chaque exécution traite tout ce qui est arrivé depuis le dernier checkpoint, puis s'arrête.
C'est exactement le modèle d'ingestion incrémentale d'un pipeline batch quotidien — et
c'est ce que fait 90 % de la production réelle. Auto Loader, les checkpoints, la reprise
sur incident, `_rescued_data`, l'évolution de schéma : tout reste au programme.

## Ce qui est indisponible, et comment on contourne

| Limitation | Contournement retenu |
|---|---|
| Pas de bucket S3/ADLS personnel, pas d'`external location` ni de `storage credential` | On utilise un **Volume UC managé** (`/Volumes/...`). C'est du vrai object storage : Auto Loader, `_metadata`, la découverte de fichiers y fonctionnent à l'identique. Aucune différence pédagogique |
| Accès internet sortant restreint à quelques domaines | Aucune ingestion d'API externe dans le tronc commun. Les fichiers sont générés en local et téléversés. ⚠️ La **vérification LinkedIn** de ton compte débloque l'accès internet sortant — si tu la fais, un module bonus « ingestion API REST » devient possible |
| Pas d'API ni de console au niveau compte | Tout se pilote au niveau workspace (UI, CLI avec un PAT, REST API workspace) |
| 5 tâches concurrentes max | Les DAG des jobs sont conçus avec ≤ 5 branches parallèles. Contrainte réaliste, pas un handicap |
| 1 pipeline déclaratif actif | Un seul module (M7) utilise Lakeflow Declarative Pipelines. Le reste est en notebooks + Jobs |

## Le vrai piège : le quota

Free Edition applique une politique d'usage équitable. **Si tu dépasses ton quota
journalier, le compute est coupé jusqu'au lendemain** (tes données et ta configuration
restent intactes).

Conséquences pratiques pour ce parcours :

- Les datasets sont volontairement petits (~52 Mo, ~288 000 lignes de commandes).
- On ne laisse **jamais** tourner un stream en continu.
- On évite les `display()` sur des DataFrames non filtrés et les `.count()` en boucle.
- On planifie les jobs à la demande pendant les modules, pas en cron toutes les heures.

## Sources

- [Databricks Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
- [Streaming on serverless compute](https://docs.databricks.com/aws/en/compute/serverless/streaming)
- [Serverless compute limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations)
