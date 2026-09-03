# 3 · Ingestion — PySpark et SQL

Le raisonnement et les arbitrages sont dans `exam/fiche-ingestion.md`. **Ici, c'est le
code.**

---

## Choisir en trois questions

| La source | La méthode |
|---|---|
| Des fichiers, peu nombreux, rythme prévisible, équipe SQL | **`COPY INTO`** |
| Des fichiers, nombreux, arrivées irrégulières, schéma mouvant | **Auto Loader** |
| Un bus de messages (Kafka…) | **Structured Streaming** `format("kafka")` |
| Une base relationnelle | **JDBC** + curseur |
| Un SaaS d'entreprise (Salesforce, Workday…) | **Lakeflow Connect** |
| Un référentiel, petit, rechargé en entier | **batch** `spark.read` + `overwrite` |

**Auto Loader *est* du Structured Streaming** — c'est son nom quand la source est
`cloudFiles`. `COPY INTO` est le seul intrus : commande SQL, sans checkpoint.

---

## 1. Batch complet

Pour un référentiel : on jette et on recharge. Simple, et suffisant tant que le volume
est petit.

```python
ref = (spark.read
         .option("header", "true").option("sep", ";")
         .schema(schema_ref)                    # déclaré, jamais inféré en production
         .csv("/Volumes/ventes/bronze/landing/ref/"))

(ref.withColumn("_ingere_le", F.current_timestamp())
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("ventes.bronze.ref_produit"))
```

```sql
CREATE OR REPLACE TABLE ventes.bronze.ref_produit AS
SELECT * FROM read_files('/Volumes/ventes/bronze/landing/ref/',
                         format => 'csv', header => true, sep => ';');
```

---

## 2. `COPY INTO`

**L'état vit dans les métadonnées de la table cible.** Idempotent : relancer la même
commande ne recharge rien.

```sql
CREATE TABLE IF NOT EXISTS ventes.bronze.commande (
  order_id STRING, order_ts STRING, montant STRING, statut STRING
);

COPY INTO ventes.bronze.commande
FROM '/Volumes/ventes/bronze/landing/commandes'
FILEFORMAT   = CSV
FORMAT_OPTIONS ('header' = 'true', 'sep' = ';', 'encoding' = 'UTF-8')
COPY_OPTIONS   ('mergeSchema' = 'true');
```

Avec sélection et transformation à la volée :

```sql
COPY INTO ventes.bronze.commande
FROM (
  SELECT order_id, order_ts, montant, statut,
         current_timestamp() AS _ingere_le,
         _metadata.file_path AS _fichier
  FROM '/Volumes/ventes/bronze/landing/commandes'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'sep' = ';');
```

**Les deux pièges :**

```sql
-- 1. TRUNCATE ne remet PAS l'historique des fichiers chargés
TRUNCATE TABLE ventes.bronze.commande;
COPY INTO ventes.bronze.commande FROM '…' FILEFORMAT = CSV;   -- 0 ligne, sans erreur

-- 2. la parade
COPY INTO ventes.bronze.commande
FROM '…' FILEFORMAT = CSV
COPY_OPTIONS ('force' = 'true');                               -- recharge tout
```

---

## 3. Auto Loader

**L'état vit dans un checkpoint séparé.** Le supprimer recharge tout.

```python
SOURCE = "/Volumes/ventes/bronze/landing/commandes"
SCHEMA = "/Volumes/ventes/bronze/landing/_schemas/commandes"
CHECK  = "/Volumes/ventes/bronze/landing/_checkpoints/commandes"

flux = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.schemaLocation", SCHEMA)
          .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
          .option("rescuedDataColumn", "_rescued_data")
          .option("header", "true").option("sep", ";")
          .load(SOURCE)
          .withColumn("_ingere_le", F.current_timestamp())
          .withColumn("_fichier", F.col("_metadata.file_path")))

(flux.writeStream
     .option("checkpointLocation", CHECK)
     .option("mergeSchema", "true")
     .trigger(availableNow=True)              # traite le retard, puis s'arrête
     .toTable("ventes.bronze.commande"))
```

En SQL, dans un pipeline déclaratif :

```sql
CREATE OR REFRESH STREAMING TABLE ventes.bronze.commande AS
SELECT *, current_timestamp() AS _ingere_le, _metadata.file_path AS _fichier
FROM STREAM read_files(
  '/Volumes/ventes/bronze/landing/commandes',
  format => 'csv', header => true, sep => ';',
  schemaEvolutionMode => 'addNewColumns');
```

### Les options qui comptent

| Option | Effet |
|---|---|
| `cloudFiles.format` | le format réel des fichiers |
| `cloudFiles.schemaLocation` | où le schéma inféré est mémorisé |
| `cloudFiles.schemaHints` | forcer le type de certaines colonnes |
| `cloudFiles.schemaEvolutionMode` | `addNewColumns` *(défaut sans schéma)* · `rescue` · `failOnNewColumns` · `none` *(défaut avec schéma)* |
| `rescuedDataColumn` | où atterrissent les écarts au schéma |
| `cloudFiles.useNotifications` | passer du listage à la notification |
| `cloudFiles.maxFilesPerTrigger` | plafonner la taille d'un lot |

> Sous `addNewColumns`, une colonne nouvelle **fait échouer le flux**, qui repart avec le
> schéma étendu au redémarrage. L'échec est volontaire ; des reprises configurées sur la
> tâche le rendent invisible.

### Les modes de déclenchement

```python
.trigger(availableNow=True)              # tout ce qui attend, puis stop  ← le cas courant
.trigger(processingTime="5 minutes")     # micro-lots, machines allumées
# sans trigger : micro-lots au plus vite, en continu
```

---

## 4. Streaming depuis un bus de messages

Même moteur, autre source. Le contenu arrive **en binaire**.

```python
from pyspark.sql.types import StructType, StringType, DoubleType

schema = StructType().add("id", StringType()).add("montant", DoubleType())

evts = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "broker:9092")
          .option("subscribe", "commandes")
          .option("startingOffsets", "latest")     # ou "earliest"
          .load()
          .select(F.from_json(F.col("value").cast("string"), schema).alias("d"),
                  F.col("timestamp").alias("_recu_le"))
          .select("d.*", "_recu_le"))

(evts.writeStream
     .option("checkpointLocation", CHECK)
     .trigger(availableNow=True)
     .toTable("ventes.bronze.evenement"))
```

---

## 5. Base relationnelle — JDBC

### Lecture complète

```python
opts = {
  "url": "jdbc:postgresql://hote:5432/base",
  "driver": "org.postgresql.Driver",
  "user": dbutils.secrets.get("coffre", "pg_user"),
  "password": dbutils.secrets.get("coffre", "pg_password"),
}

df = spark.read.format("jdbc").options(**opts, dbtable="public.commande").load()
```

### Lecture parallélisée

Sans ces quatre options, **une seule connexion** lit toute la table.

```python
df = (spark.read.format("jdbc").options(**opts)
        .option("dbtable", "public.commande")
        .option("partitionColumn", "id")      # numérique ou date, indexée
        .option("lowerBound", 1)
        .option("upperBound", 10_000_000)
        .option("numPartitions", 8)           # 8 requêtes en parallèle
        .option("fetchsize", 10_000)
        .load())
```

### Ingestion incrémentale par curseur

Le motif complet : lire le watermark, extraire le delta, écrire, **recalculer le
watermark sur ce qui a été écrit**.

```python
BATCH = str(uuid.uuid4())

# 1. le curseur précédent
wm = (spark.table("ops.watermark")
        .filter("source = 'pg.commande'").select("valeur").first())
wm = wm[0] if wm else "1970-01-01 00:00:00"

# 2. le delta — filtré CÔTÉ SOURCE, dans la requête poussée
requete = f"""(SELECT * FROM public.commande
               WHERE updated_at > TIMESTAMP '{wm}') AS d"""
delta = (spark.read.format("jdbc").options(**opts)
           .option("dbtable", requete).load()
           .withColumn("_lot", F.lit(BATCH))
           .withColumn("_ingere_le", F.current_timestamp()))

# 3. UNE seule action sur la source : l'écriture
delta.write.mode("append").saveAsTable("ventes.bronze.commande")

# 4. le nouveau curseur se lit EN RETOUR sur la cible, jamais sur le plan
ecrit = (spark.table("ventes.bronze.commande")
           .filter(F.col("_lot") == BATCH)
           .agg(F.count("*").alias("n"), F.max("updated_at").alias("wm")).first())

if ecrit["n"]:
    set_watermark("pg.commande", ecrit["wm"])
```

**Trois choses à ne pas rater :**

- Un DataFrame est un **plan**. `count()`, `write` et `agg()` relisent chacun la source.
  Sur une base vivante, le watermark calculé sur le plan peut dépasser ce qui a été écrit.
- Le `>` strict perd la ligne de bordure. Le `>=` la récupère, au prix de doublons qu'une
  cible idempotente (`MERGE`) absorbe.
- Un curseur est **aveugle aux suppressions**. On le double d'une réconciliation par
  comptage complet, moins fréquente.

---

## 6. CDC et SCD2

### En impératif — le `MERGE` en deux temps

Un seul `MERGE` ne peut pas à la fois **fermer** une version et en **insérer** une autre
pour la même clé. On duplique la source, une occurrence portant une clé de correspondance
nulle pour forcer la branche `INSERT`.

```sql
MERGE INTO gold.dim_client AS cible
USING (
  -- ligne 1 : ferme la version courante (clé renseignée)
  SELECT maj.id_cli AS cle_fusion, maj.* FROM maj
  UNION ALL
  -- ligne 2 : force l'insertion de la nouvelle version (clé nulle)
  SELECT NULL AS cle_fusion, maj.* FROM maj
  JOIN gold.dim_client d ON d.id_cli = maj.id_cli AND d.est_courant
  WHERE d.nom <> maj.nom OR d.segment <> maj.segment       -- vrai changement
) AS src
ON cible.id_cli = src.cle_fusion AND cible.est_courant

WHEN MATCHED THEN UPDATE SET
  est_courant = false,
  valide_jusqu_a = src.maj_le

WHEN NOT MATCHED THEN INSERT
  (id_cli, nom, segment, valide_depuis, valide_jusqu_a, est_courant)
  VALUES (src.id_cli, src.nom, src.segment, src.maj_le, NULL, true);
```

Un `MERGE` simple, pour une SCD1 ou un rattrapage idempotent :

```sql
MERGE INTO silver.commande AS c
USING delta AS d ON c.order_id = d.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

### En déclaratif — le mécanisme automatisé

Tout le motif ci-dessus tient en dix lignes.

```python
dlt.create_streaming_table("gold.dim_client")

dlt.apply_changes(
    target      = "gold.dim_client",
    source      = "silver_clients",
    keys        = ["id_cli"],
    sequence_by = F.col("maj_le"),           # rend insensible à l'ordre d'arrivée
    apply_as_deletes    = F.expr("_change_type = 'delete'"),
    except_column_list  = ["_change_type", "_commit_version"],
    stored_as_scd_type  = 2,                 # 1 écrase · 2 historise
)
```

```sql
CREATE FLOW maj_client AS AUTO CDC INTO gold.dim_client
FROM STREAM(LIVE.silver_clients)
KEYS (id_cli) SEQUENCE BY maj_le STORED AS SCD TYPE 2;
```

| Paramètre | Rôle |
|---|---|
| `keys` | la clé métier |
| `sequence_by` | **quel changement est le plus récent** |
| `stored_as_scd_type` | `1` écrase · `2` historise par intervalles |
| `apply_as_deletes` | la condition qui marque une suppression |
| `except_column_list` | les colonnes techniques à **ne pas** propager |

> Oublier `except_column_list` fait grossir la dimension sans raison : chaque changement
> technique crée une version.

### Lire un flux de changements Delta

```sql
ALTER TABLE silver.client SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

SELECT * FROM table_changes('silver.client', 12);   -- depuis la version 12
```

```python
(spark.readStream.format("delta")
   .option("readChangeFeed", "true")
   .option("startingVersion", 12)
   .table("silver.client"))
```

---

## Ce qu'on ajoute toujours en bronze

```python
.withColumn("_ingere_le", F.current_timestamp())
.withColumn("_fichier",   F.col("_metadata.file_path"))
.withColumn("_lot",       F.lit(BATCH_ID))
```

Sans ces trois colonnes, on ne peut ni tracer, ni rejouer, ni départager deux versions.
Et **tout reste en `STRING` en bronze** : c'est silver qui type.
