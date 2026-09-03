# 2 · Structure — catalogues, schémas, tables, volumes

**La hiérarchie** : `metastore → catalog → schema → table`. Il faut le droit de
**traverser** chaque niveau supérieur avant de pouvoir agir sur un objet.

---

## Créer

```sql
CREATE CATALOG IF NOT EXISTS ventes
  COMMENT 'Domaine commercial — sources OLTP et fichiers partenaires';

CREATE SCHEMA IF NOT EXISTS ventes.bronze
  COMMENT 'Dépôt fidèle, aucune transformation';
CREATE SCHEMA IF NOT EXISTS ventes.silver;
CREATE SCHEMA IF NOT EXISTS ventes.gold;

-- un volume : des FICHIERS gouvernés, pas des lignes
CREATE VOLUME IF NOT EXISTS ventes.bronze.landing
  COMMENT 'Zone de dépôt des fichiers partenaires';
```

Chemin d'un volume : `/Volumes/<catalog>/<schema>/<volume>/…`

> **Table ou volume ?** Une table gouverne des **lignes**, un volume gouverne des
> **fichiers**. Les deux existent en versions managée et externe.

## Créer une table

```sql
-- managée : Databricks possède les fichiers
CREATE TABLE ventes.silver.commande (
  order_id     STRING  COMMENT 'Identifiant métier, unique',
  order_ts     TIMESTAMP,
  montant      DECIMAL(12,2),
  statut       STRING
)
CLUSTER BY (order_ts)                    -- le rangement recommandé
COMMENT 'Commandes nettoyées et typées. Grain : une ligne par commande.'
TBLPROPERTIES ('quality' = 'silver', 'equipe' = 'data');

-- externe : un chemin, dans un external location déclaré
CREATE TABLE ventes.bronze.archive
  LOCATION 's3://bucket/archive/'
  AS SELECT * FROM ventes.bronze.commande;

-- à partir d'une requête
CREATE OR REPLACE TABLE ventes.gold.ca_mensuel AS
  SELECT date_trunc('MONTH', order_ts) AS mois, sum(montant) AS ca
  FROM ventes.silver.commande GROUP BY 1;
```

En PySpark :

```python
(df.write
   .mode("overwrite")                       # ou "append"
   .option("overwriteSchema", "true")       # remplacer le schéma entier
   .clusterBy("order_ts")
   .saveAsTable("ventes.silver.commande"))
```

## Managée contre externe

| | Managée | Externe |
|---|---|---|
| Qui possède les fichiers | Databricks | Toi |
| `DROP TABLE` | supprime les fichiers | **les fichiers survivent** |
| Optimisation automatique | oui | non |
| Conversion | `ALTER TABLE t SET MANAGED` | `ALTER TABLE t SET EXTERNAL` |

```sql
DESCRIBE EXTENDED ventes.silver.commande;   -- lire la ligne "Type"
```

---

## Documenter

C'est ce que verront les gens qui ne liront jamais ta documentation.

```sql
COMMENT ON CATALOG ventes IS 'Domaine commercial';
COMMENT ON SCHEMA ventes.gold IS 'Couche de publication, prête pour la BI';
COMMENT ON TABLE ventes.gold.fact_commande IS
  'Faits de commande. Grain : une ligne par ligne de commande.';

ALTER TABLE ventes.silver.commande
  ALTER COLUMN montant COMMENT 'Montant TTC en euros, hors frais de port';
```

**Le commentaire de table doit contenir le grain.** Une phrase : *« une ligne par … »*.
C'est l'information que personne n'écrit et que tout le monde cherche.

## Étiqueter — pour la gouvernance par attributs

```sql
ALTER TABLE ventes.silver.client SET TAGS ('domaine' = 'commercial');
ALTER TABLE ventes.silver.client
  ALTER COLUMN email SET TAGS ('pii' = 'true');
```

Une politique attachée au **catalogue** peut ensuite couvrir toute colonne portant
l'étiquette — y compris celles qui n'existent pas encore. C'est ce qui rend le travail
proportionnel au nombre de **règles** plutôt qu'au nombre d'**objets**.

---

## Inventorier

Le catalogue est lui-même une table, interrogeable en SQL.

```sql
-- toutes les tables d'un schéma
SELECT table_name, table_type, comment
FROM ventes.information_schema.tables
WHERE table_schema = 'silver';

-- les colonnes sans commentaire : la dette de documentation
SELECT table_name, column_name
FROM ventes.information_schema.columns
WHERE table_schema = 'gold' AND comment IS NULL;

-- qui a quel droit sur quoi
SELECT * FROM ventes.information_schema.table_privileges
WHERE table_name = 'commande';
```

## Droits

```sql
GRANT USE CATALOG ON CATALOG ventes TO `analystes`;
GRANT USE SCHEMA  ON SCHEMA  ventes.gold TO `analystes`;
GRANT SELECT      ON TABLE   ventes.gold.fact_commande TO `analystes`;

GRANT CREATE TABLE ON SCHEMA ventes.silver TO `ingenieurs`;

REVOKE SELECT ON TABLE ventes.gold.fact_commande FROM `stagiaires`;
```

> **Traverser n'est pas agir.** `USE CATALOG` et `USE SCHEMA` donnent le droit de
> *descendre* ; `SELECT`, `MODIFY`, `CREATE TABLE` donnent le droit d'*agir*. Un compte
> qui a `SELECT` sur une table mais pas `USE SCHEMA` sur son schéma échoue.

---

## À retenir

- `metastore → catalog → schema → table`, et il faut traverser chaque niveau.
- **Table = lignes, volume = fichiers.**
- `DROP TABLE` sur une **externe** ne supprime pas les fichiers.
- Le commentaire de table contient le **grain**.
- Les étiquettes rendent la gouvernance proportionnelle aux règles, pas aux objets.
