# 📖 Fiche — Tables managées et tables externes

**Objectif du guide** : *« Différencier les tables managées et externes dans Unity
Catalog et effectuer les opérations de base (créer, modifier, supprimer, convertir). »*

**Pourquoi une fiche et pas un exercice** : une table externe exige un *external
location*, donc un *storage credential*, donc un compte cloud. La Free Edition ne le
permet pas. Tout ce que tu as construit jusqu'ici est en tables **managées** — ce qui
est d'ailleurs la recommandation par défaut de Databricks.

---

## La différence en une phrase

Unity Catalog gère **les métadonnées dans les deux cas**. Ce qui change, c'est qui
possède **les fichiers**.

| | Table managée | Table externe |
|---|---|---|
| Emplacement des fichiers | Stockage géré par Unity Catalog | Chemin que tu désignes, dans ton stockage |
| `DROP TABLE` | **Supprime les fichiers** (avec 30 jours de rétention avant purge) | Supprime uniquement l'entrée du catalogue. **Les fichiers restent** |
| Format | Delta (ou Iceberg selon la configuration) | Delta, Parquet, CSV, JSON, ORC, Avro… |
| Optimisation automatique | Oui — *predictive optimization*, compactage, `VACUUM` gérés | Non, à ta charge |
| Partage du chemin avec un autre outil | Non | Oui, c'est tout l'intérêt |
| Recommandation Databricks | **Par défaut** | Cas justifiés seulement |

---

## Quand une table externe se justifie vraiment

Trois cas, et ils sont plus rares qu'on ne le croit :

1. **Un autre moteur écrit dans les mêmes fichiers.** Un job Spark hors Databricks, un
   outil d'ingestion tiers, un autre lakehouse.
2. **Migration en cours.** On enregistre l'existant sans le déplacer, le temps de basculer.
3. **Contrainte de localisation imposée.** Réglementation, contrat, ou données qui doivent
   rester dans un compartiment précis.

En dehors de ça, la table externe coûte : pas d'optimisation automatique, pas de
suppression propre, un chemin de plus à gouverner, et un risque de désynchronisation
entre le catalogue et les fichiers.

---

## Les opérations

```sql
-- Managée : aucun chemin
CREATE TABLE catalogue.schema.ma_table (id BIGINT, libelle STRING);

-- Externe : un chemin, dans un external location déclaré
CREATE TABLE catalogue.schema.ma_table_ext (id BIGINT, libelle STRING)
LOCATION 's3://mon-bucket/chemin/ma_table';

-- Savoir dans quel cas on est
DESCRIBE EXTENDED catalogue.schema.ma_table;   -- lire la ligne "Type" : MANAGED ou EXTERNAL
```

### Conversion

Dans les deux sens, sans réécrire les données :

```sql
ALTER TABLE catalogue.schema.ma_table SET MANAGED;    -- externe -> managée
ALTER TABLE catalogue.schema.ma_table SET EXTERNAL;   -- managée -> externe
```

`information_schema.tables` expose la colonne `table_type` — c'est la façon
programmatique d'auditer un catalog entier.

---

## Les trois pièges d'examen

**1. `DROP TABLE` sur une externe ne supprime pas les fichiers.** Conséquence pratique :
recréer la table au même chemin ressuscite les données. C'est parfois voulu, souvent
surprenant, et c'est une question classique.

**2. Deux tables externes peuvent pointer le même chemin.** Rien ne l'interdit, et les
deux se marcheront dessus. Avec des tables managées, c'est structurellement impossible.

**3. Ne pas confondre avec les volumes.** Un volume gouverne des **fichiers** (`/Volumes/...`),
une table gouverne des **lignes**. Les deux existent en versions managée et externe, et le
raisonnement est le même. Tout ce parcours utilise un volume managé — c'est le substitut
au bucket personnel qu'on ne peut pas déclarer en Free Edition.

---

## Ce que tu peux quand même vérifier maintenant

```sql
SELECT table_schema, table_name, table_type
FROM novamarket.information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'gold', 'ops')
ORDER BY table_type, table_schema, table_name;
```

Tout doit ressortir en `MANAGED`. Si une table apparaît en `VIEW`, c'en est une — et
souviens-toi de M10 : **on ne peut poser ni masque ni filtre de lignes sur une vue.**

---

## QCM associés

`exam/qcm-section-7.md`, questions sur les tables managées et externes.
