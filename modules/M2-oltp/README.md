# M2 — Ingestion d'une source relationnelle par watermark

**Durée estimée** : 2 h · **Prérequis** : M1 validé

> 🧰 **Commence par `OUTILLAGE.md`** — les bibliothèques et fonctions dont tu auras besoin,
> sans les réponses. Et `docs/07-python-pour-le-parcours.md` si c'est ton premier passage.

---

## Objectif

Ingérer une base applicative vivante — celle qui gère les comptes clients et les plans
d'abonnement vendeurs — sans la relire intégralement à chaque exécution.

C'est le deuxième grand motif d'ingestion, complémentaire d'Auto Loader. Auto Loader
répond à « quels **fichiers** n'ai-je pas encore lus ? ». Ici la question devient
« quelles **lignes** ont changé depuis ma dernière extraction ? ». Le fichier n'existe
plus : c'est toi qui dois tenir l'état.

---

## Deux voies d'accès, un seul résultat évalué

Le grader ne regarde que le contenu de tes tables bronze. La façon dont tu atteins la
source ne l'intéresse pas. Choisis selon ton envie de plomberie.

> **Prends la voie B si tu es pressé.** L'exercice y est rigoureusement identique. La
> voie A t'apprend à créer une instance Postgres managée — instructif, mais c'est de
> l'administration de base, et **aucun objectif d'examen n'en dépend**. Compte 45 minutes
> de plomberie de plus, dont une installation logicielle.

### Voie B — extraction fichier (recommandée)

Les fichiers `data/lakebase/app_customers.csv` et `app_sellers.csv` **sont** le résultat
d'un `SELECT *` sur ces tables. Téléverse-les et lis-les avec `spark.read` :

```bash
databricks fs mkdir "dbfs:/Volumes/novamarket/landing/files/oltp"
```

```bash
databricks fs cp "data/lakebase/app_customers.csv" "dbfs:/Volumes/novamarket/landing/files/oltp/app_customers.csv" --overwrite
```

```bash
databricks fs cp "data/lakebase/app_sellers.csv" "dbfs:/Volumes/novamarket/landing/files/oltp/app_sellers.csv" --overwrite
```

> Voie B assumée : tu perds le passage par un vrai connecteur, tu gardes 100 % de la
> logique d'ingestion incrémentale. Si ton objectif est de valider des compétences de
> Data Engineer et non d'administrateur Postgres, c'est un choix défendable.

### Voie A — Lakebase Postgres (la vraie source relationnelle)

1. Crée une instance Lakebase depuis la barre latérale du workspace. Regarde du côté de
   **Compute → Database instances**, ou du bouton **New**. *(Le chemin exact bouge d'une
   version à l'autre — celui que ce README indiquait, « Apps → Lakebase Postgres », s'est
   révélé faux le 4 août 2026.)*
   Choisis **autoscaling** et la plus petite capacité : la charge est intermittente, et
   une instance réservée consommerait ton quota pendant que tu travailles ailleurs.
2. Exécute `data/lakebase/01_ddl.sql` dans le SQL Editor de Lakebase.
3. **Prérequis non fourni par Databricks : le client `psql`.** La commande
   `databricks psql` existe, mais elle délègue au client PostgreSQL local. Vérifie-le
   d'abord :

```bash
psql --version
```

   S'il est absent, installe les *Command Line Tools* de PostgreSQL — le serveur est
   inutile ici — puis rouvre ton terminal pour que le `PATH` soit relu :

```bash
winget install -e --id PostgreSQL.PostgreSQL.17
```

4. Connecte-toi. Sans argument, la CLI te propose la liste de tes instances :

```bash
databricks psql
```

5. Charge les deux tables. **Donne des chemins absolus** : `\copy` lit depuis ta machine,
   relativement au répertoire d'où tu as lancé `psql`.

```
\copy app_customers FROM 'C:/chemin/absolu/vers/data/lakebase/app_customers.csv' CSV HEADER
```

```
\copy app_sellers FROM 'C:/chemin/absolu/vers/data/lakebase/app_sellers.csv' CSV HEADER
```

   Contrôle : `SELECT count(*) FROM app_customers;` doit rendre **25 000**, et
   `app_sellers` **600**. Un de plus, et le `CSV HEADER` a été oublié.

6. Enregistre la base dans Unity Catalog pour la lire depuis Spark en SQL fédéré, en
   lecture seule.

Les scripts de changement des étapes suivantes s'exécutent aussi depuis psql :

```
\i 'C:/chemin/absolu/vers/data/lakebase/02_changes_D1.sql'
```

---

## Ce que tu dois produire

| Objet | Rôle |
|---|---|
| `novamarket.bronze.app_customers_raw` | Journal **append-only** des versions observées |
| `novamarket.bronze.app_sellers_raw` | Idem |
| `novamarket.ops.ingest_watermarks` | L'état de ton ingestion : jusqu'où tu es allé |

### Schéma imposé de `bronze.app_customers_raw`

| Colonne | Type |
|---|---|
| `customer_id`, `first_name`, `last_name`, `email`, `country`, `city`, `zip_code`, `segment` | `string` |
| `is_opt_in`, `is_deleted` | `boolean` |
| `created_at`, `updated_at` | `timestamp` |
| `_extracted_at` | `timestamp` |
| `_ingest_batch_id` | `string` |
| `_source_system` | `string` (`lakebase` ou `file`) |

`bronze.app_sellers_raw` suit la même logique : colonnes source + `is_active` en
`boolean`, `onboarded_at` en `date`, `updated_at` en `timestamp`, plus les trois colonnes
techniques.

> **Pourquoi typer ici alors qu'on refusait de le faire en M1 ?** Parce que la source
> n'est plus du texte. Un `timestamp` Postgres est déjà un `timestamp` : le conserver
> n'ajoute aucune interprétation, alors que le convertir en chaîne en détruirait une.
> La règle bronze reste « ne rien ajouter, ne rien perdre » — c'est son application qui
> change avec la nature de la source.

### Schéma imposé de `ops.ingest_watermarks`

| Colonne | Type |
|---|---|
| `source_name` | `string` |
| `watermark_column` | `string` |
| `watermark_value` | `timestamp` |
| `updated_at` | `timestamp` |

Une ligne par source, mise à jour à chaque extraction réussie.

---

## Déroulé

### Étape 1 — Extraction initiale

Aucun watermark n'existe. Charge l'intégralité des deux tables et **initialise**
`ops.ingest_watermarks` avec le `max(updated_at)` que tu viens de voir.

Attendu : 25 000 clients, 600 vendeurs.

### Étape 2 — Une journée d'activité

- **Voie A** : exécute `data/lakebase/02_changes_D1.sql` dans le SQL Editor de Lakebase.
- **Voie B** : téléverse `app_customers_v2.csv` et `app_sellers_v2.csv` — ce sont les
  mêmes tables après cette journée. Fais pointer ta lecture dessus.

Il s'y passe quatre choses : des changements de segment, des créations de comptes, des
suppressions douces (RGPD), et des changements de plan vendeur.

### Étape 3 — Extraction incrémentale

Relance ton notebook. Il doit :

1. lire le watermark stocké ;
2. n'extraire que les lignes concernées ;
3. les **ajouter** au journal bronze (surtout pas un `MERGE` : on garde toutes les
   versions observées, c'est ce qui rendra le SCD2 possible en M4) ;
4. avancer le watermark.

---

## Le piège du module

Ta requête d'extraction s'écrit-elle `updated_at > watermark` ou `updated_at >= watermark` ?

Les deux paraissent raisonnables. Elles ne ramènent pas le même nombre de lignes, et
**une des deux perd des données**. La différence tient à ce qui se passe quand une
transaction est ouverte avant ton extraction et validée juste après : sa ligne porte un
`updated_at` antérieur ou égal à ton watermark, alors que tu ne l'as jamais vue.

Réponds dans ton notebook :

1. Combien de lignes ton extraction ramène-t-elle en `>` ? En `>=` ?
2. Quel est le coût de chaque erreur : que perd-on avec `>` ? Que paie-t-on avec `>=` ?
3. En quoi le choix d'un journal **append-only** en bronze change-t-il l'arbitrage ?
4. Un watermark sur `updated_at` ne détecte pas un cas de figure. Lequel, et comment le
   contourne-t-on ici ?

---

## Critères d'acceptation

| # | Critère | Valeur attendue |
|---|---|---|
| 1 | `bronze.app_customers_raw` : schéma exact | voir tableau ci-dessus |
| 2 | Lignes après les deux extractions | entre **25 385** et **25 411** |
| 3 | Les 5 clients de bordure sont présents avec `city = 'Toulouse'` | **5** |
| 4 | Clients créés pendant la journée d'activité | **60** |
| 5 | Lignes portant `is_deleted = true` | **25** |
| 6 | `bronze.app_sellers_raw` : lignes | entre **640** et **641** |
| 7 | Vendeurs présents en 2 versions dans le journal | **40** ou **41** |
| 8 | Aucun `customer_id` nul | — |
| 9 | `ops.ingest_watermarks` : schéma exact, watermark clients à `2026-06-05 08:15:00` | — |
| 10 | `ops.pipeline_runs` contient une entrée `bronze_oltp` | — |

Le critère 3 est celui qui tranche : si ta table contient 25 385 lignes et qu'aucun
client de bordure n'a déménagé à Toulouse, ton extraction est incrémentale mais lacunaire.

---

## Points d'attention

- Un journal append-only croît indéfiniment. C'est acceptable ici (quelques dizaines de
  milliers de lignes par an) et c'est le prix de l'auditabilité. À l'échelle du million
  de lignes/jour, on bascule sur du CDC natif — hors périmètre de la Free Edition.
- Le watermark doit être écrit **après** l'écriture réussie des données, jamais avant.
  Réfléchis à ce qui se passe si ton job meurt entre les deux, dans chaque ordre.
- La suppression douce (`is_deleted`) est une chance : une vraie suppression physique
  serait invisible pour un watermark sur `updated_at`. C'est l'objet de la question 4.
