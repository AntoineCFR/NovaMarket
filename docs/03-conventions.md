# Conventions du projet

Les graders s'appuient sur ces conventions. Si tu en changes une, les graders échoueront.

## Unity Catalog

```
novamarket                          (catalog)
├── landing                         (schema)
│   └── files                       (volume managé)
│       ├── orders/                 dépôts CSV des commandes
│       ├── events/                 dépôts JSONL.gz des événements
│       └── ref/                    snapshots des référentiels
├── bronze                          fidélité à la source, aucune transformation métier
├── silver                          typé, nettoyé, dédupliqué, historisé
├── gold                            modèle métier, agrégats, vues de service
├── ldp                             sortie du pipeline déclaratif (M7 uniquement)
└── ops                             (schema)
    ├── checkpoints                 (volume managé) checkpoints et schémas Auto Loader
    ├── pipeline_runs               (table) journal d'exécution
    └── ...                         tables de qualité et de quarantaine (M3, M6)
```

> Si `CREATE CATALOG novamarket` échoue sur ton workspace, replie-toi sur le catalog
> `workspace` en préfixant les schemas : `nm_landing`, `nm_bronze`, … et déclare-le en
> tête des graders via le widget `catalog`.

## Nommage des tables

| Couche | Motif | Exemples |
|---|---|---|
| bronze | `<source>_raw` | `orders_raw`, `events_raw`, `ref_products_raw` |
| silver | `<entité>` au singulier | `order_line`, `event`, `product`, `customer_scd2` |
| gold | `dim_<entité>` / `fact_<fait>` / `agg_<sujet>` | `dim_seller`, `fact_order_line`, `agg_revenue_monthly` |
| ops | `<fonction>_<objet>` | `pipeline_runs`, `dq_checks`, `quarantine_order_line` |

## Colonnes techniques obligatoires en bronze

Toute table bronze porte ces colonnes, en plus des colonnes source :

| Colonne | Type | Contenu |
|---|---|---|
| `_rescued_data` | string | Colonne de sauvetage d'Auto Loader |
| `_source_file` | string | `_metadata.file_name` |
| `_source_file_modification_time` | timestamp | `_metadata.file_modification_time` |
| `_ingested_at` | timestamp | `current_timestamp()` au moment de l'écriture |
| `_ingest_batch_id` | string | Identifiant de l'exécution qui a écrit la ligne |

**Les colonnes source sont conservées en `STRING` en bronze.** Le typage est un acte
métier : il appartient à la couche silver, avec sa gestion des rejets. Une table bronze
qui perd de l'information à cause d'un cast raté n'est pas une table bronze.

## Chemins de checkpoint

```
/Volumes/novamarket/ops/checkpoints/<nom_du_flux>/checkpoint
/Volumes/novamarket/ops/checkpoints/<nom_du_flux>/schema
```

Un flux = un répertoire. Ne jamais partager un checkpoint entre deux flux.

## Journal d'exécution

Chaque notebook d'ingestion ou de transformation écrit une ligne dans
`novamarket.ops.pipeline_runs` :

| Colonne | Type |
|---|---|
| `run_id` | string |
| `task_name` | string |
| `source_name` | string |
| `started_at` | timestamp |
| `ended_at` | timestamp |
| `status` | string (`SUCCESS` / `FAILED`) |
| `rows_written` | bigint |
| `rows_rescued` | bigint |
| `files_processed` | bigint |
| `notes` | string |
