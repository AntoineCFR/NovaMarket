# Fiche express — les méthodes d'ingestion

Écrite le 1er septembre 2026, en réponse à un flottement mesuré au blanc n°2.
**Section 2 de l'examen : 21 %, le deuxième poids.**

Le manuel traite tout ceci au chapitre 9, mais sous des noms génériques et sans jamais
prononcer les noms de produits Databricks. C'est ce pont qui manquait.

---

## 1. Le pont entre les deux vocabulaires

| Le manuel dit | Databricks dit | Où c'est |
|---|---|---|
| « Tout recharger » | **Full load / batch complet** | 9.1, p. 155 |
| « Seulement le nouveau » | **Incremental / incremental batch** | 9.1, p. 155 |
| « Le curseur » | **Watermark**, colonne de suivi | 9.2, p. 156 |
| « La capture des changements » | **CDC** — *Change Data Capture* | 9.3, p. 159 |
| « Par lots, par micro-lots, en continu » | **Batch · micro-batch · continuous** | 9.4, p. 160 |
| « Rejouer sans casser » | **Idempotence**, exactly-once | 9.5, p. 163 |
| « Recharger le passé » | **Backfill** | 9.6, p. 165 |
| « Quand le schéma bouge » | **Schema evolution / schema drift** | 9.7, p. 165 |
| « Les connecteurs managés » | **Lakeflow Connect** | 8.5, p. 145 |
| « Lire une base relationnelle » | **JDBC** | 8.1, p. 137 |

**« Incremental batch » n'est pas un produit** — c'est un *mode*. Il désigne toute
ingestion qui ne traite que le nouveau, mais dans un traitement qui démarre, travaille et
s'arrête. Deux mécanismes le réalisent : `COPY INTO`, et Auto Loader déclenché en
`availableNow`. C'est ce qui explique que le terme apparaisse dans la formation
Databricks sans correspondre à une commande.

---

## 2. Trois couches, pas six méthodes de même rang

Avant le tableau : **ces méthodes ne sont pas des sœurs.** Elles vivent à trois niveaux
différents, et c'est la confusion la plus coûteuse de la section 2.

| Couche | Ce que c'est | Exemples |
|---|---|---|
| **Le moteur** | **Structured Streaming** — l'API d'exécution incrémentale de Spark : `readStream`, `writeStream`, un checkpoint | — |
| **La source** | Ce qu'on met dans `.format(...)` | **`cloudFiles` = Auto Loader** · `kafka` · `delta` |
| **Le service managé** | Un produit qui fait le travail à ta place. **Pas une API Spark** | **Lakeflow Connect** |

Autrement dit : **Auto Loader *est* du Structured Streaming.** C'est son nom quand la
source est `cloudFiles`, c'est-à-dire des fichiers sur du stockage objet. Lire Kafka, c'est
le même moteur avec une autre source. Seul le `.format()` change.

```python
# Auto Loader : Structured Streaming, source = fichiers
(spark.readStream
   .format("cloudFiles")                          # <- la source
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", SCHEMA)   # évolution de schéma
   .load("/Volumes/cat/sch/vol/arrivees"))

# Kafka : Structured Streaming, source = bus de messages
(spark.readStream
   .format("kafka")                               # <- la source
   .option("kafka.bootstrap.servers", "broker:9092")
   .option("subscribe", "commandes")
   .option("startingOffsets", "latest")
   .load())
   # le contenu arrive en binaire dans `key` et `value` :
   # .select(F.col("value").cast("string")) puis from_json(...)

# L'écriture est identique dans les deux cas
(df.writeStream
   .option("checkpointLocation", CHECKPOINT)      # <- l'état vit ici
   .trigger(availableNow=True)                    # seul mode en Free Edition
   .toTable("bronze.commandes"))
```

En SQL et dans un pipeline déclaratif, le même moteur s'écrit `STREAM(...)` autour de la
source, et `read_files(...)` remplace Auto Loader.

`COPY INTO`, lui, **n'est pas du Structured Streaming** : c'est une commande SQL, sans
checkpoint, dont l'état vit dans les métadonnées de la table cible.

> **Free Edition** : aucun bus de messages n'est accessible. La lecture Kafka est du
> 📖 — à connaître, pas à pratiquer.

---

## 3. Les six méthodes, et ce qui les sépare

| Méthode | Source | Où vit l'état | Quand la choisir |
|---|---|---|---|
| **`spark.read` + `overwrite`** | Fichiers, tables | Nulle part | Rechargement complet, petits volumes, référentiels |
| **`COPY INTO`** | Fichiers sur stockage objet | **Métadonnées de la table cible** | Peu de fichiers, rythme prévisible, **équipe SQL** |
| **Auto Loader** = Structured Streaming `format("cloudFiles")` | Fichiers sur stockage objet | **Checkpoint séparé** | Beaucoup de fichiers, arrivées imprévisibles, évolution de schéma |
| **Structured Streaming** `format("kafka")` | Bus de messages (Kafka…) | Checkpoint séparé | Flux d'événements — ni `COPY INTO` ni Auto Loader ne lisent un bus |
| **JDBC** | Base relationnelle | À écrire soi-même (curseur) | Extraction d'une base applicative |
| **Lakeflow Connect** | SaaS et bases d'entreprise | **Géré pour toi** | Salesforce, Workday, SQL Server… CDC et schéma pris en charge |

### `COPY INTO` contre Auto Loader — la question qui tombe

C'est la comparaison la plus probable de la section 2. Elle se tranche sur trois axes.

| | `COPY INTO` | Auto Loader |
|---|---|---|
| **Écriture** | Commande **SQL** | Lecteur de flux (`readStream`), Python ou SQL |
| **État de progression** | Dans les **métadonnées de la table cible** | Dans un **checkpoint** que tu désignes |
| **Découverte des fichiers** | Liste le répertoire à chaque exécution | *Directory listing* **ou** *file notification* |
| **Passage à l'échelle** | Se dégrade quand les fichiers se comptent en millions | Conçu pour ça |
| **Évolution de schéma** | Limitée | `schemaEvolutionMode`, `rescuedDataColumn` |
| **Le piège** | `TRUNCATE` **ne remet pas** l'historique des fichiers chargés | Supprimer le checkpoint fait **tout recharger** |

**Les deux conséquences à retenir**, parce qu'elles sont la même idée vue des deux côtés :

- `TRUNCATE TABLE` puis `COPY INTO` identique → **aucune ligne chargée**. La table reste
  vide, sans la moindre erreur. L'état vit dans la table, mais `TRUNCATE` ne vide que
  les *données*, pas l'historique des fichiers.
- Supprimer le checkpoint d'Auto Loader → **tout est rechargé**. L'état vit à côté ;
  l'effacer remet le compteur à zéro.

### Les deux modes de découverte d'Auto Loader

| Mode | Comment | Quand |
|---|---|---|
| *Directory listing* | Liste le répertoire | Défaut. Convient jusqu'à des volumes modérés |
| *File notification* | S'abonne aux événements du stockage objet | Très grands répertoires, arrivées à haute fréquence |

### Les quatre modes d'évolution de schéma

| `cloudFiles.schemaEvolutionMode` | Sur une colonne nouvelle |
|---|---|
| `addNewColumns` | Le flux **échoue**, la colonne entre au schéma, le redémarrage la prend |
| `rescue` | Le schéma ne bouge pas ; la colonne va dans la colonne de sauvetage |
| `failOnNewColumns` | Échoue et ne repart pas tant que le schéma n'est pas corrigé à la main |
| `none` | Colonne **ignorée silencieusement** |

> **Le défaut dépend de toi** : `addNewColumns` si tu ne fournis **pas** de schéma
> explicite, `none` si tu en fournis un. *(À reconfirmer sur docs.databricks.com — détail
> susceptible de bouger.)*

Avec `addNewColumns`, **l'échec est volontaire** : le flux s'arrête pour signaler le
changement, et des reprises configurées sur la tâche le font repartir tout seul.

### Les deux colonnes de récupération — à ne jamais confondre

| Colonne | Ce qu'elle attrape | Ce qu'elle n'attrape pas |
|---|---|---|
| `_rescued_data` | **Écart au schéma** : colonne inattendue, type qui ne correspond pas | Une ligne inanalysable |
| `_corrupt_record` | **Échec d'analyse** : ligne JSON syntaxiquement invalide | Un écart de schéma |

Et le fait mesuré le 31 juillet sur tes propres données : **sur un CSV, les jetons
excédentaires sont tronqués, pas sauvés.** Aucune des deux colonnes ne les récupère.

---

## 4. Le curseur, et son piège de bordure

Une ingestion incrémentale par curseur lit `WHERE colonne_de_suivi > dernier_watermark`.

- Le `>` **strict** perd les lignes horodatées exactement à la valeur du curseur.
  Définitivement, et sans bruit.
- Le `>=` ne ferme que le cas d'égalité. Une transaction **horodatée avant** le watermark
  mais **validée après** la lecture reste invisible pour toujours — d'où la marge de
  sécurité en production (`watermark - INTERVAL 15 MINUTES`).
- Un curseur est **aveugle aux suppressions** : une ligne effacée n'a plus de date de mise
  à jour à comparer. On le double d'une réconciliation par comptage complet, moins
  fréquente.

Le watermark se calcule **sur ce qui a été écrit**, jamais sur le plan de lecture — un
DataFrame est un plan, et chaque action le rejoue depuis la source.

---

## 5. Batch, micro-batch, continu

| Mode | Déclenchement | Machines |
|---|---|---|
| **Batch** | Une exécution, un lot | Libérées à la fin |
| **Incremental batch** | `Trigger.AvailableNow` : traite tout ce qui attend, puis s'arrête | Libérées à la fin |
| **Micro-batch** | `Trigger.ProcessingTime('5 minutes')` | Allumées en permanence |
| **Continu** | Sans interruption | Allumées en permanence |

> **Free Edition** : sur serverless, seul **`availableNow`** est disponible en notebook et
> en job. C'est aussi le mode qui convient à la quasi-totalité des besoins réels, y
> compris ceux qu'on croit temps réel.

---

## 6. La matrice de décision

Pose les questions dans cet ordre.

1. **La source est-elle un fichier ?**
   Non, c'est un bus de messages → **Structured Streaming** directement.
   Non, c'est une base ou un SaaS → **Lakeflow Connect**, sinon **JDBC**.
2. **Combien de fichiers, et à quel rythme ?**
   Une douzaine par jour, prévisibles → **`COPY INTO`**.
   Des milliers, imprévisibles → **Auto Loader**.
3. **L'équipe écrit-elle du SQL ou du Python ?**
   SQL exclusivement → **`COPY INTO`** en tâche de requête SQL.
4. **Le schéma va-t-il bouger ?**
   Oui → **Auto Loader**, avec `schemaEvolutionMode` et `rescuedDataColumn`.
5. **Le passé peut-il être corrigé en source ?**
   Oui → surtout pas une table de flux : il faut une **vue matérialisée**, qui recalcule.

---

## À retenir — six phrases

1. `COPY INTO` range son état dans **la table cible** ; Auto Loader dans un **checkpoint**.
   Tout le reste en découle, `TRUNCATE` compris.
2. Ni l'un ni l'autre ne lit un **bus de messages** : c'est Structured Streaming.
3. `_rescued_data` = écart au schéma. `_corrupt_record` = échec d'analyse. Et sur CSV,
   les jetons en trop sont **tronqués**.
4. « Incremental batch » est un **mode**, pas un produit : `availableNow` ou `COPY INTO`.
5. Le `>` strict d'un curseur perd la bordure, **définitivement et en silence**.
6. Un curseur ne voit pas les **suppressions**. Il se double d'une réconciliation.
