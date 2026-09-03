# Atelier — une boucle médaillon complète avec `COPY INTO`

Écrit le 2 septembre 2026. Trente lignes, trois dépôts quotidiens, un référentiel.
Volontairement petit : tu dois pouvoir **vérifier chaque compte à la main**.

Données : `data/atelier/landing/`

| Fichier | Lignes |
|---|---|
| `ventes/ventes_2026-09-01.csv` | 12 |
| `ventes/ventes_2026-09-02.csv` | 10 |
| `ventes/ventes_2026-09-03.csv` | 8 |
| `ref/ref_produits.csv` | 5 |

Séparateur `;`, en-tête présent, encodage UTF-8, **prix en virgule décimale**.

Quatre défauts seulement, et c'est tout : deux `order_ts` inanalysables, une quantité à
zéro, une quantité négative, et deux `order_id` en double. Rien d'autre.

---

## 0. Téléverser

```sql
CREATE CATALOG IF NOT EXISTS atelier;
CREATE SCHEMA IF NOT EXISTS atelier.bronze;
CREATE SCHEMA IF NOT EXISTS atelier.silver;
CREATE SCHEMA IF NOT EXISTS atelier.gold;
CREATE VOLUME IF NOT EXISTS atelier.bronze.landing;
```

Puis, depuis ton poste :

```bash
databricks fs cp -r "data/atelier/landing" "dbfs:/Volumes/atelier/bronze/landing"
```

*(ou l'interface : Catalog → atelier → bronze → landing → Upload)*

**Ne téléverse que les deux premiers fichiers de ventes.** Le troisième sert à l'étape 2.

---

## 1. Bronze — le premier `COPY INTO`

```sql
CREATE TABLE IF NOT EXISTS atelier.bronze.ventes (
  order_id     STRING,
  order_ts     STRING,
  customer_id  STRING,
  product_id   STRING,
  quantity     STRING,
  unit_price   STRING,
  status       STRING
);

COPY INTO atelier.bronze.ventes
FROM '/Volumes/atelier/bronze/landing/ventes'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'sep' = ';');
```

> **Tout en `STRING` en bronze, volontairement.** Bronze conserve la donnée telle qu'elle
> est arrivée ; c'est silver qui type. Si tu déclares `quantity INT` ici, la ligne à
> quantité négative passera quand même, mais celle dont le prix porte une virgule
> échouera — et tu auras perdu de la donnée avant de l'avoir regardée.

**Compte attendu : 22 lignes.** Relance la **même** commande : **0 ligne ajoutée**. Les
deux fichiers sont déjà dans l'historique de chargement.

## 2. L'incrément

Téléverse maintenant `ventes_2026-09-03.csv`, puis relance le **même** `COPY INTO`.

**8 lignes ajoutées, 30 au total.** Aucun fichier ancien n'est relu.

## 3. L'expérience à faire de tes mains

C'est le point de l'atelier. Exécute dans cet ordre et **note ce que tu observes** :

```sql
TRUNCATE TABLE atelier.bronze.ventes;
SELECT count(*) FROM atelier.bronze.ventes;        -- 0, évidemment
```

```sql
-- exactement la même commande qu'aux étapes 1 et 2
COPY INTO atelier.bronze.ventes
FROM '/Volumes/atelier/bronze/landing/ventes'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'sep' = ';');

SELECT count(*) FROM atelier.bronze.ventes;        -- ?
```

Puis la parade :

```sql
COPY INTO atelier.bronze.ventes
FROM '/Volumes/atelier/bronze/landing/ventes'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'sep' = ';')
COPY_OPTIONS ('force' = 'true');

SELECT count(*) FROM atelier.bronze.ventes;        -- ?
```

Regarde le résultat de la deuxième requête avant de lire la suite. C'est la seule chose de
cette journée qu'il faut voir plutôt que réviser.

---

## 4. Silver — dédupliquer, nettoyer, typer, mettre en quarantaine

Trois opérations, dans cet ordre.

**Dédupliquer** sur `order_id`. Outillage : `Window.partitionBy(...).orderBy(...)` et
`F.row_number()`. Rappelle-toi pourquoi `rank()` ne convient pas.

**Nettoyer et typer** :

| Colonne | Ce qu'il faut faire | Outillage |
|---|---|---|
| `unit_price` | virgule → point, puis décimal | `F.regexp_replace`, `.try_cast("decimal(10,2)")` |
| `quantity` | entier | `.try_cast("int")` |
| `order_ts` | horodatage au format `yyyy-MM-dd HH:mm:ss` | `F.try_to_timestamp` |
| `status` | normaliser | `F.trim`, `F.upper` |

> **`try_*` partout** : le mode ANSI est actif, un `cast` raté **lève** au lieu de rendre
> `NULL` — et toute la quarantaine repose sur `isNull()`.

**Écarter sans jeter** : une ligne part en quarantaine si son horodatage est inanalysable
**ou** si sa quantité n'est pas strictement positive. Écris-les dans
`atelier.silver.ventes_quarantaine` avec une colonne `quarantine_reasons` de type
`ARRAY<STRING>`.

Outillage : `F.array(F.when(...), F.when(...))` puis `F.array_compact`, et `F.size(...)`
pour trier les deux populations.

**L'invariant** : `count(silver) + count(quarantaine) = count(après déduplication)`.

## 5. Gold — joindre et agréger

Charge `ref_produits.csv` dans `atelier.gold.ref_produits`, puis construis
`atelier.gold.ca_par_jour_categorie` :

- jointure de silver sur le référentiel par `product_id`
- filtre sur `status = 'COMPLETED'`
- `chiffre_affaires = quantity * unit_price`
- groupé par **jour** et par **catégorie**

Deux vérifications que tu dois faire toi-même :

1. **La jointure ne doit pas changer le nombre de lignes.** Compte avant, compte après.
   `product_id` est unique dans le référentiel — si le compte bouge, c'est que non.
2. **Quelle forme de jointure ?** Si un produit vendu était absent du référentiel, veux-tu
   perdre la vente ? Choisis, puis vérifie que ton choix se voit dans le résultat.

---

<details>
<summary><b>Les attendus — ne déplier qu'après avoir tout exécuté</b></summary>

Chiffres **calculés à partir des fichiers livrés**, pas écrits de mémoire.

| Étape | Attendu |
|---|---|
| Bronze après les 2 premiers fichiers | **22** |
| Bronze, même `COPY INTO` relancé | **22** — 0 ligne ajoutée |
| Bronze après le 3ᵉ fichier | **30** |
| **Après `TRUNCATE` + même `COPY INTO`** | **0** — la table reste vide |
| Après `COPY_OPTIONS ('force' = 'true')` | **30** |
| Après déduplication sur `order_id` | **28** — 2 doublons retirés (`C0005`, `C0013`) |
| Quarantaine | **4** — `C0015`, `C0024` (horodatage), `C0018`, `C0026` (quantité) |
| Silver | **24** |
| Gold | **8** lignes |

Gold en détail — chiffre d'affaires par jour et catégorie, statut `COMPLETED` :

| Jour | Catégorie | CA |
|---|---|---|
| 2026-09-01 | Affichage | 996,00 |
| 2026-09-01 | Audio | 79,90 |
| 2026-09-01 | Peripheriques | 502,60 |
| 2026-09-02 | Affichage | 498,00 |
| 2026-09-02 | Audio | 159,80 |
| 2026-09-02 | Peripheriques | 49,00 |
| 2026-09-03 | Affichage | 498,00 |
| 2026-09-03 | Peripheriques | 384,20 |
| | **TOTAL** | **3 167,50** |

**Pourquoi la table reste vide après le `TRUNCATE`** : l'historique des fichiers chargés
vit dans les **métadonnées de la table**, et `TRUNCATE` ne vide que les *données*. Les
trois fichiers sont toujours marqués comme traités, donc aucun n'est considéré comme
nouveau. Aucune erreur n'est levée — c'est ce silence qui rend le piège coûteux.

Auto Loader se comporte à l'inverse : son état vit dans un **checkpoint séparé**, et
c'est en supprimant ce checkpoint qu'on provoque un rechargement complet.

</details>

---

## Si tu veux pousser

Deux prolongements courts, seulement si le temps le permet :

- **Auto Loader sur le même répertoire**, avec `cloudFiles`, `schemaLocation` et
  `trigger(availableNow=True)`. Compare : où vit l'état, et que se passe-t-il si tu
  supprimes le checkpoint ?
- **Un masque de colonne sur `customer_id`** dans gold, écrit **fermé par défaut**, puis
  vérifié **dans les deux sens** — un compte autorisé voit, un compte non autorisé ne voit
  pas. C'est le geste n°10, et il tient en vingt minutes.
