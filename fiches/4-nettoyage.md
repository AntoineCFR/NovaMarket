# 4 · Nettoyage — dédupliquer, nettoyer, typer, isoler

**L'ordre compte.** On déduplique d'abord — inutile de nettoyer quatre fois la même ligne
— puis on nettoie, puis on type, puis on isole ce qui n'a pas passé.

L'invariant à vérifier à chaque passage :

```
count(après déduplication) = count(silver) + count(quarantaine)
```

---

## 1. Dédupliquer

`dropDuplicates` ne convient pas dès que deux versions du même enregistrement diffèrent
d'un horodatage. La fenêtre, si.

```python
from pyspark.sql import Window

w = (Window.partitionBy("order_id")
           .orderBy(F.col("_fichier_modifie_le").desc(),
                    F.col("_fichier").desc()))          # ← le départage

deduplique = (brut.withColumn("_n", F.row_number().over(w))
                  .filter("_n = 1")
                  .drop("_n"))
```

**Deux règles :**

- **`row_number`, jamais `rank`.** À critère de tri identique, `rank` donne 1 aux deux
  lignes et les deux passent le filtre.
- **Toujours une colonne de départage.** Sans elle, deux exécutions sur les mêmes données
  peuvent garder deux lignes différentes — l'incident ne lève aucune erreur, il produit
  deux vérités.

Ne date pas sur `_ingere_le` : cette colonne date le **pipeline**, pas la **donnée**. Un
fichier de mars rechargé en avril passerait devant.

Pour un cas simple, sans version :

```python
brut.dropDuplicates(["order_id"])      # arbitraire : quelle ligne survit ?
brut.distinct()                        # lignes entièrement identiques
```

### Mesurer avant de décider

```python
dups = brut.groupBy("order_id").count().filter("count > 1")
print("clés en double :", dups.count())

# les copies sont-elles identiques entre elles ?
cols = [c for c in brut.columns if not c.startswith("_")]
print("contenus distincts :", brut.join(dups, "order_id").select(*cols).distinct().count())
```

Si les deux nombres sont égaux, toutes les copies sont identiques et n'importe quel
critère convient. Sinon, il faut choisir laquelle gagne — et le documenter.

---

## 2. Nettoyer

```python
def nettoie_texte(c):
    """Espaces de bord, espaces insécables, casse."""
    return F.upper(F.trim(F.regexp_replace(c, r"[ ​ ⁠]", " ")))

def nettoie_decimal(c):
    """Chaîne polluée -> décimal. Liste blanche : chiffres, virgule, point, signe."""
    net = F.regexp_replace(c, r"[^0-9,.\-]", "")
    return F.regexp_replace(net, ",", ".")          # le cast attend un point
```

| Besoin | Fonction |
|---|---|
| Retirer les espaces de bord | `F.trim` · `F.ltrim` · `F.rtrim` |
| Normaliser la casse | `F.upper` · `F.lower` · `F.initcap` |
| Substituer par motif | `F.regexp_replace(c, motif, remplacement)` |
| Extraire par motif | `F.regexp_extract(c, motif, groupe)` |
| Tester un motif | `c.rlike(motif)` |
| Découper | `F.split(c, ";")` |
| Assembler | `F.concat_ws("-", a, b)` |
| Remplir à gauche | `F.lpad(c, 13, "0")` |

### Traquer les caractères invisibles

```python
# inventaire exhaustif des points de code présents dans une colonne
(df.select(F.explode(F.split("adresse", "")).alias("ch"))
   .select("ch", F.ascii("ch").alias("code"))
   .groupBy("ch", "code").count()
   .orderBy("code").show(200, truncate=False))
```

`F.ascii` ne lit que le **premier** caractère : d'où le `split` en caractères d'abord.

---

## 3. Typer

**Le mode ANSI est actif.** Un `cast` qui échoue **lève** au lieu de rendre `NULL` — et
toute la quarantaine repose sur `isNull()`.

```python
TS_FORMAT = "yyyy-MM-dd HH:mm:ss"       # deux paires en majuscules : MM et HH

typed = (deduplique
  .withColumn("montant",  nettoie_decimal(F.col("montant")).try_cast("decimal(12,2)"))
  .withColumn("quantite", F.regexp_replace("quantite", r"[^0-9\-]", "").try_cast("int"))
  .withColumn("order_ts", F.try_to_timestamp("order_ts", F.lit(TS_FORMAT)))
  .withColumn("statut",   nettoie_texte(F.col("statut"))))
```

| Ce qui lève | Ce qu'on écrit |
|---|---|
| `.cast("int")` | `.try_cast("int")` |
| `F.to_timestamp(c, fmt)` | `F.try_to_timestamp(c, F.lit(fmt))` |
| `F.to_number(c, fmt)` | `F.try_to_number(c, F.lit(fmt))` |
| débordement arithmétique | `F.try_add` · `F.try_divide` · `F.try_multiply` |

**La règle : sur de la donnée venant de bronze, jamais `cast`, toujours `try_cast`.**

### Absences

```python
df.fillna(0, subset=["quantite"])          # ou df.na.fill(...)
df.dropna(subset=["order_id"])             # ou df.na.drop(...)
F.coalesce(F.col("a"), F.col("b"), F.lit(0))
F.nullif(F.col("statut"), F.lit(""))       # "" devient NULL
```

Avant de remplacer une absence par zéro, demande-toi ce qu'elle signifie. `sum`, `avg`,
`min`, `max` **ignorent** les absences : la moyenne de trois valeurs dont une est absente
se calcule sur deux.

---

## 4. Isoler — écarter sans jeter

On construit un tableau de motifs, on retire les cases vides, on trie sur sa taille.

```python
VALIDES = ["COMPLETED", "PENDING", "CANCELLED"]

valide = typed.withColumn("motifs", F.array_compact(F.array(
    F.when(F.col("order_ts").isNull(),              F.lit("INVALID_TIMESTAMP")),
    F.when(F.col("quantite").isNull() |
           (F.col("quantite") <= 0),                F.lit("INVALID_QUANTITY")),
    F.when(F.col("montant").isNull() |
           (F.col("montant") <= 0),                 F.lit("INVALID_PRICE")),
    F.when(~F.col("statut").isin(VALIDES),          F.lit("UNKNOWN_STATUS")),
)))

silver      = valide.filter(F.size("motifs") == 0).drop("motifs")
quarantaine = valide.filter(F.size("motifs") > 0)

silver.write.mode("overwrite").saveAsTable("ventes.silver.commande")
(quarantaine.withColumn("_mis_de_cote_le", F.current_timestamp())
            .write.mode("append").saveAsTable("ventes.silver.commande_quarantaine"))
```

**Pourquoi cette forme.** Il n'existe pas d'`append` conditionnel dans l'API DataFrame :
on construit un tableau de taille fixe où les branches non déclenchées valent `NULL`, puis
`array_compact` retire les trous. La forme générale, si tu veux filtrer autrement :

```python
F.filter(F.array(...), lambda x: x.isNotNull())
```

**Une ligne peut porter plusieurs motifs** — `F.size("motifs") > 1` les compte.

### Vérifier

```python
print(deduplique.count(), silver.count() + quarantaine.count())   # doivent être égaux

quarantaine.select(F.explode("motifs").alias("motif")) \
           .groupBy("motif").count().orderBy(F.desc("count")).show()
```

---

## Le degré de sévérité — trois niveaux

Le même besoin s'exprime autrement selon l'endroit où l'on contrôle.

```python
# en impératif : la quarantaine ci-dessus — la ligne est conservée avec son motif

# en déclaratif
@dlt.expect("montant_connu", "montant IS NOT NULL")     # compte, laisse passer
@dlt.expect_or_drop("qte_positive", "quantite > 0")     # écarte et compte
@dlt.expect_or_fail("cle_presente", "order_id IS NOT NULL")  # arrête le lot
```

```sql
-- au niveau de la table : l'écriture ENTIÈRE échoue
ALTER TABLE silver.commande ALTER COLUMN order_id SET NOT NULL;
ALTER TABLE silver.commande ADD CONSTRAINT devise_connue
  CHECK (devise IN ('EUR', 'USD', 'GBP'));
```

> **On commence toujours par observer.** Poser une règle bloquante dont on n'a pas mesuré
> le taux de violation, c'est programmer l'échec de la chaîne pour la nuit suivante.

Et la question à trancher avant d'écarter automatiquement : **où vont les lignes
écartées, et peut-on les rejouer ?** Si la réponse est « nulle part », le mécanisme n'est
qu'un filtre silencieux avec de meilleures manières.
