# Fiche express — optimisation, rangement, clustering

Écrite le 1er septembre 2026. **Section 6 : 10 %.** Elle irrigue aussi la section 3 (22 %)
et l'objectif coût de la section 1.

Tu as demandé un « cours pour les nuls » sur le sujet. Le voici, construit autour d'une
seule idée dont tout le reste découle.

---

## 1. L'idée unique : lire moins de fichiers

Tout ce chapitre sert **un seul objectif** — ouvrir moins de fichiers.

Pas « calculer plus vite ». Pas « prendre une plus grosse machine ». Doubler la taille du
cluster donne au mieux un gain **linéaire**, pour un coût **récurrent**, et laisse le
problème intact. Diviser par dix le volume lu donne un gain d'un **ordre de grandeur**,
pour un coût **nul**, et de façon permanente.

Retiens la hiérarchie, dans cet ordre d'efficacité :

1. **Ne lire que les colonnes utiles** — sur un format colonnaire, ça divise réellement
   les octets transférés
2. **Filtrer le plus tôt possible**, idéalement au moment de la lecture
3. **Ranger les données** pour que des fichiers entiers puissent être ignorés
4. *(seulement ensuite)* toucher aux réglages, ou à la taille des machines

---

## 2. Comment le moteur saute un fichier

C'est le mécanisme que tout le reste sert, et il est simple.

Chaque fichier d'une table Delta est accompagné de **statistiques** : pour un certain
nombre de colonnes, le **minimum**, le **maximum** et le nombre de valeurs nulles qu'il
contient.

Quand tu écris `WHERE jour = '2026-03-12'`, le moteur lit ces statistiques **avant**
d'ouvrir quoi que ce soit. Un fichier dont l'intervalle va du 1er au 5 janvier ne peut pas
contenir le 12 mars : il est **ignoré sans être ouvert**.

C'est ce qu'on appelle l'**élagage de fichiers** (*data skipping* / *file pruning*).

**Toute la question du rangement se ramène donc à une seule** : les intervalles min-max de
mes fichiers sont-ils **serrés** sur les colonnes que je filtre ?

- Données rangées par date → chaque fichier couvre quelques jours → intervalles serrés →
  élagage excellent
- Données écrites dans l'ordre d'arrivée → chaque fichier contient un peu de tout →
  intervalles larges → **aucun fichier ne peut être écarté**

### Deux conséquences directes

**Un filtre sur une *expression* de la colonne casse l'élagage.** Les statistiques portent
sur `jour`, pas sur `year(jour)` — le moteur ne peut plus rapprocher les deux.

```python
faits.filter("year(jour) = 2026")      # 1 460 fichiers ouverts
faits.filter("jour >= '2026-01-01'")   #    42 fichiers ouverts
```

**Un filtre sur une colonne produite plus tard ne remonte pas jusqu'à la lecture.** Si la
colonne naît d'une jointure ou d'un calcul, l'élagage a déjà eu lieu. Filtre **avant** la
jointure.

---

## 3. « Y a-t-il des index ? » — la réponse à ta question

**Non, pas au sens d'une base relationnelle.** Il n'existe pas de `CREATE INDEX` qui
construirait une structure de recherche à côté de la table.

Ce qui en tient lieu, ce sont les statistiques de la section précédente. Et ce qu'on
appelle « optimiser » consiste donc à **réorganiser les fichiers** pour rendre ces
statistiques discriminantes — jamais à ajouter une structure auxiliaire.

Une exception marginale, à connaître pour ne pas être surpris : les **index de Bloom**
existent sur Delta, pour les recherches d'**égalité** sur des colonnes à très forte
cardinalité (un identifiant technique). Ils sont peu employés, considérés comme hérités,
et ce n'est pas ce que l'examen attend. Si une option mentionne « un index », c'est
presque toujours le distracteur.

---

## 4. Les trois façons de ranger

| | `PARTITIONED BY` | `ZORDER BY` | **`CLUSTER BY`** *(liquid clustering)* |
|---|---|---|---|
| **Quoi** | Des **répertoires** physiques, un par valeur | Réordonne le contenu des fichiers sur plusieurs colonnes | Rangement déclaré sur la table, entretenu par la plateforme |
| **Quand ça s'applique** | À l'écriture | À chaque `OPTIMIZE`, **sur toute la table** | Incrémental, au fil des écritures |
| **Cardinalité** | **Faible** uniquement (pays, année) | Moyenne à forte | Toutes |
| **Changer les colonnes** | Réécrire toute la table | Relancer un `OPTIMIZE` complet | `ALTER TABLE … CLUSTER BY` — **sans tout réécrire** |
| **Le défaut** | Cardinalité élevée = **des milliers de petits fichiers** | Non incrémental, coûteux à rejouer | — |
| **Verdict 2026** | Hérité | Toujours supporté, **plus le défaut** | **La recommandation pour une table neuve** |

```sql
-- la recommandation actuelle, dès la création
CREATE TABLE gold.faits (…) CLUSTER BY (date_vente, id_client);

-- changer d'avis plus tard, sans réécrire les données
ALTER TABLE gold.faits CLUSTER BY (date_vente, region);

-- l'ancienne approche, toujours valide mais plus par défaut
OPTIMIZE gold.faits ZORDER BY (date_vente);
```

> Une variante automatique existe, où la plateforme choisit elle-même les colonnes
> d'après les requêtes observées. À connaître de nom ; vérifie sa disponibilité sur
> docs.databricks.com plutôt que sur ma parole.

### La règle qui décide des colonnes

**On range sur les colonnes qui servent de filtre, telles qu'on les observe dans les
requêtes réelles — jamais d'après le modèle logique.**

C'est le cas « Industrie » du manuel (p. 357) : une équipe range sa table de mesures par
identifiant de machine, parce que c'est la clé principale du modèle. Aucun gain. 90 % des
requêtes filtraient en réalité sur une **plage de dates**. Rangement refait sur la date :
temps de réponse divisé par sept.

---

## 5. L'entretien de la table

| Opération | Ce qu'elle fait | Quand |
|---|---|---|
| **`OPTIMIZE`** | **Compacte** les petits fichiers en gros fichiers | Sur toute table écrite fréquemment |
| **`VACUUM`** | Supprime les fichiers obsolètes | Libère du stockage, **au prix du retour arrière** |
| **`ANALYZE TABLE … COMPUTE STATISTICS`** | Donne à l'optimiseur de quoi estimer les tailles | **Après tout chargement massif** |
| *Predictive optimization* | Fait les trois automatiquement sur les tables **managées** | Quand c'est disponible, mieux qu'une maintenance manuelle |
| *Deletion vectors* | Marque les lignes supprimées au lieu de réécrire le fichier | Accélère `DELETE`, `UPDATE`, `MERGE` |

**Le symptôme des petits fichiers** : divise la taille de la table par son nombre de
fichiers. En dessous de quelques mégaoctets par fichier, le temps d'**ouverture** domine
le calcul — et la dégradation est **progressive**, donc invisible d'une semaine à l'autre.

**Ne compresse pas le stockage pour économiser.** Le stockage est bon marché, le calcul ne
l'est pas. Réduire la rétention rapporte peu et coûte cher en capacité de reprise.

---

## 6. Ce qui n'est pas du rangement

| Levier | Effet | Ce qu'il coûte |
|---|---|---|
| **`F.broadcast(petit)`** | Diffuse la petite table au lieu de tout redistribuer | **Une ligne** — le meilleur rapport effort/gain |
| `autoBroadcastJoinThreshold` | Le seuil de diffusion automatique. **`-1` la désactive** | Sert à mesurer ce que la diffusion apporte |
| *Adaptive Query Execution* | Ajuste les partitions et corrige le déséquilibre **en cours d'exécution** | Rien. Le régler à la main défait souvent une bonne décision |
| **Photon** | Moteur vectorisé, sans changer une ligne de code | Gain surtout sur agrégats et jointures |
| Le **salage** | Répartit artificiellement une clé trop fréquente | Complique le code — **dernier recours** |

Un chiffre à retenir : une jointure entre une table de faits et une dimension de 12 Mo,
redistribuée parce qu'elle dépasse **de peu** le seuil, dure **quarante minutes**.
Diffusée, **six**.

---

## 7. Les quatre signatures d'une lenteur

| Symptôme observable | Cause | Correction |
|---|---|---|
| Une tâche **cent fois** plus longue que la médiane | **Déséquilibre** : une clé concentre les lignes | Vérifier l'adaptatif ; traiter à part la clé dominante |
| **Octets écrits sur disque** au niveau d'une étape | **Débordement** mémoire | Partitions plus petites, étape intermédiaire |
| Taille ÷ nombre de fichiers **sous quelques Mo** | **Petits fichiers** accumulés | `OPTIMIZE`, automatisé si possible |
| Un même échange **répété** dans le plan | Résultat intermédiaire **recalculé** | Le moins cher à écarter : vérifier en premier |

**Le débordement ne lève aucune erreur.** Il n'apparaît dans aucun journal, le traitement
réussit — simplement cinq à dix fois plus lentement. Tout traitement dont la durée a
doublé sans incident mérite qu'on regarde de ce côté.

**Ajouter des machines ne corrige pas un déséquilibre.** Une tâche qui traite 40 % des
données durera aussi longtemps sur un cluster deux fois plus gros.

---

## 8. Mesurer — et l'erreur qui fait abandonner de bonnes optimisations

| Où | Ce qu'on y lit |
|---|---|
| `EXPLAIN FORMATTED` | **`number of files read`** — le chiffre le plus parlant du plan |
| | `PartitionFilters` vide → le partitionnement ne sert pas |
| | `PushedFilters` vide alors qu'on a filtré → chercher pourquoi |
| `DESCRIBE DETAIL t` | `numFiles`, `sizeInBytes`, colonnes de rangement |
| `DESCRIBE HISTORY t` | Ce qu'a fait chaque écriture |
| Onglet des étapes | Durée médiane contre maximum, octets sur disque |

> **Un gain se mesure en fichiers et en octets lus, jamais au chronomètre sur un petit jeu
> de données.** Sur un volume modeste, les coûts fixes dominent et la durée ne bouge pas —
> alors même que le mécanisme fonctionne parfaitement. Beaucoup d'optimisations correctes
> ont été abandonnées pour cette raison.

Et la première question devant une lenteur n'est jamais « comment accélérer » mais
**« est-ce nouveau, et depuis quand »**. L'historique des exécutions compare sans
détailler ; l'interface d'exécution détaille sans comparer. On commence par la première.

---

## À retenir — sept phrases

1. Tout sert un seul but : **ouvrir moins de fichiers**.
2. Le moteur saute un fichier grâce aux **statistiques min-max**. Ranger, c'est resserrer
   ces intervalles.
3. **Il n'y a pas d'index** au sens relationnel. Si une option en propose un, c'est le
   distracteur.
4. Sur une table neuve : **`CLUSTER BY`**. Le partitionnement est hérité, le Z-ordering
   n'est plus le défaut.
5. On range d'après les **requêtes observées**, jamais d'après le modèle logique.
6. Un filtre sur une **expression** de la colonne, ou sur une colonne **produite plus
   tard**, casse l'élagage.
7. Un gain se mesure en **fichiers lus**, pas en secondes.
