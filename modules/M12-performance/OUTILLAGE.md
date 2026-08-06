# 🧰 Outillage — M12

*Cette fiche dit **avec quoi**, pas **comment**.*

> **Section 6 — 10 % de l'examen, et ta section la plus faible : 50 % au diagnostic.**
> Six erreurs sur douze, dont quatre sur la même racine — **où s'exécute quoi, et où ça
> se lit**. C'est la deuxième priorité du parcours après M3, et c'est un trou de
> connaissance, pas un réflexe faux : il se comble en le lisant une fois.

---

## Ce que tu vas faire

Regarder ce que Spark fait réellement quand tu lances une requête, nommer les trois
pathologies classiques, et mesurer un gain autrement qu'au chronomètre.

---

## 1. Le vocabulaire d'exécution, d'abord

Sans ça, le Spark UI est illisible.

| Terme | Ce que c'est |
|---|---|
| **Driver** | le processus qui pilote. `collect()` y rapatrie les données — **cause n°1 des OOM** |
| **Exécuteur** | les processus qui traitent les partitions en parallèle |
| **Job → Stage → Task** | une action → une étape entre deux shuffles → une partition traitée |
| **Shuffle** | redistribution des données entre exécuteurs. Coûteux, inévitable pour un `join` ou un `groupBy` |
| **Partition** | une tranche de données, l'unité de parallélisme |

`collect()` ramène **sur le driver**. Le remède à un OOM de driver n'est pas d'agrandir le
driver : c'est d'agréger avant de ramener, ou de ne pas ramener.

## 2. Les trois pathologies

| Pathologie | Signature | Remède |
|---|---|---|
| **Skew** | une tâche à 10 min quand la médiane est à 30 s ; *shuffle read* max ≫ médian | vérifier que l'AQE et son traitement du *skew join* sont actifs ; salage en dernier recours |
| **Shuffle** | beaucoup de données échangées entre stages | filtrer plus tôt, diffuser la petite table |
| **Spill** | colonnes ***Spill (memory)*** et ***Spill (disk)*** non nulles au niveau du stage | réduire la taille des partitions, moins de données |

> **Le *spill* ne lève aucune erreur.** Le job réussit, beaucoup plus lentement. Il ne se
> lit **pas** dans les journaux : il se lit dans les **métriques de stage** du Spark UI.
> C'est précisément pourquoi il passe inaperçu — et c'est la question que tu as ratée deux
> fois au diagnostic.

## 3. Où lire quoi

| Question | Où |
|---|---|
| Ce job est-il plus lent qu'avant ? | **historique des exécutions** de l'interface Lakeflow Jobs |
| Pourquoi cette exécution-ci est-elle lente ? | **Spark UI**, onglet *Stages* |
| Pourquoi le cluster n'a-t-il pas démarré ? | **journal d'événements du cluster** — le Spark UI ne sert à rien, il n'y a pas eu d'application |
| Que s'est-il passé sur cette table ? | `DESCRIBE HISTORY` |
| Comment cette table est-elle stockée ? | `DESCRIBE DETAIL` |

Le Spark UI détaille **une** exécution, sans point de comparaison. L'historique compare
sans détailler. Les deux sont complémentaires, et confondre leurs usages fait perdre du
temps.

## 4. Les paramètres

| Paramètre | Ce qu'il fait |
|---|---|
| `spark.sql.shuffle.partitions` | nombre de partitions après un shuffle |
| `spark.sql.autoBroadcastJoinThreshold` | seuil de diffusion automatique — **`-1` la désactive** |
| `spark.sql.adaptive.enabled` | AQE : réajuste le plan à l'exécution |
| `spark.sql.adaptive.skewJoin.enabled` | découpe les partitions surdimensionnées |
| `spark.conf.get(nom)` · `spark.conf.set(nom, v)` | lire, modifier |

`-1` **désactive** la diffusion et force le shuffle. Utile pour mesurer ce que la
diffusion apporte — et pour éviter une saturation quand l'estimation de taille est fausse.
Diffuser une table de 2 Go la copie **intégralement sur chaque exécuteur** : un shuffle
lent vaut mieux qu'un OOM.

## 5. L'organisation physique

| Outil | Ce qu'il fait |
|---|---|
| `CREATE TABLE ... CLUSTER BY (a, b)` | **liquid clustering** — la recommandation actuelle |
| `ALTER TABLE ... CLUSTER BY (a, b)` | changer les colonnes, sans réécrire |
| `OPTIMIZE table` | compacter |
| `OPTIMIZE table ZORDER BY (a)` | l'ancien mécanisme, supporté mais **plus le défaut** |
| `VACUUM table` | supprimer les fichiers obsolètes |
| *predictive optimization* | `OPTIMIZE` et `VACUUM` automatiques sur les **tables managées** |

Le partitionnement classique fige une arborescence : trop de partitions produit une
myriade de petits fichiers, trop peu ne filtre rien, et un mauvais choix de colonne coûte
cher à corriger. Le liquid clustering évite ce piège.

> **Piège de vocabulaire** : toute IA entraînée avant le changement te recommandera
> `ZORDER`. L'examen attend `CLUSTER BY`.

## 6. Mesurer un gain

| Outil | Ce qu'il fait |
|---|---|
| `df.explain(True)` | le plan d'exécution |
| `DESCRIBE DETAIL` → `numFiles`, `sizeInBytes` | l'état physique |
| métriques de scan du Spark UI | **fichiers lus / fichiers totaux** |

> **Ne mesure pas un regroupement au chronomètre.** Sur un petit jeu, la durée est dominée
> par les coûts fixes et ne bougera pas, alors que le mécanisme fonctionne parfaitement.
> La vraie mesure est le **taux d'élagage de fichiers**.

---

## Le meilleur levier, et il n'est pas dans ce tableau

Face à une tâche lente, **réduire le volume lu** — filtrer plus tôt, élaguer, ne lire que
les colonnes utiles — donne souvent un ordre de grandeur, pour un coût nul et un bénéfice
permanent. Agrandir le cluster donne au mieux un gain linéaire, pour un coût récurrent, et
masque le problème au lieu de le corriger.

## Le vocabulaire à retenir

**Driver / exécuteur** · **stage / task** · **skew, shuffle, spill** · **AQE** ·
**liquid clustering** · **predictive optimization** · **élagage de fichiers**.

Section 6 — 10 %.
