# Fiche express — les types de compute

Écrite le 1er septembre 2026. **Section 1 : 6 %** — l'objectif « services de compute :
caractéristiques, limites, modèles de coût, choix selon la charge ». Elle irrigue aussi
la section 6 (diagnostic) et l'objectif coût.

Petit poids, mais c'est un objectif du guide qui n'est traité nulle part ailleurs dans le
parcours, et la Free Edition ne t'en montre qu'**une seule** des variantes.

---

## 1. Les quatre familles

| Type | Ce que c'est | Pour quoi |
|---|---|---|
| **All-purpose** *(interactif)* | Une machine allumée, partagée, à laquelle on attache des notebooks | Exploration, développement, travail à plusieurs |
| **Job compute** *(cluster de job)* | Créée **pour une exécution**, détruite à la fin | Traitements planifiés |
| **SQL warehouse** | Un moteur dédié aux requêtes SQL et à la restitution | Tableaux de bord, analystes, outils BI |
| **Serverless** | Databricks fournit et gère la capacité. **Aucun choix d'instance** | Démarrage en secondes, tout usage |

Le **compute de pipeline** est une cinquième forme, mais tu ne la choisis pas : elle est
gérée par le pipeline déclaratif lui-même.

---

## 2. Le SQL warehouse — celui qu'on oublie

C'est la réponse quand la charge est faite de **requêtes SQL courtes et concurrentes**,
typiquement un tableau de bord consulté par des dizaines de personnes.

| Variante | Démarrage | Remarque |
|---|---|---|
| Classic | Minutes | Le plus ancien |
| Pro | Minutes | Fonctionnalités avancées |
| **Serverless** | **Secondes** | La capacité est prête ; facturation à l'usage |

Trois propriétés à retenir pour l'examen :

- **Démarrage en secondes** sur la variante serverless — décisif pour un usage interactif
  où personne n'attend cinq minutes
- **Mise à l'échelle pour la concurrence** : il ajoute des unités quand les requêtes
  s'accumulent, au lieu de les mettre en file
- **Facturation à l'usage**, sans machine allumée en permanence

Un cluster de job ne convient pas ici : il est conçu pour démarrer, travailler, s'éteindre.
Le rallumer à chaque requête d'un tableau de bord ferait payer la latence de démarrage
cinquante fois par matinée.

---

## 2 bis. ⚠️ Correction du 2 septembre — ce que dit le guide officiel

Le guide d'examen contient une question d'exemple sur exactement ce cas, et **sa réponse
n'est pas celle que cette fiche donnait**. Voici l'énoncé, résumé :

> *Des analystes lancent des requêtes SQL ad hoc toute la journée sur des tables Delta.
> Il faut de bonnes performances, un **démarrage rapide**, le support de **plusieurs
> utilisateurs simultanés**, et maîtriser le coût.*
>
> **A.** un cluster de job avec autoscaling · **B.** un cluster tout-usage à nombre de
> nœuds fixe · **C.** **un cluster *high-concurrency* avec autoscaling** · **D.** un
> cluster mono-nœud
>
> **Réponse officielle : C.**

Deux enseignements, et le second vaut plus que le premier.

**Le vocabulaire.** Databricks emploie encore *high-concurrency cluster* dans ses propres
supports. Si tu le vois proposé, c'est une option sérieuse, pas un piège de nom périmé.

**Et surtout : on coche la meilleure des options offertes, pas la réponse idéale.** Aucune
des quatre options ne mentionnait un SQL warehouse. Devant un énoncé de charge SQL
concurrente, cherche le SQL warehouse serverless d'abord ; s'il n'est pas là, l'option
« concurrence + autoscaling » est la bonne. Ne cherche pas la réponse que tu aurais écrite
— cherche la moins fausse des quatre.

## 3. Le modèle de coût

Le prix se décompose en deux : la **machine** (facturée par le fournisseur de cloud) et
l'**unité Databricks** consommée. Ce qui change d'un type à l'autre, c'est le **tarif de
l'unité**.

> **Le job compute coûte moins cher que l'all-purpose, à machine égale.** C'est la raison
> économique de basculer les traitements planifiés hors des clusters interactifs.

### Les leviers, par ordre de rendement

| Levier | Effet | Ce qu'il faut savoir |
|---|---|---|
| **`autotermination_minutes`** | Éteint une machine inactive | **Le premier poste d'économie, de très loin.** Une machine allumée se paie même inactive |
| Machines dédiées à la tâche | Créées puis détruites pour une exécution | Bien moins chères que l'interactif |
| `autoscale {min, max}` | Ajuste le nombre de machines à la charge | Fixer un plancher **et** un plafond |
| Instances ponctuelles *(spot)* | Capacité au rabais, révocable | **Jamais pour le pilote** |
| Pools d'instances | Machines préchauffées | Réduit la latence de démarrage d'un cluster de job |
| `custom_tags` | Rattache la dépense à une équipe | Sans elles, **aucune imputation possible** |

Et les deux erreurs de raisonnement les plus fréquentes :

- **Économiser sur le stockage.** Le stockage est bon marché, le **calcul** est le vrai
  poste. Réduire la rétention rapporte peu et coûte cher en capacité de reprise.
- **Sous-dimensionner.** Une machine deux fois plus petite qui met trois fois plus de
  temps — parce qu'elle **déborde sur disque** — revient plus cher qu'une machine
  correctement taillée. Le sous-dimensionnement coûte plus que le surdimensionnement.

---

## 4. La matrice de décision

| La charge | Le compute |
|---|---|
| J'explore, j'écris du code dans un notebook | **All-purpose** — avec extinction automatique, sans exception |
| Un traitement ETL planifié chaque nuit | **Job compute**, créé et détruit pour l'exécution |
| Un tableau de bord, requêtes courtes et concurrentes | **SQL warehouse serverless** |
| Un pipeline déclaratif | Le **compute de pipeline**, géré par le pipeline |
| Je veux démarrer en secondes sans rien configurer | **Serverless** |

---

## 5. Ce que la Free Edition t'a caché

Tu n'as jamais vu qu'**une seule** de ces options : le **serverless**. Conséquences à
connaître, parce que l'examen ne fait pas la différence :

- **Aucun choix d'instance.** Pas de type de machine, pas de nombre de nœuds, pas de
  configuration mémoire. Si une question propose de « dimensionner les nœuds », ce n'est
  pas ton environnement.
- **`.cache()` / `.persist()` refusés** — `NOT_SUPPORTED_WITH_SERVERLESS`, SQLSTATE 0A000.
  Le concept reste au programme ; seule la pratique t'est fermée.
- **Un seul mode de déclenchement de flux** : `availableNow`. Pas de
  `trigger(processingTime=…)`, pas de mode continu.
- **Le mode ANSI est actif**, donc `try_cast` partout sur du bronze.

---

## À retenir — cinq phrases

1. **Quatre familles** : all-purpose, job, SQL warehouse, serverless.
2. Requêtes SQL courtes et **concurrentes** → **SQL warehouse serverless** : démarrage en
   secondes, mise à l'échelle sur la concurrence, facturation à l'usage. **Mais si le SQL
   warehouse n'est pas proposé, la bonne option est « concurrence + autoscaling »** — voir
   la correction en 2 bis.
3. Le **job compute coûte moins cher** que l'all-purpose, à machine égale.
4. **`autotermination` est le premier levier d'économie**, de très loin — une machine
   allumée se paie même inactive.
5. **Serverless = aucun choix d'instance.** C'est le seul compute que la Free Edition t'a
   montré.
