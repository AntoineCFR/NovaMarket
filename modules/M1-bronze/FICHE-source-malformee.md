# 📖 Fiche — Que faire d'une source malformée

**La situation** : le CSV des commandes n'échappe pas les `;` contenus dans
`shipping_address`. Environ 0,38 % des lignes comptent 16 champs au lieu de 14. Le
lecteur CSV en mappe 14 et **jette les deux derniers, sans erreur ni compteur**.

**Pourquoi une fiche** : le parcours a retenu une des quatre réponses possibles, et ce
n'est pas celle qu'une équipe choisirait en premier. Cette page dit lesquelles existent,
ce que chacune coûte, et pourquoi celle-là a été prise ici.

---

## Le tableau

| # | Réponse | Quand c'est le bon choix | Ce que ça coûte |
|---|---|---|---|
| 1 | **Faire corriger la source** | Toujours, en parallèle du reste | Délai hors de ton contrôle · ne répare pas l'historique déjà livré |
| 2 | **Parser correctement du premier coup** | Le défaut est structurellement borné — ici, il ne touche que le dernier champ | Tu quittes le parseur natif : évolution de schéma, sauvetage et typage repassent à ta charge |
| 3 | **Normaliser avant d'ingérer** | Le fichier reçu doit rester intact pour l'audit · le défaut est trop irrégulier pour être parsé | Une copie complète des données · un saut de plus dans la chaîne · une zone de plus à gouverner |
| 4 | **Table de réparation à côté** | Le pipeline tourne déjà en production et on découvre des dégâts sur l'historique | Dette · une jointure de plus · **une clé de réconciliation à trouver** |

---

## 1. Faire corriger la source

Ce n'est pas un cas limite exotique : c'est un bug d'export. Toute bibliothèque CSV
entoure de guillemets un champ contenant le séparateur — le module `csv` de Python le
fait par défaut. Quelqu'un a écrit l'équivalent de `";".join(champs)` à la main.

**Signale-le, systématiquement, et en premier.** Une ligne de code chez eux t'économise
un mécanisme permanent chez toi.

Deux raisons de ne pas s'arrêter là : le délai ne t'appartient pas, et **les fichiers
déjà livrés resteront cassés**. Six mois d'historique ne se rejouent pas parce que
l'export a été corrigé aujourd'hui.

---

## 2. Parser correctement du premier coup

C'est la meilleure réponse technique quand le défaut est **borné**. Ici il l'est : seul
le dernier champ déborde.

```python
F.split(ligne, ";", 14)
```

Le troisième argument est une limite : au plus 14 éléments, **le dernier absorbant tout
le reste**. `"12 rue X; Batiment A; 75011 Paris"` revient entier dans la 14ᵉ case. Une
passe, une table, zéro perte, zéro réconciliation.

> À vérifier toi-même avant de t'en servir en confiance — c'est un comportement documenté
> de `split`, mais tu as vu ce que valent les affirmations non exécutées.

**Ce que ça coûte** : tu lis la ligne brute et tu parses à la main. Tu perds donc tout ce
qu'Auto Loader apporte au-dessus du texte — l'inférence des noms de colonnes, la colonne
de sauvetage, et surtout **l'évolution de schéma**. La vague W3 ajoute `promo_code` et
`channel` à l'en-tête : avec un parsing manuel, c'est à toi de les détecter et de les
propager.

Si le défaut touchait une colonne **du milieu**, cette option disparaîtrait : rien ne
permettrait de savoir où s'arrête le champ abîmé et où commence le suivant.

---

## 3. Normaliser avant d'ingérer

Une étape préalable relit les fichiers reçus, réécrit les champs correctement échappés,
et dépose le résultat dans une seconde zone d'atterrissage. L'ingestion lit la zone
assainie et ne voit jamais le défaut.

Fréquent en environnement régulé, pour une raison précise : **le fichier reçu doit rester
bit à bit celui qu'on a reçu**, parce qu'il fait foi. On ne le corrige pas, on le
recopie.

**Ce que ça coûte** : une copie complète du volume, un traitement de plus à ordonnancer
et à surveiller, et une zone supplémentaire à gouverner. Sur des téraoctets quotidiens,
la copie n'est pas un détail.

---

## 4. Table de réparation à côté — le choix de ce parcours

Le flux principal ne bouge pas. Un second flux relit la ligne brute, reconstitue le champ
abîmé, et écrit une table de réconciliation que la couche silver recolle par `coalesce`.

**Le motif est réel**, et c'est un motif de *remédiation* : le pipeline tourne déjà en
production, on découvre des dégâts sur l'historique, on ne réécrit pas l'ingestion dans
l'urgence. On répare à côté, et **on ouvre un ticket pour retirer la rustine** une fois
la source ou le parseur corrigé. Une table de réparation qui vit trois ans est une dette,
pas une architecture.

**Le point de fragilité, à savoir nommer** : la réconciliation suppose une clé fiable.
Ici `order_line_id` ne l'est pas — 1 087 lignes réparées pour 1 073 clés distinctes,
parce que quatorze lignes défectueuses sont elles-mêmes dupliquées dans les fichiers.
Toute jointure doit donc dédupliquer d'abord, sous peine de faire grossir la table de
faits de quatorze lignes : assez pour rater le compte, trop peu pour se voir.

### Pourquoi ce choix ici

Pour une raison qui tient au **parcours**, pas à l'ingénierie. La vague W3 doit faire
jouer `cloudFiles.schemaEvolutionMode`, qui est un objectif d'examen. Parser à la main
(option 2) supprimerait ce mécanisme du programme.

C'est un arbitrage pédagogique assumé. En production, sur cette source précise, **l'option
2 serait le bon choix**, doublée de l'option 1.

---

## Ce qu'il faut retenir

**Il n'y a pas de réponse par défaut.** Le choix se déduit de trois questions :

1. Le défaut est-il **borné** ? S'il ne touche que le dernier champ, on peut parser. S'il
   touche une colonne du milieu, on ne peut plus.
2. Le fichier reçu doit-il **rester intact** ? Si oui, on normalise dans une copie, on ne
   corrige jamais sur place.
3. Le pipeline est-il **déjà en production** ? Si oui, la réparation à côté achète du
   temps — et c'est tout ce qu'elle achète.

Et une règle qui vaut pour les quatre : **on signale toujours à la source.** Aucune de ces
options n'est gratuite, et la seule qui supprime le problème plutôt que de le contenir se
joue chez quelqu'un d'autre.
