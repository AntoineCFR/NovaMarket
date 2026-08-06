# QCM — Section 5 : CI/CD

Réalisation le 29/07/2026
Début à 17h10
Fin à 17h16

**10 % de l'examen · ~5 questions · 12 questions ici**

Objectifs couverts : Git Folders (branches, commit, push, pull request) · configuration
par environnement avec les variables et overrides de bundle · déploiement de Declarative
Automation Bundles · CLI Databricks.

> ⚠️ Vocabulaire : le guide dit **Declarative Automation Bundles**, anciennement
> *Databricks Asset Bundles*. La commande CLI est restée `databricks bundle`.
> *Databricks Repos* s'appelle désormais **Git Folders**.

---

**1.** Une équipe veut une façon modulaire de déployer, versionner et orchestrer ses
pipelines ETL, avec du CI/CD et de la reproductibilité. Quelle fonctionnalité répond au
besoin ?

- **A.** Enregistrer les jobs comme des modèles dans Unity Catalog et promouvoir par alias
- **B.** Empaqueter la logique en *wheels* stockées dans des volumes, liées aux tâches
- **C.** Un notebook monté depuis un volume, déclenché par API, versionné par l'historique de révisions
- **D.** Des bundles déclaratifs, versionnés en Git et promus par du CI/CD automatisé

Réponse : D

---

**2.** Que fait `databricks bundle validate -t prod` ?

- **A.** Déploie le bundle sur la production
- **B.** Résout les variables, vérifie la configuration et l'affiche, sans rien modifier
- **C.** Supprime les ressources obsolètes
- **D.** Lance les jobs du bundle

Réponse : A

---

**3.** En mode de déploiement `development`, que fait Databricks automatiquement ?

- **A.** Préfixe les ressources et suspend les planifications
- **B.** Réduit la taille des clusters
- **C.** Désactive les notifications par courriel
- **D.** Chiffre les notebooks

Réponse : A

---

**4.** Un bundle déclare un job. On supprime ce job du fichier YAML, puis on relance
`databricks bundle deploy`. Que se passe-t-il ?

- **A.** Rien : le job reste, il faut le supprimer à la main
- **B.** Le job est supprimé du workspace
- **C.** Le déploiement échoue
- **D.** Le job est renommé avec un suffixe d'archivage

Réponse : A

---

**5.** Où faut-il stocker un mot de passe de base de données utilisé par un job déployé
par bundle ?

- **A.** Dans une variable du bundle, avec un `default`
- **B.** Dans un scope de secrets Databricks, référencé par `{{secrets/scope/cle}}`
- **C.** Dans un fichier `.env` commité à côté du YAML
- **D.** Dans les `base_parameters` de la tâche

Réponse : B

---

**6.** Deux développeurs déploient le même bundle en mode `development` dans le même
workspace. Que se passe-t-il ?

- **A.** Le second écrase les ressources du premier
- **B.** Chacun obtient ses propres ressources, préfixées par son identité
- **C.** Le second déploiement échoue pour cause de conflit
- **D.** Les ressources sont fusionnées

Réponse : B

---

**7.** À quoi sert le bloc `targets` d'un `databricks.yml` ?

- **A.** À déclarer les tables cibles des pipelines
- **B.** À définir les environnements et leurs surcharges de variables
- **C.** À lister les utilisateurs autorisés
- **D.** À configurer les notifications

Réponse : A

---

**8.** Un notebook non commité est ouvert dans un Git Folder. Le développeur bascule sur
une autre branche. Que risque-t-il ?

- **A.** Rien, les modifications suivent la branche
- **B.** De perdre ses modifications non commitées
- **C.** Un conflit de fusion automatique
- **D.** La suppression du Git Folder

Réponse : B

---

**9.** `databricks bundle deploy` est exécuté deux fois de suite, sans aucun changement.
Quel est le résultat ?

- **A.** Les ressources sont dupliquées
- **B.** Le workspace reste identique : le déploiement est idempotent
- **C.** Une erreur de conflit
- **D.** Une nouvelle version est empilée pour chaque ressource

Réponse : B

---

**10.** Un job créé à la main dans l'interface n'est pas déclaré dans le bundle. Que lui
arrive-t-il lors d'un `deploy` ?

- **A.** Il est supprimé
- **B.** Il reste intact : le bundle ne gère que ce qu'il a déployé
- **C.** Il est importé automatiquement dans le bundle
- **D.** Le déploiement échoue

Réponse : B

---

**11.** Une équipe n'a qu'un seul workspace mais veut simuler dev et prod. Quelle approche
est correcte ?

- **A.** C'est impossible, il faut deux workspaces
- **B.** Deux targets avec des surcharges de variables pointant vers deux catalogs distincts
- **C.** Deux dépôts Git distincts
- **D.** Deux comptes utilisateurs

Réponse : B

---

**12.** Quel élément **ne devrait pas** figurer dans un dépôt Git de projet data ?

- **A.** Les définitions de bundle en YAML
- **B.** Les notebooks de transformation
- **C.** Les fichiers de données générés ou extraits
- **D.** Les tests et leurs valeurs attendues

Réponse : C

---

Réponses : D, A, A, A, B, B, A, B, B, B, B, C

---

<details>
<summary><b>Corrigé — ne déplier qu'après avoir répondu aux 12 questions</b></summary>

**1 — D.** C'est la question 5 du guide officiel. Les bundles déclaratifs définissent les
ressources et le code, se versionnent en Git et se promeuvent par CI/CD. Les trois autres
options détournent des fonctionnalités conçues pour autre chose.

**2 — B.** `validate` résout les variables, contrôle la syntaxe et affiche la
configuration finale, **sans rien modifier**. C'est le meilleur moyen de comprendre ce
que les surcharges de target ont réellement produit.

**3 — A.** Le préfixage empêche deux développeurs de s'écraser ; la suspension des
planifications empêche des exécutions de développement de tourner toutes les nuits sur
les données de production. Deux problèmes distincts, une même option.

**4 — B.** Le YAML fait autorité : il décrit un **état souhaité**, pas une suite
d'actions. Retirer une ressource du fichier la supprime au déploiement suivant. C'est ce
qui surprend le plus souvent, et c'est voulu.

**5 — B.** Jamais dans le YAML ni dans une variable : tout cela est versionné en Git,
donc lisible par quiconque a accès au dépôt, et pour toujours. Le bundle peut en revanche
contenir le **nom** du scope et de la clé, qui sont de bonnes variables de target.

**6 — B.** Le mode `development` isole les déploiements par utilisateur. C'est
précisément ce qui rend un workspace unique utilisable par plusieurs personnes.

**7 — B.** `targets` définit les environnements, chacun pouvant surcharger les variables,
le mode de déploiement et l'hôte du workspace.

**8 — B.** Comme avec Git en ligne de commande, changer de branche avec des modifications
non commitées est risqué. Le réflexe reste le même : committer ou mettre de côté avant de
basculer.

**9 — B.** Le déploiement est idempotent. Rien n'est recréé, aucune version n'est empilée.
C'est la même logique qu'un pipeline idempotent : l'état est défini par la cible, pas par
l'historique des opérations.

**10 — B.** Le bundle ne gère que les ressources qu'il a lui-même déployées, identifiées
par son état interne. Un job créé à la main lui est invisible — ce qui est autant une
protection qu'une source de dérive.

**11 — B.** Deux targets, deux surcharges de la variable de catalog. Le mécanisme évalué
est identique à celui de deux workspaces. Ce qu'on ne teste pas ainsi : l'isolation des
permissions et la séparation des identités.

**12 — C.** Les fichiers de données n'ont rien à faire dans un dépôt de code : ils sont
volumineux, souvent reproductibles, mal gérés par Git, et le jour où quelqu'un applique
la même habitude à un extrait de production, l'incident est d'une autre nature.

</details>
