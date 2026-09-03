# Fiches pratiques — les gestes, avec le code

Écrites le 2 septembre 2026. Elles répondent à *« comment j'écris ça »*, pas à *« pourquoi
ça marche »*. Le raisonnement est dans le manuel et dans `exam/`.

| | Fiche | Ce qu'elle couvre |
|---|---|---|
| **1** | [Exploration](1-exploration.md) | Regarder le fichier brut, CSV, JSON, diagnostiquer en trois requêtes |
| **2** | [Structure](2-structure.md) | Catalogues, schémas, volumes, tables, commentaires, étiquettes, droits |
| **3** | [Ingestion](3-ingestion.md) | Batch · `COPY INTO` · Auto Loader · Kafka · JDBC · CDC et SCD2 |
| **4** | [Nettoyage](4-nettoyage.md) | Dédupliquer, nettoyer, typer, isoler en quarantaine |
| **5** | [Agrégation et publication](5-agregation-publication.md) | Réduire ou annoter · les quatre objets gold |
| **6** | [Orchestration](6-orchestration.md) | Le graphe, les valeurs de tâche, `run_if`, la porte de contrôle |
| **7** | [Pipelines déclaratifs](7-pipelines-declaratifs.md) | `dlt`, attentes, `AUTO CDC`, modes d'exécution |
| **8** | [CI/CD](8-ci-cd.md) | Un bundle complet, les six commandes, les secrets |

---

## Le fil qui les relie

Les quatre décisions qui reviennent partout :

**Où vit l'état ?** `COPY INTO` dans les métadonnées de la table · Auto Loader dans un
checkpoint · un curseur JDBC dans une table que tu maintiens · un pipeline déclaratif s'en
charge pour toi. Tout le reste en découle, `TRUNCATE` compris.

**Le passé peut-il changer ?** Si oui, ni table de flux ni curseur incrémental : il faut
une vue matérialisée, ou une réconciliation complète en doublure.

**Rejouer est-il sans conséquence ?** C'est l'idempotence, et c'est elle qui décide si
tu peux activer des reprises automatiques — pas le volume.

**Qui lira, et que lui promets-tu ?** Structure, sémantique, fraîcheur. Une table publiée
est une interface.

---

## Les pièges qui ne lèvent aucune erreur

Ceux-là méritent d'être relus ensemble, parce qu'ils partagent une propriété : **le
traitement réussit**.

| | Où |
|---|---|
| `TRUNCATE` puis `COPY INTO` charge **zéro ligne** | 3 |
| Les jetons CSV excédentaires sont **tronqués**, pas sauvés | 1 |
| Sous ANSI, un `cast` raté **lève** — d'où `try_cast` partout | 4 |
| `rank` laisse passer les ex æquo là où `row_number` n'en garde qu'un | 4 |
| Une fenêtre triée a un cadre qui s'arrête à la **ligne courante** | 5 |
| Une vue matérialisée à définition relative **décale sa fenêtre** en silence | 5 |
| Une tâche **sautée** n'est pas une tâche en échec : le graphe finit en vert | 6 |
| Sans `timeout_seconds`, rien n'arrête une tâche bloquée | 6 |
| `spark.read.table` dans un pipeline déclaratif **ne crée aucune dépendance** | 7 |
| Un rafraîchissement complet **efface l'historique** d'une SCD2 | 7 |
| Retirer une ressource du YAML la **supprime** au déploiement suivant | 8 |
| Une ressource créée à la main est **invisible** au bundle | 8 |
