# Complément M5 — Les quatre objets de la couche gold

**Objectif du guide** : *« Comprendre la différence entre les objets de la couche gold —
vues matérialisées, vues, tables de streaming et tables — et savoir les construire pour
les équipes BI et analytique dans Unity Catalog. »*

Tu as construit les quatre au fil du parcours, sans jamais les comparer entre eux. C'est
pourtant la comparaison qui est évaluée.

---

## Le tableau

| | Table | Vue | Vue matérialisée | Table de streaming |
|---|---|---|---|---|
| Stocke des données | Oui | **Non** | Oui | Oui |
| Contenu | Ce qu'on y a écrit | Recalculé à chaque lecture | Résultat d'une requête, rafraîchi | Résultat d'un flux, en ajout |
| Rafraîchissement | Ton pipeline s'en charge | Sans objet | Automatique ou planifié, **incrémental si possible** | À chaque exécution du pipeline |
| Toujours à jour | Non | **Oui, par construction** | Non, à la dernière actualisation | Non |
| Coût de lecture | Faible | Celui de la requête sous-jacente | Faible | Faible |
| Coût d'écriture | À chaque exécution | Nul | À chaque actualisation | À chaque exécution |
| Masque / filtre de lignes | Oui | **Non** | Non | Oui |
| Où dans NovaMarket | `gold.fact_order_line` | `gold.v_top_products_90d` | `ldp.revenue_by_month_country` | `ldp.orders_bronze` |

---

## Comment choisir

**Table** — quand tu contrôles l'écriture et que le résultat est stable. C'est le défaut
pour un fait ou une dimension : tu maîtrises quand elle change, et tu peux y poser des
politiques de sécurité.

**Vue** — quand le résultat dépend du moment où l'on regarde. `v_top_products_90d` porte
une fenêtre glissante : matérialisée, elle serait périmée le lendemain, et périmée **sans
erreur**, ce qui est pire. En vue, elle est juste par construction.

C'est aussi le bon choix pour figer une définition métier sans dupliquer la donnée — une
vue qui applique systématiquement `WHERE is_revenue` évite à tout le monde de l'oublier.

**Vue matérialisée** — quand une même requête coûteuse est relue souvent et que la
fraîcheur à la minute n'est pas requise. Le moteur sait la rafraîchir **incrémentalement**
quand la requête s'y prête, ce qu'un `CREATE OR REPLACE TABLE AS SELECT` écrit à la main
ne saura jamais faire.

C'est le meilleur candidat pour remplacer `gold.agg_revenue_monthly`, dont M5 concluait
qu'elle ne se justifiait pas comme table.

**Table de streaming** — quand la source est en ajout continu et qu'on veut de
l'incrémental fiable sans gérer de checkpoint. C'est l'ingestion, pas l'agrégation.

---

## Les trois pièges d'examen

**1. « Une vue matérialisée est toujours plus rapide qu'une vue. »** En lecture, oui. Mais
elle a un coût d'actualisation, et elle peut servir des données périmées. Sur une requête
peu lue, la vue simple gagne au total.

**2. Croire qu'une vue matérialisée se rafraîchit toute seule en temps réel.** Elle se
rafraîchit à la demande, sur planification, ou dans le cadre d'un pipeline. Entre deux
actualisations, elle est en retard.

**3. Vouloir poser un masque de colonne sur une vue.** Impossible — c'est une limite
explicite d'Unity Catalog, et elle oriente toute la conception : les politiques vont sur
les tables, les vues en héritent par transitivité.

---

## Le raccourci mnémotechnique

> Le résultat dépend-il de **quand** on regarde ? → **vue**
> Est-il coûteux et relu souvent ? → **vue matérialisée**
> Est-ce que **je** contrôle l'écriture, et faut-il y poser de la sécurité ? → **table**
> La source arrive-t-elle en continu ? → **table de streaming**

---

## À faire, en trente minutes

Dans ton catalog, crée les quatre objets sur la même donnée — le CA mensuel — et compare :

1. `gold.tbl_ca_mensuel` par `CREATE TABLE AS SELECT`
2. `gold.v_ca_mensuel` par `CREATE VIEW`
3. `gold.mv_ca_mensuel` par `CREATE MATERIALIZED VIEW`
4. La table de streaming existe déjà : `ldp.orders_bronze`

Puis ajoute une ligne dans `silver.order_line` et regarde lesquels ont bougé, tout de
suite et après actualisation. C'est le geste n°6 de la liste du protocole de révision.

---

## QCM associés

`exam/qcm-section-3.md`.
