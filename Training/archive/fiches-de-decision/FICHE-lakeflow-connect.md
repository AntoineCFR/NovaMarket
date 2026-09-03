# 📖 Fiche — Lakeflow Connect

**Objectifs du guide** : *« Configurer Lakeflow Connect pour ingérer de façon fiable
depuis des sources d'entreprise variées vers des tables gouvernées par Unity Catalog »*
et *« importer des données depuis des sources telles que les fichiers locaux, les
connecteurs standard et les connecteurs managés de Lakeflow Connect »*.

**Pourquoi une fiche** : les connecteurs managés visent des sources commerciales
(Salesforce, Workday, SQL Server, ServiceNow…) qu'on ne peut pas provisionner en Free
Edition. Le sujet est au programme de la section la plus lourde après la transformation.

---

## Ce que c'est

Lakeflow Connect est la brique d'ingestion de Lakeflow — celle qui amène la donnée
**jusqu'à** Unity Catalog. Elle se décline en deux familles qu'il faut savoir distinguer,
parce que c'est la distinction que le guide nomme explicitement.

### Connecteurs standard

Des sources que tu atteins avec les moteurs intégrés, en écrivant un peu de
configuration : fichiers en stockage objet (c'est Auto Loader), Kafka et assimilés,
bases via JDBC.

Tu gères l'orchestration, l'incrémentalité et les erreurs. C'est ce que fait NovaMarket
en M1 et M2.

### Connecteurs managés

Des intégrations clés en main pour des applications d'entreprise. Tu fournis des
identifiants, tu choisis les objets à répliquer, et le service s'occupe du reste :
extraction initiale, capture des changements, gestion du schéma, reprise après incident.

Aucun code d'ingestion à écrire ni à maintenir.

---

## Comment on en configure un

1. **Créer une connexion** dans Unity Catalog — c'est un objet gouverné, avec ses propres
   privilèges. Elle porte les identifiants de la source.
2. **Créer un pipeline d'ingestion** : choisir la connexion, sélectionner les objets
   sources (tables, objets Salesforce…), désigner le catalog et le schéma de destination.
3. **Planifier** : le pipeline se lance à intervalle régulier ou en continu.
4. **Consommer** : les tables atterrissent dans Unity Catalog, gouvernées, traçables, et
   se lisent comme n'importe quelle table Delta.

Le point à retenir pour l'examen : **la connexion est un objet Unity Catalog**. Elle
s'accorde avec `GRANT`, elle apparaît dans le lineage, et elle centralise les
identifiants au lieu de les disperser dans des notebooks.

---

## Quand ça remplace du code, et quand ça ne le remplace pas

**Ça remplace du code** quand la source est une application d'entreprise standard, que le
besoin est de la réplication fidèle, et que l'équipe n'a pas envie de maintenir un client
d'API pendant trois ans. C'est le cas le plus fréquent, et le plus sous-estimé : un
script d'extraction Salesforce écrit à la main a un coût de possession très supérieur à
son coût d'écriture.

**Ça ne le remplace pas** quand il faut transformer à l'ingestion — un connecteur managé
réplique, il ne transforme pas ; quand la source est propriétaire ou interne, sans
connecteur ; ou quand le connecteur ne couvre pas les objets dont tu as besoin.

---

## Les trois pièges d'examen

**1. Confondre Lakeflow Connect et Lakeflow Declarative Pipelines.** Connect **amène** la
donnée dans le lakehouse. Les pipelines déclaratifs la **transforment** une fois qu'elle
est là. Les deux commencent par « Lakeflow » et font des choses différentes.

**2. Croire qu'un connecteur managé dispense de la couche bronze.** Il produit des tables
gouvernées, fidèles à la source. Ce qu'il ne fait pas : nettoyer, typer, dédupliquer,
arbitrer. Le médaillon reste entier.

**3. Choisir un connecteur managé pour une source fichier.** Pour des fichiers en
stockage objet, la réponse reste Auto Loader ou `COPY INTO`.

---

## Le rapprochement avec ce que tu as construit

| Étape NovaMarket | Équivalent Lakeflow Connect |
|---|---|
| M1 — Auto Loader sur le volume | Connecteur **standard** de fichiers |
| M2 — extraction Postgres par watermark | Ce qu'un connecteur **managé** ferait seul, y compris la détection des changements |
| M2 — la gestion des suppressions douces | Pris en charge par le CDC du connecteur |
| M2 — l'arbitrage `>` contre `>=` sur le watermark | **N'existerait pas** : le connecteur tient son propre état de progression |

Ce dernier point mérite d'être médité. Le piège de bordure de M2 — cinq lignes perdues
sans aucun signal — est un problème que tu as eu **parce que** tu as écrit l'ingestion
toi-même. C'est le meilleur argument en faveur des connecteurs managés, et il ne se
mesure pas en lignes de code économisées.

---

## QCM associés

`exam/qcm-section-2.md`.
