# 📖 Fiche — Choisir sa méthode d'ingestion

**Objectif du guide** : *« Prioriser entre Auto Loader, Lakeflow Connect (connecteurs
standard et managés), connecteurs partenaires et autres méthodes d'ingestion, selon des
exigences techniques telles que le volume, la fréquence, les types de données et les
besoins de gouvernance. »*

C'est un objectif d'**arbitrage**. Les questions d'examen sont des mises en situation :
on te décrit un contexte, et les quatre réponses sont toutes des méthodes valides — une
seule répond au contexte.

---

## L'arbre de décision

**1. La source est-elle un fichier dans du stockage objet ?**

- Oui, et les fichiers arrivent en continu ou par lots fréquents → **Auto Loader**
- Oui, mais c'est un chargement ponctuel ou peu fréquent, sur peu de fichiers → **`COPY INTO`**
- Non → question 2

**2. La source est-elle une application ou une base d'entreprise connue** (Salesforce,
Workday, SQL Server, PostgreSQL, ServiceNow…) ?

- Oui, et un connecteur managé existe → **Lakeflow Connect, connecteur managé**
- Oui, mais pas de connecteur → question 3

**3. La source expose-t-elle du JDBC/ODBC ?**

- Oui → **lecture JDBC en notebook, orchestrée par un job**, avec extraction incrémentale
  par watermark. C'est ce que fait M2
- Non, c'est une API REST → **client REST en notebook**, orchestré par un job. Le plus
  coûteux à maintenir : c'est du code sur mesure, et il faut gérer la pagination, les
  limites de débit, les jetons et les reprises

**4. Le flux vient-il d'un bus de messages** (Kafka, Event Hubs, Kinesis) ?

- → **Structured Streaming** directement sur la source, pas de fichier intermédiaire

---

## Le tableau comparatif

| | Auto Loader | `COPY INTO` | Lakeflow Connect managé | JDBC scripté |
|---|---|---|---|---|
| Source | Fichiers | Fichiers | Applications, bases | Bases |
| État de progression | Checkpoint (dans ton stockage) | Historique de la table cible | Géré par le service | À ta charge |
| Millions de fichiers | Conçu pour | Se dégrade | Sans objet | Sans objet |
| Streaming | Oui | Non — c'est du batch | Selon le connecteur | Non |
| Évolution de schéma | Oui, plusieurs modes | Avec `mergeSchema` | Gérée | À ta charge |
| Code à maintenir | Faible | Très faible | **Nul** | Élevé |
| Coût | Compute | Compute | Compute + licence connecteur | Compute + temps d'ingénieur |
| Gouvernance UC | Native | Native | Native | Native si on écrit dans UC |

---

## Les quatre critères du guide, et ce qu'ils tranchent réellement

**Volume.** Ce n'est pas le volume de données qui décide, c'est le **nombre de fichiers**.
`COPY INTO` liste le répertoire à chaque exécution ; Auto Loader tient un état
incrémental. Le point de bascule se situe vers quelques milliers de fichiers.

**Fréquence.** Toutes les minutes ou en continu → Auto Loader. Une fois par jour sur un
export → `COPY INTO` suffit et se lit en trois lignes de SQL.

**Types de données.** Semi-structuré et imbriqué : les deux savent faire. La différence
est dans le **rattrapage** : la colonne de sauvetage et les modes d'évolution de schéma
d'Auto Loader sont plus riches.

**Gouvernance.** Le vrai discriminant, et le plus souvent négligé. Un connecteur managé
apporte la traçabilité de bout en bout et n'a aucun secret à gérer côté client. Un script
JDDC maison, c'est une chaîne de connexion, un compte de service, une rotation de mot de
passe, et personne pour s'en occuper dans deux ans.

---

## Les trois pièges d'examen

**1. « Auto Loader est toujours le meilleur choix. »** Faux. Sur un chargement unique de
douze fichiers, `COPY INTO` est plus simple, plus lisible et sans état à gérer.

**2. Confondre `COPY INTO` et `INSERT INTO ... SELECT`.** `COPY INTO` est **idempotent** :
il mémorise les fichiers déjà chargés dans l'historique de la table cible et ne les
recharge pas. Un `INSERT INTO ... SELECT * FROM csv...` relancé duplique tout.

**3. Croire que le déclencheur d'arrivée de fichier est de l'événementiel instantané.**
Il interroge l'emplacement périodiquement. Il y a un délai, et il faut le mesurer avant
de promettre quoi que ce soit.

---

## Déclencheurs : temporel ou piloté par la donnée

| | Temporel | Arrivée de fichier / mise à jour de table |
|---|---|---|
| Prévisible | Oui — on sait quand ça tourne | Non |
| Traite du vide | Oui, et ça consomme du compute pour rien | Non |
| Latence si la donnée arrive juste après | Une période entière | Le délai de détection |
| Rafales | Une exécution, tout est traité ensemble | Risque d'exécutions en cascade |
| Coordination entre jobs | Fragile : on cale sur l'horloge et on espère | Native avec la mise à jour de table |

**Temporel** quand la source est régulière et que le retard est acceptable — le job de
nuit. **Piloté par la donnée** quand les dépôts sont irréguliers, ou pour chaîner deux
jobs sans se coordonner sur l'horloge.

Le cas où le pilotage par la donnée se retourne contre toi : un partenaire qui dépose
200 fichiers d'un coup peut déclencher 200 exécutions. Le nombre maximal d'exécutions
concurrentes du job est le garde-fou — et en Free Edition, il vaut mieux le régler à 1.

---

## QCM associés

`exam/qcm-section-2.md` et `exam/qcm-section-4.md`.
