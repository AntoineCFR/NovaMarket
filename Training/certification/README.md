# Banque de QCM et examens blancs

Adossée au guide officiel du **4 mai 2026** : 45 questions, 90 minutes, 7 sections.

---

## Comment s'en servir

Le protocole complet est dans `docs/06-protocole-revision.md`. En résumé :

1. **Diagnostiquer d'abord.** Passe les sept fiches de section **avant** de réviser. Tu
   sauras où est ton temps le mieux investi. C'est l'erreur la plus coûteuse de
   repasser du temps sur ce qu'on maîtrise déjà.
2. **Approfondir** les objectifs ratés, avec le module correspondant et la documentation
   officielle.
3. **Endurance** en fin de parcours : les examens blancs, chronométrés, dans les
   conditions réelles.

Les corrigés sont dans un bloc dépliable en fin de chaque fiche. **Ne le déplie pas
avant d'avoir répondu à toutes les questions** — c'est la consigne du guide officiel, et
elle a une raison : répondre en sachant qu'on peut vérifier ne mesure rien.

---

## Contenu

| Fiche | Section | Poids | Questions |
|---|---|---|---|
| `qcm-section-1.md` | Plateforme Databricks | 6 % | 10 |
| `qcm-section-2.md` | Ingestion et chargement | 21 % | 12 |
| `qcm-section-3.md` | Transformation et modélisation | 22 % | 12 |
| `qcm-section-4.md` | Lakeflow Jobs | 16 % | 10 |
| `qcm-section-5.md` | CI/CD | 10 % | 12 |
| `qcm-section-6.md` | Diagnostic et optimisation | 10 % | 12 |
| `qcm-section-7.md` | Gouvernance et sécurité | 15 % | 12 |
| `mock-exam-1.md` | Toutes | — | 45 |
| `mock-exam-2.md` | Toutes | — | 45 |

Les examens blancs respectent la répartition officielle : 3 / 9 / 10 / 7 / 5 / 4 / 7.

---

## Un défaut de fabrication, et sa réparation

Dans la version initiale de ces fiches, la bonne réponse était en **B** dans 67 cas sur
80 — et dans 41 cas sur 45 pour chaque examen blanc. Répondre « B » sans lire rapportait
84 % aux fiches et 91 % aux blancs. Autrement dit, l'instrument mesurait la capacité à
repérer une habitude de rédaction, pas la connaissance.

Les **deux examens blancs ont été rebrassés** (`generator/shuffle_qcm.py`) : les options
sont redistribuées, la bonne réponse est désormais répartie 12 / 11 / 11 / 11 sur les
quatre positions, et jamais plus de deux fois de suite au même endroit. Le texte des
questions et des corrigés est inchangé.

Les **sept fiches de section gardent le biais**, volontairement : leurs réponses ont déjà
été consignées, et les rebrasser rendrait ce relevé illisible. Retiens simplement que le
score de diagnostic du 29 juillet est **surévalué**, pour la raison expliquée dans le
journal.

---

## Avertissement

Ces questions sont écrites à partir des objectifs du guide et de la documentation
Databricks. **Ce ne sont pas des questions d'examen**, et un score élevé ici ne garantit
rien — le guide officiel le dit lui-même à propos des examens blancs générés par IA.

Ton vrai signal de préparation, c'est la couverture complète des objectifs
(`docs/04-couverture-certification.md`) et l'aisance sur les dix gestes du protocole.

Deux réflexes à garder :

- **Vérifie tout ce qui te surprend** sur docs.databricks.com. Si un corrigé te paraît
  faux, il peut l'être : la plateforme évolue, et ces fiches ont une date.
- **Recoupe avec une seconde IA** sur les sujets clés. Un désaccord entre deux modèles
  est un signal, pas un détail.

---

## Journal

Consigne tes ratés dans `exam/journal.md` : date, objectif, ce que tu n'as pas su, où tu
as vérifié. C'est ce que tu reliras la veille — pas les corrigés.
