# Databricks notebook source
# MAGIC %md
# MAGIC # M10 — Gouvernance et sécurité
# MAGIC
# MAGIC Section 7 de l'examen · 15 % · la mieux payée du guide.
# MAGIC
# MAGIC Objectif : une seule table, et le résultat de la requête dépend de qui la pose.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

CATALOG = "novamarket"
ME = spark.sql("SELECT current_user()").first()[0]

print(f"utilisateur courant : {ME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. La hiérarchie des privilèges
# MAGIC
# MAGIC `metastore → catalog → schema → table`. Un privilège se propage vers le bas, mais
# MAGIC il faut `USE` sur **tous** les niveaux au-dessus pour atteindre un objet.
# MAGIC
# MAGIC Observe `SHOW GRANTS` après chaque opération — c'est en regardant ce qui change
# MAGIC qu'on comprend le modèle, pas en lisant la documentation.

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON CATALOG {CATALOG}"))

# COMMAND ----------

# TODO A — accorde, dans l'ordre :
#   USE CATALOG sur le catalog        -> `account users`
#   USE SCHEMA et SELECT sur gold     -> `account users`
# Affiche SHOW GRANTS après chaque étape et regarde ce qui apparaît.


# COMMAND ----------

# TODO B — retire le SELECT sur gold, puis observe ce qui reste.
# Question : `USE SCHEMA` a-t-il disparu aussi ?


# COMMAND ----------

# TODO C — refuse explicitement SELECT sur silver.customer_scd2 à `account users`.
# Puis essaie de lire la table. Que se passe-t-il, et pourquoi ?


# COMMAND ----------

# MAGIC %md
# MAGIC ### `ops.privilege_audit`
# MAGIC
# MAGIC `SHOW GRANTS` ne se conserve pas. Une photographie des privilèges est le premier
# MAGIC livrable qu'on demande en audit.

# COMMAND ----------

# TODO D — crée ops.privilege_audit (schéma dans le README) et capture l'état final :
# les privilèges du catalog, du schéma gold et de la table silver.customer_scd2.
#
# Piste : SHOW GRANTS renvoie un DataFrame. Tu peux aussi regarder
# novamarket.information_schema.table_privileges et schema_privileges.


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. La table de pilotage
# MAGIC
# MAGIC Le contrôle d'accès ne se code pas en dur dans une fonction : il se **donne** dans
# MAGIC une table, qu'on peut modifier sans redéployer quoi que ce soit.

# COMMAND ----------

# TODO E — crée novamarket.ops.access_policy
#   principal STRING, can_see_pii BOOLEAN, allowed_country STRING, updated_at TIMESTAMP
# et insère ta propre ligne, en commençant par le cas le plus restrictif.


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Masquage de colonnes
# MAGIC
# MAGIC Une fonction de masquage reçoit la valeur et renvoie ce que l'appelant a le droit
# MAGIC de voir. Elle s'applique **à la lecture** : la donnée en base ne bouge pas.
# MAGIC
# MAGIC Exigence non négociable : **fermeture par défaut**. Un principal absent de
# MAGIC `ops.access_policy` ne doit rien voir en clair. Demande-toi ce que renvoie une
# MAGIC sous-requête sans résultat, et ce qu'en fait un `CASE WHEN`.

# COMMAND ----------

# TODO F — crée la fonction gold.mask_pii(value STRING)
#
# spark.sql(f"""
#     CREATE OR REPLACE FUNCTION {CATALOG}.gold.mask_pii(value STRING)
#     RETURN ...
# """)


# COMMAND ----------

# MAGIC %md
# MAGIC ⚠️ **Avant de poser le masque** : sur quelle table ?
# MAGIC
# MAGIC `silver.customer_scd2` est la source de vérité, donc tentante. Elle est alimentée
# MAGIC par un `MERGE` en M4, et un `MERGE` ne supporte pas les tables portant certaines
# MAGIC politiques de masquage. Poser le masque là casserait ton historisation.
# MAGIC
# MAGIC Deuxième contrainte : on ne peut pas poser de masque sur une **vue**.

# COMMAND ----------

# TODO G — pose le masque sur les 4 colonnes PII de gold.dim_customer
# Syntaxe : ALTER TABLE ... ALTER COLUMN ... SET MASK ...


# COMMAND ----------

# MAGIC %md
# MAGIC ### Vérifie **les deux** comportements
# MAGIC
# MAGIC Un masque qu'on n'a testé que dans un sens n'est pas testé.

# COMMAND ----------

def show_customers(label):
    print(f"--- {label} ---")
    display(spark.sql(f"""
        SELECT customer_id, first_name, last_name, email, zip_code, country
        FROM {CATALOG}.gold.dim_customer LIMIT 5
    """))


# TODO H — bascule ops.access_policy et observe :
#   1. can_see_pii = false  -> tout doit être masqué
#   2. can_see_pii = true   -> tout doit être lisible
#   3. ta ligne supprimée   -> tout doit être masqué (fermeture par défaut)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Filtrage de lignes
# MAGIC
# MAGIC Même principe, appliqué aux lignes. La fonction reçoit la valeur d'une colonne et
# MAGIC renvoie un booléen.
# MAGIC
# MAGIC `allowed_country` à `NULL` doit signifier « tous les pays », sinon un
# MAGIC administrateur ne peut plus rien voir.

# COMMAND ----------

# TODO I — crée gold.filter_by_country(country STRING) puis pose le filtre sur
# gold.fact_order_line.
# Syntaxe : ALTER TABLE ... SET ROW FILTER ... ON (colonne)


# COMMAND ----------

# TODO J — vérifie les deux comportements :
#   allowed_country = 'FR'  -> uniquement FR, et strictement moins de lignes
#   allowed_country = NULL  -> tous les pays


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. ABAC — la même chose, en une fois
# MAGIC
# MAGIC Poser un masque colonne par colonne ne passe pas à l'échelle. Une politique ABAC
# MAGIC s'attache au catalog ou au schema et s'applique à toute colonne portant une
# MAGIC étiquette donnée — y compris aux tables créées demain.
# MAGIC
# MAGIC Tes étiquettes `pii` de M6 deviennent le critère. Vérifie qu'elles sont bien là :

# COMMAND ----------

display(spark.sql(f"""
    SELECT schema_name, table_name, column_name
    FROM {CATALOG}.information_schema.column_tags
    WHERE lower(tag_name) = 'pii'
    ORDER BY schema_name, table_name, column_name
"""))

# COMMAND ----------

# TODO K — déclare une politique ABAC de masquage sur les colonnes étiquetées pii.
# Si la fonctionnalité n'est pas disponible sur ton workspace, note-le et passe :
# le grader traite ce critère en avertissement.


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. ⚠️ Remise en état
# MAGIC
# MAGIC Le filtre de lignes s'applique à **toutes** les requêtes sur `fact_order_line`,
# MAGIC y compris celles de tes graders précédents. Laisse la politique en permissif.

# COMMAND ----------

spark.sql(f"""
    UPDATE {CATALOG}.ops.access_policy
    SET can_see_pii = true, allowed_country = NULL, updated_at = current_timestamp()
    WHERE principal = current_user()
""")

display(spark.table(f"{CATALOG}.ops.access_policy"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Tes réponses
# MAGIC
# MAGIC **1. Masque, vue filtrée, table dupliquée : comparaison sur 4 axes**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **2. Coût d'une fonction de masquage qui interroge une table à chaque ligne ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **3. Qui peut modifier `ops.access_policy` ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **4. À quoi sert un `DENY` puisqu'il ne t'arrête pas ?**
# MAGIC
# MAGIC > …
# MAGIC
# MAGIC **5. La même adresse e-mail est-elle lisible ailleurs dans le catalog ?**
# MAGIC
# MAGIC > …
