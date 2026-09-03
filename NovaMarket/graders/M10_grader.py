# Databricks notebook source
# MAGIC %md
# MAGIC # Grader — M10 : gouvernance et sécurité
# MAGIC
# MAGIC ⚠️ Ce grader **modifie `ops.access_policy`** : c'est le seul moyen de vérifier
# MAGIC qu'une politique fonctionne plutôt que de constater qu'elle est déclarée. Il
# MAGIC remet l'état permissif à la fin, y compris si un critère échoue.
# MAGIC
# MAGIC Les critères sont comportementaux : aucun ne dépend d'un comptage, donc ce grader
# MAGIC reste valable quelle que soit la vague de données ingérée.

# COMMAND ----------

# MAGIC %run ./_grader_lib

# COMMAND ----------

dbutils.widgets.text("catalog", "novamarket", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F

g = Grader(f"M10 — gouvernance et sécurité ({CATALOG})")

ME = spark.sql("SELECT current_user()").first()[0]

POLICY_SCHEMA = [
    ("principal", "string"), ("can_see_pii", "boolean"),
    ("allowed_country", "string"), ("updated_at", "timestamp"),
]
AUDIT_SCHEMA = [
    ("captured_at", "timestamp"), ("securable_type", "string"),
    ("securable_name", "string"), ("principal", "string"),
    ("privilege", "string"), ("action_type", "string"),
]
PII_COLUMNS = ["first_name", "last_name", "email", "zip_code"]

# COMMAND ----------


def set_policy(can_see_pii=None, allowed_country="__KEEP__", present=True):
    """Positionne la politique de l'utilisateur courant, puis renvoie."""
    spark.sql(f"DELETE FROM {CATALOG}.ops.access_policy WHERE principal = current_user()")
    if not present:
        return
    country = "NULL" if allowed_country in (None, "__KEEP__") else f"'{allowed_country}'"
    spark.sql(f"""
        INSERT INTO {CATALOG}.ops.access_policy
        VALUES (current_user(), {str(bool(can_see_pii)).lower()}, {country}, current_timestamp())
    """)


def sample_emails(n=200):
    rows = spark.sql(f"SELECT email FROM {CATALOG}.gold.dim_customer "
                     f"WHERE email IS NOT NULL LIMIT {n}").collect()
    return [r["email"] for r in rows]


def readable_emails():
    return sum(1 for e in sample_emails() if e and "@" in e)


def fact_countries():
    rows = spark.sql(f"SELECT DISTINCT shipping_country "
                     f"FROM {CATALOG}.gold.fact_order_line").collect()
    return {r["shipping_country"] for r in rows}


def fact_count():
    return spark.table(f"{CATALOG}.gold.fact_order_line").count()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Structure

# COMMAND ----------

g.equals("ops.access_policy : schéma exact",
         lambda: [(f.name, f.dataType.simpleString())
                  for f in spark.table(f"{CATALOG}.ops.access_policy").schema],
         POLICY_SCHEMA)
g.truthy("la fonction de masquage existe",
         lambda: spark.sql(f"SHOW USER FUNCTIONS IN {CATALOG}.gold LIKE 'mask_pii'").count() == 1)
g.truthy("la fonction de filtrage existe",
         lambda: spark.sql(f"SHOW USER FUNCTIONS IN {CATALOG}.gold "
                           f"LIKE 'filter_by_country'").count() == 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comportement du masquage
# MAGIC
# MAGIC Trois états, trois résultats attendus. Un masque testé dans un seul sens n'est pas
# MAGIC testé.

# COMMAND ----------

probes = {}
try:
    set_policy(can_see_pii=False, allowed_country=None)
    probes["masque_actif"] = readable_emails()

    set_policy(can_see_pii=True, allowed_country=None)
    probes["masque_leve"] = readable_emails()

    set_policy(present=False)
    probes["ferme_par_defaut"] = readable_emails()

    set_policy(can_see_pii=True, allowed_country=None)
    probes["pays_tous"] = fact_countries()
    probes["lignes_tous"] = fact_count()

    set_policy(can_see_pii=True, allowed_country="FR")
    probes["pays_fr"] = fact_countries()
    probes["lignes_fr"] = fact_count()
finally:
    # Remise en état inconditionnelle : un filtre laissé actif casserait les graders amont.
    set_policy(can_see_pii=True, allowed_country=None)

probes

# COMMAND ----------

g.equals("can_see_pii = false : aucun e-mail lisible",
         lambda: probes["masque_actif"], 0)
g.truthy("can_see_pii = true : e-mails lisibles",
         lambda: probes["masque_leve"] > 0,
         hint="> 0 adresse contenant @")
g.equals("fermeture par défaut : principal absent = rien en clair",
         lambda: probes["ferme_par_defaut"], 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comportement du filtrage de lignes

# COMMAND ----------

g.equals("allowed_country = 'FR' : uniquement des lignes FR",
         lambda: probes["pays_fr"], {"FR"})
g.truthy("allowed_country = 'FR' : strictement moins de lignes",
         lambda: probes["lignes_fr"] < probes["lignes_tous"],
         hint="le filtre coupe réellement")
g.truthy("allowed_country = NULL : plusieurs pays reviennent",
         lambda: len(probes["pays_tous"]) > 1,
         hint="NULL = tous les pays")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Où les politiques sont posées

# COMMAND ----------


def masked_columns(schema, table):
    rows = spark.sql(f"""
        SELECT column_name FROM {CATALOG}.information_schema.column_masks
        WHERE table_schema = '{schema}' AND table_name = '{table}'
    """).collect()
    return {r["column_name"] for r in rows}


def has_row_filter(schema, table):
    return spark.sql(f"""
        SELECT count(*) FROM {CATALOG}.information_schema.row_filters
        WHERE table_schema = '{schema}' AND table_name = '{table}'
    """).first()[0] > 0


g.soft("les 4 colonnes PII de gold.dim_customer sont masquées",
       lambda: set(PII_COLUMNS).issubset(masked_columns("gold", "dim_customer")),
       hint="first_name, last_name, email, zip_code")
g.soft("gold.fact_order_line porte un filtre de lignes",
       lambda: has_row_filter("gold", "fact_order_line"))
g.soft("aucun masque sur silver.customer_scd2 (elle est alimentée par MERGE)",
       lambda: len(masked_columns("silver", "customer_scd2")) == 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Privilèges

# COMMAND ----------

g.equals("ops.privilege_audit : schéma exact",
         lambda: [(f.name, f.dataType.simpleString())
                  for f in spark.table(f"{CATALOG}.ops.privilege_audit").schema],
         AUDIT_SCHEMA)
g.truthy("au moins 3 privilèges capturés",
         lambda: spark.table(f"{CATALOG}.ops.privilege_audit").count() >= 3)
g.truthy("un DENY figure dans la capture",
         lambda: spark.table(f"{CATALOG}.ops.privilege_audit")
                      .filter("action_type = 'DENY'").count() >= 1)
g.truthy("les trois niveaux de la hiérarchie sont représentés",
         lambda: {"CATALOG", "SCHEMA", "TABLE"}.issubset(
             {r["securable_type"] for r in spark.table(f"{CATALOG}.ops.privilege_audit")
              .select("securable_type").distinct().collect()}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ABAC
# MAGIC
# MAGIC Avertissement et non échec : la disponibilité de la fonctionnalité varie selon les
# MAGIC workspaces. L'objectif reste au programme de l'examen — voir les QCM de la section 7.

# COMMAND ----------

g.soft("une politique ABAC est déclarée",
       lambda: spark.sql(f"SHOW POLICIES ON SCHEMA {CATALOG}.gold").count() >= 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remise en état

# COMMAND ----------

g.truthy("la politique est laissée en état permissif",
         lambda: spark.sql(f"""
             SELECT can_see_pii AND allowed_country IS NULL
             FROM {CATALOG}.ops.access_policy WHERE principal = current_user()
         """).first()[0] is True,
         hint="sinon les graders amont échoueront")

# COMMAND ----------

g.report()
