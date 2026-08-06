# Databricks notebook source
# MAGIC %md
# MAGIC # Corrigé — M10 : gouvernance et sécurité

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "novamarket"
PII_COLUMNS = ["first_name", "last_name", "email", "zip_code"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO A à C — la hiérarchie des privilèges
# MAGIC
# MAGIC L'ordre n'est pas décoratif : `SELECT` sur un schéma ne sert à rien sans `USE
# MAGIC SCHEMA`, qui ne sert à rien sans `USE CATALOG`. C'est la cause n°1 des « on m'a
# MAGIC donné les droits mais je ne vois pas la table ».

# COMMAND ----------

spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `account users`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.gold TO `account users`")
spark.sql(f"GRANT SELECT ON SCHEMA {CATALOG}.gold TO `account users`")

display(spark.sql(f"SHOW GRANTS ON SCHEMA {CATALOG}.gold"))

# COMMAND ----------

spark.sql(f"REVOKE SELECT ON SCHEMA {CATALOG}.gold FROM `account users`")

# MAGIC %md
# MAGIC `USE SCHEMA` est toujours là : révoquer un privilège ne touche pas les autres.
# MAGIC On peut donc « traverser » un schéma sans pouvoir lire ses tables — ce qui est
# MAGIC exactement l'intérêt de séparer les deux.

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON SCHEMA {CATALOG}.gold"))

# COMMAND ----------

spark.sql(f"DENY SELECT ON TABLE {CATALOG}.silver.customer_scd2 TO `account users`")

# Et pourtant :
print(spark.table(f"{CATALOG}.silver.customer_scd2").count(), "lignes toujours lisibles")

# MAGIC %md
# MAGIC Tu es **propriétaire** de la table. Le propriétaire n'est pas soumis aux `DENY`
# MAGIC posés sur ses propres objets — sinon il pourrait se verrouiller dehors sans recours.
# MAGIC Le `DENY` s'appliquera aux autres membres de `account users`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO D — `ops.privilege_audit`

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.ops.privilege_audit (
        captured_at    TIMESTAMP COMMENT 'Instant de la capture',
        securable_type STRING    COMMENT 'CATALOG, SCHEMA ou TABLE',
        securable_name STRING    COMMENT 'Nom complet de l objet',
        principal      STRING    COMMENT 'Utilisateur, groupe ou principal de service',
        privilege      STRING    COMMENT 'Privilege accorde ou refuse',
        action_type    STRING    COMMENT 'GRANT ou DENY'
    )
    COMMENT 'Photographie des privileges. SHOW GRANTS ne se conserve pas tout seul.'
""")


def capture(securable_type, securable_name, action_type="GRANT"):
    """SHOW GRANTS n'a pas exactement les memes colonnes selon les versions :
    on se repere par position plutot que par nom."""
    df = spark.sql(f"SHOW GRANTS ON {securable_type} {securable_name}")
    cols = df.columns
    return df.select(
        F.current_timestamp().alias("captured_at"),
        F.lit(securable_type).alias("securable_type"),
        F.lit(securable_name).alias("securable_name"),
        F.col(cols[0]).alias("principal"),
        F.col(cols[1]).alias("privilege"),
        F.lit(action_type).alias("action_type"),
    )


snapshot = (capture("CATALOG", CATALOG)
            .unionByName(capture("SCHEMA", f"{CATALOG}.gold"))
            .unionByName(capture("TABLE", f"{CATALOG}.silver.customer_scd2")))

# Selon la version, SHOW GRANTS ne distingue pas toujours le DENY : on le trace
# explicitement plutot que de faire confiance a la sortie.
deny_row = spark.createDataFrame(
    [(None, "TABLE", f"{CATALOG}.silver.customer_scd2", "account users", "SELECT", "DENY")],
    "captured_at timestamp, securable_type string, securable_name string, "
    "principal string, privilege string, action_type string",
).withColumn("captured_at", F.current_timestamp())

(snapshot.unionByName(deny_row)
         .write.mode("overwrite").option("overwriteSchema", "true")
         .saveAsTable(f"{CATALOG}.ops.privilege_audit"))

display(spark.table(f"{CATALOG}.ops.privilege_audit"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO E — la table de pilotage
# MAGIC
# MAGIC Le contrôle d'accès se **donne**, il ne se code pas. Cette table se modifie sans
# MAGIC redéployer une seule fonction.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.ops.access_policy (
        principal       STRING    COMMENT 'Resultat de current_user()',
        can_see_pii     BOOLEAN   COMMENT 'Droit de lire les donnees personnelles en clair',
        allowed_country STRING    COMMENT 'Pays autorise ; NULL = tous',
        updated_at      TIMESTAMP
    )
    COMMENT 'Pilotage des masques et filtres. Un principal absent ne voit rien : ferme par defaut.'
""")

spark.sql(f"DELETE FROM {CATALOG}.ops.access_policy WHERE principal = current_user()")
spark.sql(f"""
    INSERT INTO {CATALOG}.ops.access_policy
    VALUES (current_user(), false, 'FR', current_timestamp())
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO F — la fonction de masquage
# MAGIC
# MAGIC Le `coalesce(..., false)` **est** la fermeture par défaut, et c'est la ligne la
# MAGIC plus importante du module.
# MAGIC
# MAGIC Sans lui : une sous-requête qui ne trouve aucune ligne renvoie `NULL`,
# MAGIC `CASE WHEN NULL` n'est pas vrai, donc on tombe dans le `ELSE`… ce qui marche ici
# MAGIC par chance. Mais écris la condition dans l'autre sens — `WHEN NOT can_see_pii THEN
# MAGIC '***' ELSE value` — et un principal inconnu voit **tout en clair**. La sécurité ne
# MAGIC doit pas dépendre du sens dans lequel on a écrit un `CASE`.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.gold.mask_pii(value STRING)
    RETURN CASE
        WHEN coalesce(
                (SELECT can_see_pii FROM {CATALOG}.ops.access_policy
                 WHERE principal = current_user()),
                false)
        THEN value
        ELSE '***'
    END
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO G — poser le masque
# MAGIC
# MAGIC Sur `gold.dim_customer`, pas sur `silver.customer_scd2` : cette dernière est
# MAGIC alimentée par un `MERGE` en M4, et un `MERGE` ne supporte pas les tables portant
# MAGIC certaines politiques de masquage ou de filtrage. Le masque casserait
# MAGIC l'historisation au prochain passage du job.
# MAGIC
# MAGIC Noter aussi que `customer_id` n'est **pas** masqué : c'est une clé de jointure.
# MAGIC La masquer rendrait la dimension inutilisable sans rien protéger de plus — un
# MAGIC identifiant technique n'est pas une donnée personnelle.

# COMMAND ----------

for column in PII_COLUMNS:
    spark.sql(f"ALTER TABLE {CATALOG}.gold.dim_customer "
              f"ALTER COLUMN {column} SET MASK {CATALOG}.gold.mask_pii")

display(spark.sql(f"""
    SELECT table_name, column_name, mask_name
    FROM {CATALOG}.information_schema.column_masks
    WHERE table_schema = 'gold'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO H — vérifier **les deux** sens

# COMMAND ----------


def peek(label):
    rows = spark.sql(f"SELECT customer_id, first_name, email, zip_code "
                     f"FROM {CATALOG}.gold.dim_customer LIMIT 3").collect()
    print(f"{label:28s} " + " | ".join(f"{r['first_name']}/{r['email']}" for r in rows))


spark.sql(f"UPDATE {CATALOG}.ops.access_policy SET can_see_pii = false "
          f"WHERE principal = current_user()")
peek("masque actif")

spark.sql(f"UPDATE {CATALOG}.ops.access_policy SET can_see_pii = true "
          f"WHERE principal = current_user()")
peek("masque leve")

spark.sql(f"DELETE FROM {CATALOG}.ops.access_policy WHERE principal = current_user()")
peek("principal inconnu")

spark.sql(f"INSERT INTO {CATALOG}.ops.access_policy "
          f"VALUES (current_user(), true, NULL, current_timestamp())")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO I — le filtre de lignes
# MAGIC
# MAGIC `EXISTS` plutôt qu'une comparaison directe : si le principal est absent, `EXISTS`
# MAGIC renvoie `false` et non `NULL`. Même logique de fermeture par défaut que le masque,
# MAGIC obtenue cette fois par la structure de la requête.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.gold.filter_by_country(country STRING)
    RETURN EXISTS (
        SELECT 1 FROM {CATALOG}.ops.access_policy
        WHERE principal = current_user()
          AND (allowed_country IS NULL OR allowed_country = country)
    )
""")

spark.sql(f"""
    ALTER TABLE {CATALOG}.gold.fact_order_line
    SET ROW FILTER {CATALOG}.gold.filter_by_country ON (shipping_country)
""")

# COMMAND ----------

# TODO J — les deux comportements

for country in ["FR", None]:
    value = f"'{country}'" if country else "NULL"
    spark.sql(f"UPDATE {CATALOG}.ops.access_policy SET allowed_country = {value} "
              f"WHERE principal = current_user()")
    df = spark.table(f"{CATALOG}.gold.fact_order_line")
    pays = sorted(r[0] for r in df.select("shipping_country").distinct().collect())
    print(f"allowed_country = {value:6s} -> {df.count():>7,} lignes, pays : {pays}"
          .replace(",", " "))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO K — ABAC
# MAGIC
# MAGIC ⚠️ **Syntaxe indicative, à vérifier sur docs.databricks.com** : cette
# MAGIC fonctionnalité est récente et sa disponibilité varie. Si elle n'est pas là, le
# MAGIC grader passe le critère en avertissement — mais l'objectif reste au programme, donc
# MAGIC le sujet est probable en QCM.
# MAGIC
# MAGIC L'intérêt par rapport aux quatre `ALTER TABLE` du TODO G : la politique s'applique
# MAGIC à toute colonne étiquetée `pii` du schéma, **y compris dans les tables créées
# MAGIC demain**. On passe d'un travail par colonne à un travail par règle.

# COMMAND ----------

try:
    spark.sql(f"""
        CREATE OR REPLACE POLICY mask_pii_by_tag
        ON SCHEMA {CATALOG}.gold
        COLUMN MASK {CATALOG}.gold.mask_pii
        TO `account users`
        FOR TABLES
        MATCH COLUMNS hasTagValue('pii', 'true') AS col
        ON COLUMN col
    """)
    display(spark.sql(f"SHOW POLICIES ON SCHEMA {CATALOG}.gold"))
except Exception as exc:
    print(f"ABAC indisponible sur ce workspace : {type(exc).__name__}")
    print("Voir la fiche de decision et les QCM de la section 7.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remise en état

# COMMAND ----------

spark.sql(f"""
    UPDATE {CATALOG}.ops.access_policy
    SET can_see_pii = true, allowed_country = NULL, updated_at = current_timestamp()
    WHERE principal = current_user()
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Réponses
# MAGIC
# MAGIC **1. Masque, vue filtrée, table dupliquée anonymisée**
# MAGIC
# MAGIC > | | Masque de colonne | Vue filtrée | Table dupliquée |
# MAGIC > |---|---|---|---|
# MAGIC > | Stockage | 1× | 1× | **2×** |
# MAGIC > | Maintenance | 1 fonction, 1 table de pilotage | 1 vue par population | 1 pipeline de plus |
# MAGIC > | Risque de divergence | **nul** — une seule donnée | nul | **élevé** — deux copies qui vivent leur vie |
# MAGIC > | Granularité | colonne, par principal, dynamique | ce que la vue expose | ce qu'on a décidé au moment de la copie |
# MAGIC > | Contournable ? | non, appliqué au moteur | **oui, si la table source est lisible** | oui, idem |
# MAGIC >
# MAGIC > La vue filtrée reste utile quand on veut exposer une **forme** différente, pas
# MAGIC > seulement cacher des valeurs. Mais elle ne protège rien si la table sous-jacente
# MAGIC > est accessible — et on ne peut poser ni masque ni filtre sur une vue, donc on ne
# MAGIC > peut pas cumuler les deux approches.
# MAGIC >
# MAGIC > La table dupliquée ne se justifie que si la population « anonymisée » a besoin
# MAGIC > d'une infrastructure séparée, par exemple un partage externe.
# MAGIC
# MAGIC **2. Coût d'une fonction qui interroge une table à chaque ligne**
# MAGIC
# MAGIC > Bien moindre qu'il n'y paraît : la sous-requête ne dépend d'aucune colonne de la
# MAGIC > table masquée, seulement de `current_user()`. Le moteur l'évalue une fois et
# MAGIC > diffuse le résultat — c'est un scalaire, pas une jointure ligne à ligne.
# MAGIC >
# MAGIC > Ce qui coûte vraiment : la table masquée devient **non éligible à certaines
# MAGIC > optimisations**, et surtout au `MERGE` dans les cas listés par la documentation.
# MAGIC > Le coût est fonctionnel, pas calculatoire.
# MAGIC >
# MAGIC > Le seuil où ça devient un problème n'est donc pas un volume de lignes, mais le
# MAGIC > moment où l'on veut écrire dans la table. C'est exactement pour ça que le masque
# MAGIC > est posé sur `gold.dim_customer`, qui est reconstruite en `overwrite`, et pas sur
# MAGIC > `silver.customer_scd2`, qui est fusionnée.
# MAGIC
# MAGIC **3. Qui peut modifier `ops.access_policy` ?**
# MAGIC
# MAGIC > Dans l'état actuel : tous ceux qui ont `MODIFY` sur le schéma `ops`. Et comme
# MAGIC > `ops` contient aussi les journaux de pipeline et les tables de quarantaine, à peu
# MAGIC > près toute l'équipe data. **On a construit une porte blindée avec la clé sur la
# MAGIC > serrure** : n'importe qui peut s'accorder `can_see_pii = true`.
# MAGIC >
# MAGIC > La correction : sortir la table du schéma courant, dans un `security` dédié dont
# MAGIC > le propriétaire n'est pas l'équipe data, avec `SELECT` accordé aux fonctions de
# MAGIC > masquage et `MODIFY` réservé aux administrateurs. Et surtout : **journaliser les
# MAGIC > modifications**. Une politique de sécurité qui change sans laisser de trace n'est
# MAGIC > pas une politique de sécurité.
# MAGIC >
# MAGIC > C'est le défaut le plus courant des implémentations de masquage que j'ai vues :
# MAGIC > le mécanisme est correct, et sa table de pilotage est en libre-service.
# MAGIC
# MAGIC **4. À quoi sert un `DENY` puisqu'il ne t'arrête pas ?**
# MAGIC
# MAGIC > Il ne t'arrête pas parce que tu es propriétaire — un propriétaire ne peut pas se
# MAGIC > verrouiller dehors. Pour tout autre principal, le `DENY` est utile dans trois
# MAGIC > situations :
# MAGIC >
# MAGIC > - **Percer un trou dans un octroi large.** `GRANT SELECT` sur tout le schéma, puis
# MAGIC >   `DENY` sur les deux tables sensibles. Sans `DENY`, il faudrait accorder table par
# MAGIC >   table et maintenir la liste à chaque création.
# MAGIC > - **Neutraliser un héritage.** Un privilège vient du catalog et on veut l'annuler
# MAGIC >   sur un objet précis : `REVOKE` ne peut pas retirer ce qui n'a pas été accordé à
# MAGIC >   ce niveau, `DENY` si.
# MAGIC > - **Exprimer une interdiction explicite.** « Ce groupe ne doit pas y accéder » se
# MAGIC >   lit dans `SHOW GRANTS` ; « ce groupe n'a pas reçu l'accès » ne se lit nulle part.
# MAGIC >   La différence compte le jour de l'audit.
# MAGIC >
# MAGIC > Règle à retenir : **le `DENY` gagne toujours sur le `GRANT`**, quel que soit le
# MAGIC > niveau où chacun est posé.
# MAGIC
# MAGIC **5. La même adresse est-elle lisible ailleurs ?**
# MAGIC
# MAGIC > **Oui, et c'est le vrai enseignement du module.**
# MAGIC >
# MAGIC > ```sql
# MAGIC > SELECT 'silver.customer_scd2' AS t, email FROM novamarket.silver.customer_scd2 LIMIT 3
# MAGIC > UNION ALL SELECT 'bronze.app_customers_raw', email FROM novamarket.bronze.app_customers_raw LIMIT 3
# MAGIC > ```
# MAGIC >
# MAGIC > Les deux renvoient des adresses en clair. Le masque de `gold.dim_customer` ne
# MAGIC > protège **rien du tout** tant que les couches amont sont lisibles : il suffit de
# MAGIC > remonter d'un cran. Et le fichier source dans le volume de landing est lisible lui
# MAGIC > aussi, avec un simple `SELECT * FROM csv.`/Volumes/...`` .
# MAGIC >
# MAGIC > La conclusion n'est pas « il faut masquer partout » — on a vu que c'est
# MAGIC > impossible sur `customer_scd2` à cause du `MERGE`. C'est que **le masquage n'est
# MAGIC > pas une politique de sécurité, c'est un mécanisme**. La politique, c'est de
# MAGIC > décider qui a accès à quel schéma, et le masquage ne sert qu'à affiner à
# MAGIC > l'intérieur de ce qui est déjà autorisé.
# MAGIC >
# MAGIC > En pratique : `bronze` et `silver` ne sont accessibles qu'à l'équipe data ; `gold`
# MAGIC > est ouvert aux analystes, et c'est là que le masque a un sens. Un masque posé sur
# MAGIC > une couche dont l'amont est ouvert donne une illusion de conformité, ce qui est
# MAGIC > pire que pas de masque du tout.
