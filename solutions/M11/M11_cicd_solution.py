# Databricks notebook source
# MAGIC %md
# MAGIC # Corrigé — M11 : CI/CD
# MAGIC
# MAGIC Le bundle est dans `solutions/M11/databricks.yml` et `solutions/M11/resources/`.
# MAGIC Ce notebook contient le code de la tâche de fumée et les réponses.
# MAGIC
# MAGIC Pour le job quotidien de M8 : reprends `solutions/M8/novamarket_daily.job.yml`
# MAGIC et remplace les chemins en dur par `${var.notebook_root}` et le catalog par
# MAGIC `${var.catalog}`. C'est le seul changement nécessaire pour qu'il devienne une
# MAGIC ressource de bundle — et c'est bien le signe que le YAML de job et le YAML de
# MAGIC bundle sont la même grammaire.

# COMMAND ----------

from datetime import datetime

dbutils.widgets.text("catalog", "novamarket", "Catalog")
dbutils.widgets.text("bundle_name", "local", "Nom du bundle")
dbutils.widgets.text("bundle_target", "manual", "Target du bundle")
dbutils.widgets.text("job_name", "manual", "Nom du job")

CATALOG = dbutils.widgets.get("catalog")
BUNDLE_NAME = dbutils.widgets.get("bundle_name")
BUNDLE_TARGET = dbutils.widgets.get("bundle_target")
JOB_NAME = dbutils.widgets.get("job_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO A — la branche Git
# MAGIC
# MAGIC Le contexte d'exécution la connaît quand le notebook vit dans un Git Folder.
# MAGIC Hors Git Folder, l'information n'existe pas : le `try/except` n'est pas de la
# MAGIC prudence décorative, c'est le cas nominal en exécution manuelle.

# COMMAND ----------


def current_git_branch() -> str:
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        tags = {str(k): str(v) for k, v in ctx.tags().toString.__self__.items()} \
            if hasattr(ctx.tags(), "toString") else {}
        for key in ("gitBranch", "branch", "notebookGitBranch"):
            if tags.get(key):
                return tags[key]
        # Repli : l'API de contexte varie selon les versions du runtime.
        return ctx.notebookPath().get().split("/")[2] if ctx.notebookPath().isDefined() \
            else "unknown"
    except Exception:
        return "unknown"


GIT_BRANCH = current_git_branch()
print(f"branche : {GIT_BRANCH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO B et C — la trace de déploiement

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ops")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.ops.deployment_log (
        deployed_at   TIMESTAMP COMMENT 'Instant de l execution',
        bundle_name   STRING    COMMENT 'Nom du bundle deploye',
        bundle_target STRING    COMMENT 'Target : dev ou prod',
        job_name      STRING    COMMENT 'Nom du job tel que deploye',
        catalog_used  STRING    COMMENT 'Catalog reellement utilise',
        git_branch    STRING    COMMENT 'Branche Git au moment du deploiement'
    )
    COMMENT 'Trace verifiable des deploiements. Rend la promotion d environnement
             constatable depuis les donnees plutot qu affirmee.'
""")

spark.createDataFrame(
    [(datetime.now(), BUNDLE_NAME, BUNDLE_TARGET, JOB_NAME, CATALOG, GIT_BRANCH)],
    "deployed_at timestamp, bundle_name string, bundle_target string, "
    "job_name string, catalog_used string, git_branch string",
).write.mode("append").saveAsTable(f"{CATALOG}.ops.deployment_log")

display(spark.table(f"{CATALOG}.ops.deployment_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Réponses
# MAGIC
# MAGIC **1. Ce que résolvent le préfixage et la suspension des planifications**
# MAGIC
# MAGIC > Deux problèmes distincts, et c'est important de ne pas les confondre.
# MAGIC >
# MAGIC > **Le préfixage `[dev <utilisateur>]`** résout la collision : trois développeurs
# MAGIC > qui déploient le même bundle dans le même workspace obtiennent trois jeux de
# MAGIC > ressources qui ne s'écrasent pas. Sans lui, le dernier `deploy` gagne, et on
# MAGIC > débogue le job d'un collègue en croyant déboguer le sien. C'est exactement ce qui
# MAGIC > rend la Free Edition utilisable pour cet exercice malgré son workspace unique.
# MAGIC >
# MAGIC > **La suspension des planifications** résout la duplication d'exécution : sans
# MAGIC > elle, chaque déploiement de développement ajouterait un job qui tourne toutes les
# MAGIC > nuits sur les mêmes données. Trois développeurs, trois écritures concurrentes sur
# MAGIC > les tables de production, et personne ne comprend pourquoi les compteurs sont
# MAGIC > faux. Sur Free Edition, le symptôme serait plus brutal : le quota consommé pour
# MAGIC > rien.
# MAGIC >
# MAGIC > Le point commun : ce sont deux garde-fous contre le fait que **le mode de
# MAGIC > déploiement par défaut est `development`**. On n'envoie pas en production par
# MAGIC > accident.
# MAGIC
# MAGIC **2. Ce que deux targets sur un workspace ne testent pas**
# MAGIC
# MAGIC > Beaucoup de choses, et il faut les nommer plutôt que de faire semblant :
# MAGIC >
# MAGIC > - **L'isolation des permissions.** En vrai dev/prod, un développeur n'a pas
# MAGIC >   `MODIFY` sur les tables de production. Ici, si — j'ai les droits partout, donc
# MAGIC >   une erreur de target ne serait arrêtée par rien.
# MAGIC > - **La séparation des identités.** En production, le job devrait tourner sous un
# MAGIC >   principal de service, pas sous mon compte. Le comportement d'un principal de
# MAGIC >   service diffère : il n'hérite pas de mes droits personnels, et c'est
# MAGIC >   précisément là que les déploiements cassent.
# MAGIC > - **Les quotas et le dimensionnement.** Un environnement de production a d'autres
# MAGIC >   limites de compute ; on ne découvre certains problèmes que là-bas.
# MAGIC > - **La configuration réseau et les secrets**, qui diffèrent normalement par
# MAGIC >   environnement.
# MAGIC >
# MAGIC > Ce que ça teste quand même, et qui est l'essentiel pour l'examen : **le mécanisme
# MAGIC > de résolution des variables et de surcharge par target**. Le YAML se comporte de
# MAGIC > la même façon avec deux workspaces.
# MAGIC
# MAGIC **3. Les secrets**
# MAGIC
# MAGIC > Jamais dans le YAML, et jamais dans une variable de bundle — les deux sont
# MAGIC > versionnés en Git, donc lisibles par toute personne ayant accès au dépôt, et
# MAGIC > lisibles pour toujours grâce à l'historique.
# MAGIC >
# MAGIC > La chaîne complète :
# MAGIC >
# MAGIC > 1. Le secret vit dans un **scope de secrets** Databricks, ou mieux, dans le
# MAGIC >    coffre du fournisseur cloud auquel le scope est adossé.
# MAGIC > 2. Le YAML référence le scope et la clé, jamais la valeur :
# MAGIC >    `{{secrets/mon_scope/ma_cle}}`.
# MAGIC > 3. Le job résout la référence à l'exécution ; la valeur n'apparaît ni dans la
# MAGIC >    configuration, ni dans les journaux — Databricks les rédige activement.
# MAGIC > 4. Les droits sur le scope se gèrent comme le reste : le principal de service de
# MAGIC >    production y a accès, pas les développeurs.
# MAGIC >
# MAGIC > Ce que le bundle peut légitimement contenir : **le nom** du scope et **le nom** de
# MAGIC > la clé, qui changent d'un environnement à l'autre et sont donc de bonnes variables
# MAGIC > de target.
# MAGIC
# MAGIC **4. `deploy` est-il idempotent ?**
# MAGIC
# MAGIC > Oui, et c'est sa propriété centrale. Le bundle décrit un **état souhaité**, pas
# MAGIC > une suite d'actions. Deux `deploy` consécutifs sans changement laissent le
# MAGIC > workspace identique : rien n'est recréé, aucune version n'est empilée.
# MAGIC >
# MAGIC > Si tu **supprimes une ressource du YAML**, le déploiement suivant la supprime du
# MAGIC > workspace. C'est ce qui surprend, et c'est voulu : le YAML fait autorité. Le
# MAGIC > corollaire pratique est qu'un job créé à la main dans l'interface et non déclaré
# MAGIC > dans le bundle **n'est pas supprimé** — le bundle ne gère que ce qu'il a déployé,
# MAGIC > identifié par son état interne.
# MAGIC >
# MAGIC > C'est la même logique que le silver idempotent de M3 : l'état est défini par la
# MAGIC > cible, pas par l'historique des opérations. Et ça produit le même bénéfice —
# MAGIC > rejouer est sans danger, donc réparer consiste à relancer.
# MAGIC
# MAGIC **5. Ce que le dépôt ne devrait pas contenir**
# MAGIC
# MAGIC > **`data/`**, soit 47 Mo de fichiers générés — et c'est le plus gros défaut de
# MAGIC > l'état actuel du projet.
# MAGIC >
# MAGIC > Trois raisons, par ordre d'importance :
# MAGIC >
# MAGIC > - Ces fichiers sont **reproductibles** : `python generator/generate.py` les
# MAGIC >   régénère à l'identique, puisque le générateur est déterministe. Versionner un
# MAGIC >   artefact qu'on sait reconstruire, c'est versionner du bruit.
# MAGIC > - Git stocke mal les binaires : chaque régénération réécrit 47 Mo dans
# MAGIC >   l'historique, définitivement.
# MAGIC > - Question de principe : **un dépôt de code ne contient pas de données**. Ici
# MAGIC >   elles sont synthétiques, donc inoffensives. Le jour où quelqu'un applique la
# MAGIC >   même habitude à un extrait de production, l'incident est d'une autre nature.
# MAGIC >
# MAGIC > Deux autres candidats : `graders/expected/*.json`, qui sont eux aussi des
# MAGIC > artefacts calculés — mais ils sont petits, lisibles en diff, et servent de
# MAGIC > référence, donc les garder se défend ; et `.databricks/`, l'état local du bundle,
# MAGIC > qui n'a rien à faire dans Git.
# MAGIC >
# MAGIC > D'où le `.gitignore` à la racine du projet.
