# Databricks notebook source
# MAGIC %md
# MAGIC # Corrigé — M8 : orchestration
# MAGIC
# MAGIC La définition du job est dans `solutions/M8/novamarket_daily.job.yml`.
# MAGIC Ce notebook contient les adaptations de code et les réponses.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

dbutils.widgets.text("catalog", "novamarket", "Catalog")
dbutils.widgets.text("run_id", "", "Run ID")
dbutils.widgets.text("job_name", "novamarket_daily", "Nom du job")

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = dbutils.widgets.get("run_id") or str(uuid.uuid4())
JOB_NAME = dbutils.widgets.get("job_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO A — `M1_bronze_ref.py` paramétrable
# MAGIC
# MAGIC Le YAML utilise un notebook de répartition (`M1_bronze_dispatch`) plutôt que trois
# MAGIC notebooks : le `for_each` itère sur `["orders", "events", "ref"]`, et le notebook
# MAGIC aiguille. C'est un compromis — voir la remarque du README sur l'usage discutable
# MAGIC de `for_each` sur une liste figée.
# MAGIC
# MAGIC Version pour le seul chargement des référentiels :

# COMMAND ----------

REF_TARGETS = {
    "categories": "ref_categories_raw",
    "sellers": "ref_sellers_raw",
    "products": "ref_products_raw",
}

# dbutils.widgets.text("ref_name", "products", "Referentiel")
# REF_NAME = dbutils.widgets.get("ref_name")
#
# n = load_ref(REF_NAME, REF_TARGETS[REF_NAME])
# log_run(CATALOG, task_name="bronze_ref", source_name=REF_NAME,
#         started_at=STARTED_AT, status="SUCCESS", rows_written=n, run_id=RUN_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC Point de vigilance : `source_name` doit porter le nom du référentiel, pas une
# MAGIC constante. Trois lignes identiques dans `ops.pipeline_runs` ne prouvent rien ; trois
# MAGIC lignes distinctes prouvent que les trois itérations ont bien tourné.

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO B — publication de la valeur de tâche
# MAGIC
# MAGIC À ajouter en fin de `M6_qualite.py`.
# MAGIC
# MAGIC La distinction `FAIL` / `WARN` faite en M6 sert exactement à ça : seuls les
# MAGIC invariants violés bloquent la publication. Un taux d'orphelins qui grimpe alerte
# MAGIC mais ne prive personne de ses données.

# COMMAND ----------

n_blocking = (spark.table(f"{CATALOG}.ops.dq_metrics")
              .filter((F.col("run_id") == RUN_ID) & (F.col("status") == "FAIL"))
              .count())
n_warnings = (spark.table(f"{CATALOG}.ops.dq_metrics")
              .filter((F.col("run_id") == RUN_ID) & (F.col("status") == "WARN"))
              .count())

dbutils.jobs.taskValues.set(key="dq_status", value="PASS" if n_blocking == 0 else "FAIL")
dbutils.jobs.taskValues.set(key="n_failures", value=int(n_blocking))
dbutils.jobs.taskValues.set(key="n_warnings", value=int(n_warnings))

print(f"dq_status = {'PASS' if n_blocking == 0 else 'FAIL'} "
      f"({n_blocking} bloquant(s), {n_warnings} avertissement(s))")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tâche finale — `ops.job_runs`

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.ops.job_runs (
        job_run_id STRING    COMMENT 'Identifiant d execution du job',
        job_name   STRING    COMMENT 'Nom du job',
        started_at TIMESTAMP COMMENT 'Debut de la premiere tache journalisee',
        ended_at   TIMESTAMP COMMENT 'Fin de la derniere tache journalisee',
        n_tasks    INT       COMMENT 'Nombre de taches distinctes ayant journalise',
        status     STRING    COMMENT 'SUCCESS si aucune tache en echec',
        notes      STRING    COMMENT 'Commentaire libre'
    )
    COMMENT 'Bilan par execution de job. Ne voit que les taches qui ont journalise :
             un job qui plante avant la tache finale n ecrit rien ici.'
""")

summary = (
    spark.table(f"{CATALOG}.ops.pipeline_runs")
    .filter(F.col("run_id") == RUN_ID)
    .agg(
        F.min("started_at").alias("started_at"),
        F.max("ended_at").alias("ended_at"),
        F.countDistinct("task_name").alias("n_tasks"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("n_failed"),
        F.sum("rows_written").alias("rows_written"),
    )
    .first()
)

(spark.createDataFrame(
    [(RUN_ID, JOB_NAME, summary["started_at"], summary["ended_at"],
      int(summary["n_tasks"]),
      "SUCCESS" if summary["n_failed"] == 0 else "FAILED",
      f"{summary['rows_written']} ligne(s) ecrite(s) au total")],
    "job_run_id string, job_name string, started_at timestamp, ended_at timestamp, "
    "n_tasks int, status string, notes string")
 .write.mode("append").saveAsTable(f"{CATALOG}.ops.job_runs"))

display(spark.table(f"{CATALOG}.ops.job_runs").orderBy(F.col("started_at").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## `M8_dq_failure` — la branche `false`
# MAGIC
# MAGIC Deux lignes, et elles sont indispensables. Sans exception levée, un job dont toutes
# MAGIC les tâches critiques ont été **skipped** se termine en `SUCCESS` : les notifications
# MAGIC d'échec ne partent pas, et le tableau de bord des jobs est tout vert.

# COMMAND ----------

# n_failures = dbutils.jobs.taskValues.get(taskKey="dq_checks", key="n_failures", default=0)
# raise Exception(f"Publication annulee : {n_failures} controle(s) bloquant(s) en echec.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Réponses
# MAGIC
# MAGIC **1. Pourquoi Auto Loader échoue-t-il au lieu d'ignorer les colonnes inconnues ?**
# MAGIC
# MAGIC > Parce qu'ignorer serait une perte de données silencieuse — exactement ce que la
# MAGIC > couche bronze est censée rendre impossible. En `addNewColumns`, Auto Loader
# MAGIC > détecte le champ inconnu, **écrit le nouveau schéma** dans son emplacement de
# MAGIC > schéma, puis lève `UnknownFieldException` et s'arrête. Le redémarrage repart du
# MAGIC > schéma mis à jour et traite le fichier. L'échec fait partie du protocole : c'est
# MAGIC > un mécanisme d'apprentissage, pas une panne.
# MAGIC >
# MAGIC > Les alternatives et ce qu'elles coûtent :
# MAGIC >
# MAGIC > | Mode | Comportement | Ce qu'on perd |
# MAGIC > |---|---|---|
# MAGIC > | `addNewColumns` (défaut) | échoue puis absorbe | rien, mais il faut un retry |
# MAGIC > | `rescue` | pas d'échec ; les nouvelles colonnes vont dans `_rescued_data` | des colonnes exploitables coincées dans un blob JSON |
# MAGIC > | `none` | pas d'échec ; les nouvelles colonnes sont **ignorées** | la donnée, définitivement |
# MAGIC > | `failOnNewColumns` | échoue et n'apprend rien | il faut intervenir à la main |
# MAGIC >
# MAGIC > `rescue` est le compromis raisonnable quand on ne peut pas se permettre l'échec :
# MAGIC > rien n'est perdu, et un contrôle sur le taux de sauvetage signale l'arrivée d'une
# MAGIC > colonne. `none` est à proscrire — c'est la seule option qui détruit de la donnée
# MAGIC > sans le dire.
# MAGIC
# MAGIC **2. Le job a rattrapé la dérive tout seul. Est-ce une bonne chose ?**
# MAGIC
# MAGIC > Pour la **disponibilité**, oui, sans hésitation : le pipeline a survécu à un
# MAGIC > changement amont non annoncé, à 4 h du matin, sans intervention.
# MAGIC >
# MAGIC > Pour la **gouvernance**, c'est plus embêtant. Le job est vert, la colonne est là,
# MAGIC > et personne n'a validé quoi que ce soit. Or « présent » ne veut pas dire
# MAGIC > « exploitable » :
# MAGIC >
# MAGIC > - je ne sais pas ce que `promo_code` signifie — code appliqué ? proposé ? éligible ?
# MAGIC > - je ne sais pas si `channel` est stable ou si ses valeurs changeront le mois prochain ;
# MAGIC > - je ne sais pas si la source considère cette livraison comme définitive ;
# MAGIC > - et surtout, `docs/02-sources-et-modele.md` ne les mentionne pas. Le contrat n'a
# MAGIC >   pas été mis à jour.
# MAGIC >
# MAGIC > Ce qu'il manque, c'est une **alerte non bloquante sur changement de schéma**. Le
# MAGIC > job absorbe pour ne pas s'arrêter, et signale pour qu'un humain tranche. Le
# MAGIC > silence, lui, transforme un changement de contrat en fait accompli.
# MAGIC >
# MAGIC > Concrètement : comparer le schéma courant de `bronze.orders_raw` à un schéma de
# MAGIC > référence versionné, et écrire une ligne dans `ops.contract_violations` avec le
# MAGIC > code `SCHEMA_DRIFT` quand ils divergent. Trois requêtes, et le fait accompli
# MAGIC > redevient une décision.
# MAGIC
# MAGIC **3. Le gold s'exécute avant `dq_checks` — où est le problème ?**
# MAGIC
# MAGIC > Le problème est que la « barrière » n'en est pas une. Quand `dq_gate` s'évalue,
# MAGIC > `gold.fact_order_line` a **déjà été écrite**. La condition n'empêche que la tâche
# MAGIC > `publish`, c'est-à-dire l'écriture d'une ligne de bilan. Les analystes, eux, lisent
# MAGIC > le gold : ils consomment déjà les données défectueuses.
# MAGIC >
# MAGIC > Le DAG donne l'illusion d'un contrôle qualité bloquant alors qu'il ne bloque rien.
# MAGIC > C'est pire que pas de contrôle du tout, parce que ça inspire confiance.
# MAGIC >
# MAGIC > Trois corrections possibles, par ordre de coût croissant :
# MAGIC >
# MAGIC > 1. **Déplacer les contrôles avant le gold.** `dq_checks` dépend de `silver_*` et
# MAGIC >    `scd2` ; `gold` dépend de la branche `true` de la condition. Simple, efficace,
# MAGIC >    et on ne contrôle plus le gold lui-même.
# MAGIC > 2. **Dédoubler** : des contrôles silver bloquants avant le gold, des contrôles
# MAGIC >    gold en aval qui déclenchent un `RESTORE` de la table à sa version précédente
# MAGIC >    en cas d'échec. Delta rend ça trivial, et c'est ce que je retiendrais ici.
# MAGIC > 3. **Écrire dans une zone de transit** puis basculer par `ALTER TABLE ... RENAME`
# MAGIC >    ou en repointant une vue. Le gold public ne change que si tout est vert. C'est
# MAGIC >    la solution propre, et la plus lourde.
# MAGIC >
# MAGIC > Le principe général : un contrôle qualité n'a de valeur que s'il s'interpose
# MAGIC > **entre** la production de la donnée et sa consommation. Placé après, ce n'est
# MAGIC > plus un contrôle, c'est un constat.
# MAGIC
# MAGIC **4. Le timeout**
# MAGIC
# MAGIC > 7 200 secondes, soit environ quatre fois la durée observée d'une exécution
# MAGIC > complète (~25 minutes sur Free Edition).
# MAGIC >
# MAGIC > Le raisonnement : un timeout trop serré transforme une lenteur passagère en
# MAGIC > incident ; trop large, il laisse un job bloqué brûler du quota toute la nuit — et
# MAGIC > sur Free Edition, dépasser le quota coupe le compute jusqu'au lendemain. Le
# MAGIC > facteur 3 à 5 sur la durée nominale est le compromis habituel.
# MAGIC >
# MAGIC > Le vrai piège n'est pas le job mais la **tâche** : un timeout uniquement au niveau
# MAGIC > du job laisse une tâche bloquée consommer l'intégralité du budget. Un timeout par
# MAGIC > tâche, plus court, attrape le problème plus tôt et désigne le coupable.
# MAGIC
# MAGIC **5. Tout recalculer à chaque exécution : jusqu'à quand ?**
# MAGIC
# MAGIC > Sur 290 000 lignes, tout recalculer coûte quelques minutes et achète une propriété
# MAGIC > qui vaut cher : **le résultat ne dépend pas de l'historique des exécutions**. Un
# MAGIC > job rejoué donne le même résultat, un bug corrigé se propage à tout l'historique
# MAGIC > au passage suivant, et il n'y a pas d'état à réparer après incident.
# MAGIC >
# MAGIC > Le seuil pratique se situe là où la durée dépasse la fenêtre disponible — pas là
# MAGIC > où le coût dérange. Pour un job de nuit avec quatre heures devant lui, ça arrive
# MAGIC > vers quelques dizaines de millions de lignes.
# MAGIC >
# MAGIC > Ce qu'on fait à ce moment-là, dans l'ordre de préférence :
# MAGIC >
# MAGIC > 1. **Partitionner le retraitement par période.** Recalculer les 7 derniers jours
# MAGIC >    et laisser l'historique tranquille. On garde l'idempotence sur la fenêtre
# MAGIC >    active, qui est la seule qui bouge.
# MAGIC > 2. **Vues matérialisées incrémentales** pour les agrégats — c'est exactement ce
# MAGIC >    que M7 fait, et le moteur gère l'incrémentalité mieux qu'un `MERGE` écrit à la
# MAGIC >    main.
# MAGIC > 3. **`MERGE` sur clé** en dernier recours, en acceptant de maintenir un état — et
# MAGIC >    en écrivant le test de vérification croisée de M4, sans lequel on ne saura
# MAGIC >    jamais si l'incrémental a dérivé.
# MAGIC >
# MAGIC > Ce qu'il ne faut pas faire : passer en incrémental « par principe » avant d'avoir
# MAGIC > mesuré. On échange une garantie contre une performance dont on n'a pas besoin.
