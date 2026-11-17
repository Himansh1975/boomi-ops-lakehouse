"""Landing JSON -> bronze -> silver (MERGE) -> gold marts, all Delta.

bronze_*  : append-only, one row per record per pull, payload kept as the raw JSON string.
silver_*  : one row per business key, latest *trustworthy* state wins (see merge rule below).
gold_*    : rebuilt from silver on every run. The data is small (thousands of rows a day), so a full
            rebuild is simpler and always correct. Switch to incremental when it stops being cheap.
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from ..fingerprint import MAX_LEN, RULES
from ..simulator import FINAL
from .lake import Lake

LANDING_SCHEMA = StructType([StructField(c, StringType()) for c in
                             ("_batch_id", "_object", "_ingested_at", "_window_start", "_window_end", "payload")])

_S, _L = StringType(), LongType()
EXECUTION_SCHEMA = StructType([StructField(n, t) for n, t in [
    ("executionId", _S), ("originalExecutionId", _S), ("account", _S), ("executionTime", _S),
    ("status", _S), ("executionType", _S), ("processName", _S), ("processId", _S),
    ("atomName", _S), ("atomId", _S), ("inboundDocumentCount", _L), ("inboundErrorDocumentCount", _L),
    ("outboundDocumentCount", _L),
    # documented as int64 but returned as ["Long", 3832]: read as raw text and parse both forms
    ("executionDuration", _S), ("inboundDocumentSize", _S), ("outboundDocumentSize", _S),
    ("message", _S), ("topLevelExecutionId", _S), ("parentExecutionId", _S), ("recordedDate", _S),
    ("nodeId", _S), ("launcherID", _S), ("reportKey", _S),
]])

DEPLOYMENT_SCHEMA = StructType([StructField(n, t) for n, t in [
    ("deploymentId", _S), ("componentId", _S), ("componentName", _S), ("componentType", _S),
    ("componentVersion", _L), ("packageVersion", _S), ("version", _L), ("deployedDate", _S),
    ("deployedBy", _S), ("environmentId", _S), ("environmentName", _S), ("active", StringType()),
    ("notes", _S),
]])

BRONZE = {"ExecutionRecord": "bronze_execution_records", "DeployedPackage": "bronze_deployed_packages"}
ERROR_STATUSES = ("ERROR", "ABORTED")
SUSPICIOUS_RATE_JUMP = 0.20
SUSPICIOUS_MIN_ERRORS = 3


def boomi_long(col: str):
    """'3832' or '["Long",3832]' -> 3832."""
    return F.regexp_extract(F.col(col), r"(-?\d+)\s*\]?\s*$", 1).cast("long")


def boomi_ts(col):
    return F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss'Z'")


def error_template(col):
    out = F.coalesce(col, F.lit(""))
    for pattern, repl in RULES:
        out = F.regexp_replace(out, pattern, repl)
    return F.substring(F.trim(out), 1, MAX_LEN)


# ---- bronze ---------------------------------------------------------------------------------
def load_bronze(lake: Lake, obj: str) -> list[str]:
    """Appends landing files not loaded yet. Returns the new batch ids.

    On Databricks this is the job Auto Loader does; listing files keeps it runnable on a laptop.
    """
    files = sorted((lake.landing / obj).glob("ingest_date=*/*.jsonl"))
    table = BRONZE[obj]
    loaded = set()
    if lake.exists(table):
        loaded = {r[0] for r in lake.read(table).select("_batch_id").distinct().collect()}
    new = [f for f in files if f.stem not in loaded and f.stat().st_size > 0]
    if not new:
        return []
    df = (lake.spark.read.schema(LANDING_SCHEMA).json([str(f) for f in new])
          .withColumn("_ingested_at", boomi_ts("_ingested_at"))
          .withColumn("_source_file", F.input_file_name()))
    lake.append(df, table)
    return [f.stem for f in new]


# ---- silver ---------------------------------------------------------------------------------
def _parse_executions(df: DataFrame) -> DataFrame:
    r = df.select(F.from_json("payload", EXECUTION_SCHEMA).alias("r"), "_ingested_at", "_batch_id")
    tmpl = error_template(F.col("r.message"))
    return r.select(
        F.col("r.executionId").alias("execution_id"),
        F.col("r.processId").alias("process_id"),
        F.col("r.processName").alias("process_name"),
        F.col("r.atomId").alias("atom_id"),
        F.col("r.atomName").alias("atom_name"),
        F.col("r.executionType").alias("execution_type"),
        F.col("r.status").alias("status"),
        F.col("r.status").isin(*FINAL).alias("is_final"),
        boomi_ts(F.col("r.executionTime")).alias("execution_time"),
        boomi_ts(F.col("r.recordedDate")).alias("recorded_time"),
        boomi_long("r.executionDuration").alias("duration_ms"),
        F.col("r.inboundDocumentCount").alias("docs_in"),
        F.col("r.outboundDocumentCount").alias("docs_out"),
        F.col("r.inboundErrorDocumentCount").alias("error_docs"),
        boomi_long("r.inboundDocumentSize").alias("bytes_in"),
        boomi_long("r.outboundDocumentSize").alias("bytes_out"),
        F.when(F.col("r.message") != "", F.col("r.message")).alias("message"),
        F.when(tmpl != "", tmpl).alias("error_template"),
        F.when(tmpl != "", F.substring(F.sha2(tmpl, 256), 1, 16)).alias("error_fingerprint"),
        F.col("r.topLevelExecutionId").alias("top_level_execution_id"),
        F.col("r.parentExecutionId").alias("parent_execution_id"),
        "_ingested_at",
        "_batch_id",
    )


def _latest(df: DataFrame, key: str, *order) -> DataFrame:
    w = Window.partitionBy(key).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def merge_executions(lake: Lake, batch_ids: list[str]) -> None:
    if not batch_ids:
        return
    bronze = lake.read(BRONZE["ExecutionRecord"]).filter(F.col("_batch_id").isin(batch_ids))
    src = _latest(_parse_executions(bronze), "execution_id",
                  F.col("is_final").desc(), F.col("_ingested_at").desc())
    if not lake.exists("silver_executions"):
        lake.overwrite(src, "silver_executions")
        return
    # Merge rule: a final status (COMPLETE/ERROR/...) beats INPROCESS no matter which arrived last,
    # so re-loading an old batch can never send a finished run back to "running".
    # Between two records of the same kind, the newer pull wins.
    (lake.delta("silver_executions").alias("t")
     .merge(src.alias("s"), "t.execution_id = s.execution_id")
     .whenMatchedUpdateAll(condition="(s.is_final AND NOT t.is_final) OR "
                                     "(s.is_final = t.is_final AND s._ingested_at > t._ingested_at)")
     .whenNotMatchedInsertAll()
     .execute())


def merge_deployments(lake: Lake, batch_ids: list[str]) -> None:
    if not batch_ids:
        return
    bronze = lake.read(BRONZE["DeployedPackage"]).filter(F.col("_batch_id").isin(batch_ids))
    r = bronze.select(F.from_json("payload", DEPLOYMENT_SCHEMA).alias("r"), "_ingested_at")
    src = _latest(r.select(
        F.col("r.deploymentId").alias("deployment_id"),
        F.col("r.componentId").alias("component_id"),
        F.col("r.componentName").alias("component_name"),
        F.col("r.componentType").alias("component_type"),
        F.col("r.componentVersion").alias("component_version"),
        F.col("r.packageVersion").alias("package_version"),
        F.col("r.version").alias("deployment_version"),
        boomi_ts(F.col("r.deployedDate")).alias("deployed_at"),
        F.col("r.deployedBy").alias("deployed_by"),
        F.col("r.environmentName").alias("environment_name"),
        F.col("r.notes").alias("notes"),
        "_ingested_at",
    ), "deployment_id", F.col("_ingested_at").desc())
    if not lake.exists("silver_deployments"):
        lake.overwrite(src, "silver_deployments")
        return
    (lake.delta("silver_deployments").alias("t")
     .merge(src.alias("s"), "t.deployment_id = s.deployment_id")
     .whenMatchedUpdateAll(condition="s._ingested_at > t._ingested_at")
     .whenNotMatchedInsertAll()
     .execute())


# ---- gold -----------------------------------------------------------------------------------
def build_gold(lake: Lake) -> None:
    ex = lake.read("silver_executions")
    fct = ex.withColumn("run_date", F.to_date("execution_time")) \
            .withColumn("is_error", F.col("status").isin(*ERROR_STATUSES))
    lake.overwrite(fct, "gold_fct_executions")
    fct = lake.read("gold_fct_executions")

    sla = fct.groupBy("process_id", "process_name", "run_date").agg(
        F.count("*").alias("runs"),
        F.sum(F.col("is_final").cast("int")).alias("finished"),
        F.sum((~F.col("is_final")).cast("int")).alias("still_running"),
        F.sum(F.col("is_error").cast("int")).alias("errors"),
        F.sum((F.col("status") == "COMPLETE_WARN").cast("int")).alias("warnings"),
        F.expr("percentile_approx(CASE WHEN is_final THEN duration_ms END, 0.5) / 1000").alias("p50_duration_s"),
        F.expr("percentile_approx(CASE WHEN is_final THEN duration_ms END, 0.95) / 1000").alias("p95_duration_s"),
        F.sum("docs_in").alias("docs_in"),
        F.sum("docs_out").alias("docs_out"),
        F.sum("error_docs").alias("error_docs"),
    ).withColumn("success_rate", F.round((F.col("finished") - F.col("errors")) / F.col("finished"), 4))
    lake.overwrite(sla, "gold_mart_process_sla_daily")

    clusters = fct.filter("is_error AND error_fingerprint IS NOT NULL").groupBy(
        "error_fingerprint", "process_id", "process_name").agg(
        F.first("error_template").alias("error_template"),
        F.count("*").alias("runs"),
        F.countDistinct("run_date").alias("days_seen"),
        F.min("execution_time").alias("first_seen"),
        F.max("execution_time").alias("last_seen"),
        F.max_by("message", "execution_time").alias("latest_message"),
        F.max_by("execution_id", "execution_time").alias("latest_execution_id"),
    )
    lake.overwrite(clusters, "gold_mart_error_clusters")

    if not lake.exists("silver_deployments"):
        return
    d = lake.read("silver_deployments").filter("component_type = 'process'")
    e = fct.filter("is_final").select("process_id", "execution_time", "is_error")
    joined = d.join(e, (e.process_id == d.component_id)
                    & (e.execution_time >= d.deployed_at - F.expr("INTERVAL 24 HOURS"))
                    & (e.execution_time < d.deployed_at + F.expr("INTERVAL 24 HOURS")))
    after = F.col("execution_time") >= F.col("deployed_at")
    rpt = joined.groupBy("deployment_id", "component_id", "component_name", "package_version",
                         "deployed_at", "deployed_by", "notes").agg(
        F.sum((~after).cast("int")).alias("runs_before"),
        F.sum(((~after) & F.col("is_error")).cast("int")).alias("errors_before"),
        F.sum(after.cast("int")).alias("runs_after"),
        F.sum((after & F.col("is_error")).cast("int")).alias("errors_after"),
    )
    rate = lambda err, runs: F.when(F.col(runs) > 0, F.round(F.col(err) / F.col(runs), 4))  # noqa: E731
    rpt = (rpt.withColumn("error_rate_before", rate("errors_before", "runs_before"))
              .withColumn("error_rate_after", rate("errors_after", "runs_after"))
              .withColumn("is_suspicious",
                          (F.coalesce(F.col("error_rate_after"), F.lit(0.0))
                           - F.coalesce(F.col("error_rate_before"), F.lit(0.0)) >= SUSPICIOUS_RATE_JUMP)
                          & (F.col("errors_after") >= SUSPICIOUS_MIN_ERRORS)))
    lake.overwrite(rpt, "gold_rpt_failures_after_deploy")


def run_transforms(lake: Lake) -> dict[str, int]:
    new_exec = load_bronze(lake, "ExecutionRecord")
    new_dep = load_bronze(lake, "DeployedPackage")
    merge_executions(lake, new_exec)
    merge_deployments(lake, new_dep)
    if lake.exists("silver_executions"):
        build_gold(lake)
    return {"new_execution_batches": len(new_exec), "new_deployment_batches": len(new_dep)}
