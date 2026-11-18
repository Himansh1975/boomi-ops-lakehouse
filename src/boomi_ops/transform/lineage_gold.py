"""Lineage gold: the graph, its transitive closure and the "what breaks if I change this?" marts.

gold_fct_lineage_edges      one row per current link (parent's current version only), API and XML combined:
                            in_api / in_xml flags, shape labels, is_dangling (child no longer exists)
gold_fct_lineage_closure    ancestor -> descendant over any number of hops, with the shortest depth.
                            Built by semi-naive iteration (join only the newest rows each round), because
                            Spark SQL has no recursive CTE. A cycle ends the loop on its own: the anti-join
                            finds nothing new. A row with ancestor = descendant marks a cycle.
gold_mart_blast_radius      per component: processes that use it (directly / at any depth / deployed),
                            and those processes' runs, failures and documents in the last 30 days
gold_rpt_orphans            components no process reaches, and top-level processes never deployed
gold_rpt_shared_hotspots    shared components with the widest reach (the "one connection holds up
                            everything" list)
gold_rpt_lineage_drift      links the API and XML disagree on, links to deleted components, and processes
                            whose deployed version is behind the latest saved version

Everything is rebuilt from silver each run: an account has hundreds to a few thousand components.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .lake import Lake

MAX_DEPTH = 25        # nested sub-processes deeper than this would be a data problem, not a design
WINDOW_DAYS = 30
HOTSPOT_MIN_PROCESSES = 3


def current_edges(lake: Lake) -> DataFrame:
    comps = lake.read("silver_components").filter("is_present")
    e = lake.read("silver_component_edges")
    cur = comps.select(F.col("component_id").alias("parent_id"), F.col("version").alias("parent_version"))
    e = e.join(cur, ["parent_id", "parent_version"])  # links of the version that is live now
    edges = e.groupBy("parent_id", "child_id").agg(
        F.max((F.col("source") == "api").cast("int")).cast("boolean").alias("in_api"),
        F.max((F.col("source") == "xml").cast("int")).cast("boolean").alias("in_xml"),
        F.max("ref_type").alias("ref_type"),
        F.array_sort(F.collect_set(F.when(F.col("shape_label") != "", F.col("shape_label")))).alias("shape_labels"),
    )
    p = comps.select(F.col("component_id").alias("parent_id"), F.col("name").alias("parent_name"),
                     F.col("type").alias("parent_type"))
    c = comps.select(F.col("component_id").alias("child_id"), F.col("name").alias("child_name"),
                     F.col("type").alias("child_type"), F.col("sub_type").alias("child_sub_type"))
    return (edges.join(p, "parent_id").join(c, "child_id", "left")
            .withColumn("is_dangling", F.col("child_name").isNull()))


def closure(edges: DataFrame) -> DataFrame:
    """All (ancestor, descendant, depth) pairs. Uses API links (the platform's own view) plus XML-only links."""
    e = edges.filter("NOT is_dangling").select(F.col("parent_id").alias("a"), F.col("child_id").alias("d")).distinct()
    e = e.localCheckpoint()
    total = delta = e.withColumn("depth", F.lit(1)).localCheckpoint()
    for depth in range(2, MAX_DEPTH + 1):
        nxt = (delta.alias("x").join(e.alias("y"), F.col("x.d") == F.col("y.a"))
               .select(F.col("x.a").alias("a"), F.col("y.d").alias("d")).distinct()
               .join(total.select("a", "d"), ["a", "d"], "left_anti")
               .withColumn("depth", F.lit(depth))).localCheckpoint()
        if nxt.isEmpty():
            break
        total = total.unionByName(nxt).localCheckpoint()
        delta = nxt
    return total.select(F.col("a").alias("ancestor_id"), F.col("d").alias("descendant_id"), "depth")


def _deployments(lake: Lake) -> DataFrame | None:
    if not lake.exists("silver_deployments"):
        return None
    d = lake.read("silver_deployments")
    return d.groupBy("component_id").agg(
        F.max_by("component_version", "deployed_at").alias("deployed_version"),
        F.max("deployed_at").alias("last_deployed_at"),
        F.array_sort(F.collect_set("environment_name")).alias("environments"))


def _process_runs(lake: Lake) -> DataFrame | None:
    if not lake.exists("gold_fct_executions"):
        return None
    f = lake.read("gold_fct_executions").filter("is_final")
    end = f.agg(F.max("execution_time")).first()[0]
    if end is None:
        return None
    f = f.filter(F.col("execution_time") > F.lit(end) - F.expr(f"INTERVAL {WINDOW_DAYS} DAYS"))
    return f.groupBy(F.col("process_id").alias("component_id")).agg(
        F.count("*").alias("runs"), F.sum(F.col("is_error").cast("int")).alias("failures"),
        F.sum("docs_in").alias("docs"), F.max("execution_time").alias("last_run_at"))


def build_lineage_gold(lake: Lake) -> dict[str, int]:
    spark = lake.spark
    comps = lake.read("silver_components").filter("is_present")
    edges = current_edges(lake)
    lake.overwrite(edges, "gold_fct_lineage_edges")
    edges = lake.read("gold_fct_lineage_edges")

    clo = closure(edges)
    lake.overwrite(clo, "gold_fct_lineage_closure")
    clo = lake.read("gold_fct_lineage_closure")

    procs = comps.filter("type = 'process'").select("component_id")
    called = edges.filter("child_type = 'process'").select(F.col("child_id").alias("component_id")).distinct()
    top = procs.join(called, "component_id", "left_anti").withColumn("is_top_level", F.lit(True))

    dep = _deployments(lake)
    if dep is None:
        dep = spark.createDataFrame([], "component_id string, deployed_version long, last_deployed_at timestamp, "
                                        "environments array<string>")
    runs = _process_runs(lake)
    if runs is None:
        runs = spark.createDataFrame([], "component_id string, runs long, failures long, docs long, "
                                         "last_run_at timestamp")

    # who uses each component: its process ancestors (a process also "uses" itself, for its own runs)
    users = (clo.select(F.col("descendant_id").alias("component_id"), F.col("ancestor_id").alias("user_id"), "depth")
             .unionByName(procs.select("component_id", F.col("component_id").alias("user_id"), F.lit(0).alias("depth"))))
    user_info = (comps.filter("type = 'process'").select(F.col("component_id").alias("user_id"),
                                                         F.col("name").alias("user_name"))
                 .join(top.select(F.col("component_id").alias("user_id"), "is_top_level"), "user_id", "left")
                 .join(dep.select(F.col("component_id").alias("user_id"), "deployed_version", "environments"),
                       "user_id", "left")
                 .join(runs.select(F.col("component_id").alias("user_id"), "runs", "failures", "docs", "last_run_at"),
                       "user_id", "left"))
    u = users.join(user_info, "user_id")  # inner join keeps process users only
    others = u.filter("depth > 0 AND user_id != component_id")  # in a cycle a component reaches itself
    agg = others.groupBy("component_id").agg(
        F.countDistinct(F.when(F.col("depth") == 1, F.col("user_id"))).alias("processes_direct"),
        F.countDistinct("user_id").alias("processes_total"),
        F.countDistinct(F.when(F.col("is_top_level"), F.col("user_id"))).alias("top_level_processes"),
        F.countDistinct(F.when(F.col("deployed_version").isNotNull(), F.col("user_id"))).alias("deployed_processes"),
        F.array_sort(F.collect_set("user_name")).alias("used_by"),
        F.array_sort(F.array_distinct(F.flatten(F.collect_list(F.coalesce("environments", F.array().cast("array<string>")))))).alias("environments"),
    )
    # runs come from top-level processes only: a sub-process runs inside its parent's execution
    run_agg = u.filter("is_top_level").groupBy("component_id").agg(
        F.coalesce(F.sum("runs"), F.lit(0)).alias(f"runs_{WINDOW_DAYS}d"),
        F.coalesce(F.sum("failures"), F.lit(0)).alias(f"failures_{WINDOW_DAYS}d"),
        F.coalesce(F.sum("docs"), F.lit(0)).alias(f"docs_{WINDOW_DAYS}d"),
        F.max("last_run_at").alias("last_run_at"))
    in_cycle = clo.filter("ancestor_id = descendant_id").select(F.col("ancestor_id").alias("component_id"),
                                                               F.lit(True).alias("in_cycle"))
    runs_col = F.col(f"runs_{WINDOW_DAYS}d")
    mart = (comps.select("component_id", "name", "type", "sub_type", "folder_name", "version", "modified_at",
                         "modified_by")
            .join(agg, "component_id", "left").join(run_agg, "component_id", "left")
            .join(top.select("component_id", "is_top_level"), "component_id", "left")
            .join(dep.withColumnRenamed("environments", "own_environments"), "component_id", "left")
            .join(in_cycle, "component_id", "left")
            .fillna(0, ["processes_direct", "processes_total", "top_level_processes", "deployed_processes",
                        f"runs_{WINDOW_DAYS}d", f"failures_{WINDOW_DAYS}d", f"docs_{WINDOW_DAYS}d"])
            .fillna(False, ["is_top_level", "in_cycle"])
            .withColumn("risk_score", F.round(F.col("processes_total") * F.log1p(runs_col)
                                              * (1 + (F.col("deployed_processes") > 0).cast("int")), 2)))
    lake.overwrite(mart, "gold_mart_blast_radius")
    mart = lake.read("gold_mart_blast_radius")

    orphans = (mart.filter("type != 'process' AND processes_total = 0")
               .join(edges.filter("NOT is_dangling").select(F.col("child_id").alias("component_id")).distinct()
                     .withColumn("has_parent", F.lit(True)), "component_id", "left")
               .withColumn("reason", F.when(F.col("has_parent"), "only used by other orphans")
                           .otherwise("nothing references it"))
               .unionByName(mart.filter("type = 'process' AND is_top_level AND deployed_version IS NULL")
                            .withColumn("has_parent", F.lit(False))
                            .withColumn("reason", F.lit("process never deployed")))
               .select("component_id", "name", "type", "folder_name", "modified_at", "modified_by", "reason"))
    lake.overwrite(orphans, "gold_rpt_orphans")

    hot = (mart.filter(f"NOT is_top_level AND processes_total >= {HOTSPOT_MIN_PROCESSES}")
           .select("component_id", "name", "type", "sub_type", "processes_total", "deployed_processes",
                   f"runs_{WINDOW_DAYS}d", f"failures_{WINDOW_DAYS}d", "risk_score", "used_by"))
    lake.overwrite(hot, "gold_rpt_shared_hotspots")

    drift = (edges.filter("is_dangling OR in_api != in_xml")
             .select(F.lit("link").alias("kind"), "parent_id", "parent_name", "child_id", "child_name",
                     F.when(F.col("is_dangling"), "references a component that no longer exists")
                     .when(F.col("in_api"), "listed by the API but not used in the XML (stale reference)")
                     .otherwise("used in the XML but not listed by the API").alias("issue"))
             .unionByName(mart.filter("deployed_version IS NOT NULL AND deployed_version < version")
                          .select(F.lit("deployment").alias("kind"), F.col("component_id").alias("parent_id"),
                                  F.col("name").alias("parent_name"), F.lit(None).cast("string").alias("child_id"),
                                  F.lit(None).cast("string").alias("child_name"),
                                  F.format_string("edited to v%d but v%d is deployed", "version",
                                                  "deployed_version").alias("issue"))))
    lake.overwrite(drift, "gold_rpt_lineage_drift")

    return {"lineage_edges": edges.count(), "closure_rows": clo.count(), "orphans": orphans.count(),
            "drift": drift.count()}


def check_lineage_gold(lake: Lake) -> list[str]:
    """Data tests. Returns the failed checks (empty = all good)."""
    fails = []
    mart = lake.read("gold_mart_blast_radius")
    comps = lake.read("silver_components").filter("is_present")
    if mart.count() != comps.count() or mart.select("component_id").distinct().count() != mart.count():
        fails.append("blast radius must have exactly one row per present component")
    clo = lake.read("gold_fct_lineage_closure")
    if clo.filter("ancestor_id IS NULL OR descendant_id IS NULL OR depth < 1").count():
        fails.append("closure has null ids or a depth below 1")
    if clo.groupBy("ancestor_id", "descendant_id").count().filter("count > 1").count():
        fails.append("closure has duplicate pairs")
    if mart.filter("processes_direct > processes_total OR deployed_processes > processes_total").count():
        fails.append("blast radius counts are inconsistent")
    edges = lake.read("gold_fct_lineage_edges")
    if edges.filter("NOT in_api AND NOT in_xml").count():
        fails.append("an edge must come from at least one source")
    return fails
