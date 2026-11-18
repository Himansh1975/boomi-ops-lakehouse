"""Lineage landing -> bronze -> silver, then the gold marts in lineage_gold.py.

bronze_component_metadata / bronze_component_refs / bronze_component_xml_edges:
    append-only, raw payload per crawl, loaded by the same loader as executions.

silver_component_versions   key (component_id, version)
    One row per version ever seen. A version is immutable in Boomi, so the only thing a later crawl
    changes is last_seen_at. This table *is* the history: Boomi itself forgets old versions' links.
silver_components           key component_id, rebuilt each run
    Latest version of each component, plus is_present = it was in the newest crawl (deleted otherwise).
silver_component_edges      key (parent_id, parent_version, child_id, source, attr, shape_name)
    Links from both sources (source = 'api' or 'xml'). Links of a version never change, so new rows are
    inserted and existing ones are left alone: re-loading any batch is a no-op.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from .lake import Lake
from .pipeline import BRONZE, boomi_ts, load_bronze

LINEAGE_BRONZE = {"ComponentMetadata": "bronze_component_metadata",
                  "ComponentReference": "bronze_component_refs",
                  "ComponentXmlEdge": "bronze_component_xml_edges"}
BRONZE.update(LINEAGE_BRONZE)  # the shared loader looks tables up here

_S, _L = StringType(), LongType()
METADATA_SCHEMA = StructType([StructField(n, t) for n, t in [
    ("componentId", _S), ("version", _L), ("name", _S), ("type", _S), ("subType", _S),
    ("folderName", _S), ("folderId", _S), ("createdDate", _S), ("createdBy", _S),
    ("modifiedDate", _S), ("modifiedBy", _S), ("deleted", _S), ("currentVersion", _S),
]])
REF_SCHEMA = StructType([StructField(n, t) for n, t in [
    ("parentComponentId", _S), ("parentVersion", _L), ("componentId", _S), ("type", _S), ("branchId", _S),
]])
XML_EDGE_SCHEMA = StructType([StructField(n, t) for n, t in [
    ("parentComponentId", _S), ("parentVersion", _L), ("componentId", _S), ("attr", _S), ("element", _S),
    ("shapeName", _S), ("shapeLabel", _S), ("shapeType", _S),
]])
EDGE_KEY = ("parent_id", "parent_version", "child_id", "source", "attr", "shape_name")


def _bronze(lake: Lake, obj: str, batch_ids: list[str], schema: StructType) -> DataFrame:
    return (lake.read(LINEAGE_BRONZE[obj]).filter(F.col("_batch_id").isin(batch_ids))
            .select(F.from_json("payload", schema).alias("r"), "_ingested_at"))


def merge_component_versions(lake: Lake, batch_ids: list[str]) -> None:
    if not batch_ids:
        return
    r = _bronze(lake, "ComponentMetadata", batch_ids, METADATA_SCHEMA)
    rows = r.select(
        F.col("r.componentId").alias("component_id"), F.col("r.version").alias("version"),
        F.col("r.name").alias("name"), F.col("r.type").alias("type"),
        F.when(F.col("r.subType") != "", F.col("r.subType")).alias("sub_type"),
        F.col("r.folderName").alias("folder_name"), F.col("r.folderId").alias("folder_id"),
        boomi_ts(F.col("r.createdDate")).alias("created_at"), F.col("r.createdBy").alias("created_by"),
        boomi_ts(F.col("r.modifiedDate")).alias("modified_at"), F.col("r.modifiedBy").alias("modified_by"),
        "_ingested_at",
    )
    src = rows.groupBy("component_id", "version").agg(
        *[F.max_by(c, "_ingested_at").alias(c) for c in
          ("name", "type", "sub_type", "folder_name", "folder_id", "created_at", "created_by",
           "modified_at", "modified_by")],
        F.min("_ingested_at").alias("first_seen_at"), F.max("_ingested_at").alias("last_seen_at"))
    if not lake.exists("silver_component_versions"):
        lake.overwrite(src, "silver_component_versions")
        return
    (lake.delta("silver_component_versions").alias("t")
     .merge(src.alias("s"), "t.component_id = s.component_id AND t.version = s.version")
     .whenMatchedUpdate(condition="s.last_seen_at > t.last_seen_at", set={
         "last_seen_at": "s.last_seen_at", "name": "s.name", "folder_name": "s.folder_name",
         "folder_id": "s.folder_id"})
     .whenMatchedUpdate(condition="s.first_seen_at < t.first_seen_at", set={"first_seen_at": "s.first_seen_at"})
     .whenNotMatchedInsertAll()
     .execute())


def build_current_components(lake: Lake) -> None:
    v = lake.read("silver_component_versions")
    newest_crawl = v.agg(F.max("last_seen_at")).first()[0]
    latest = (v.groupBy("component_id").agg(F.max_by(F.struct(*v.columns), "version").alias("c"),
                                            F.count("*").alias("versions_seen"))
              .select("c.*", "versions_seen"))
    lake.overwrite(latest.withColumn("is_present", F.col("last_seen_at") == F.lit(newest_crawl)),
                   "silver_components")


def merge_edges(lake: Lake, ref_batches: list[str], xml_batches: list[str]) -> None:
    parts = []
    if ref_batches:
        r = _bronze(lake, "ComponentReference", ref_batches, REF_SCHEMA)
        parts.append(r.select(
            F.col("r.parentComponentId").alias("parent_id"), F.col("r.parentVersion").alias("parent_version"),
            F.col("r.componentId").alias("child_id"), F.lit("api").alias("source"),
            F.col("r.type").alias("ref_type"), F.lit("").alias("attr"), F.lit("").alias("element"),
            F.lit("").alias("shape_name"), F.lit(None).cast("string").alias("shape_label"),
            F.lit(None).cast("string").alias("shape_type"), "_ingested_at"))
    if xml_batches:
        x = _bronze(lake, "ComponentXmlEdge", xml_batches, XML_EDGE_SCHEMA)
        parts.append(x.select(
            F.col("r.parentComponentId").alias("parent_id"), F.col("r.parentVersion").alias("parent_version"),
            F.col("r.componentId").alias("child_id"), F.lit("xml").alias("source"),
            F.lit(None).cast("string").alias("ref_type"), F.col("r.attr").alias("attr"),
            F.col("r.element").alias("element"), F.coalesce(F.col("r.shapeName"), F.lit("")).alias("shape_name"),
            F.col("r.shapeLabel").alias("shape_label"), F.col("r.shapeType").alias("shape_type"),
            "_ingested_at"))
    if not parts:
        return
    df = parts[0] if len(parts) == 1 else parts[0].unionByName(parts[1])
    src = (df.groupBy(*EDGE_KEY)
           .agg(*[F.first(c, ignorenulls=True).alias(c) for c in ("ref_type", "element", "shape_label", "shape_type")],
                F.min("_ingested_at").alias("first_seen_at")))
    if not lake.exists("silver_component_edges"):
        lake.overwrite(src, "silver_component_edges")
        return
    cond = " AND ".join(f"t.{k} = s.{k}" for k in EDGE_KEY)
    lake.delta("silver_component_edges").alias("t").merge(src.alias("s"), cond).whenNotMatchedInsertAll().execute()


def run_lineage_transforms(lake: Lake) -> dict[str, int]:
    if not (lake.landing / "ComponentMetadata").exists():
        return {}
    new_meta = load_bronze(lake, "ComponentMetadata")
    new_refs = load_bronze(lake, "ComponentReference") if (lake.landing / "ComponentReference").exists() else []
    new_xml = load_bronze(lake, "ComponentXmlEdge") if (lake.landing / "ComponentXmlEdge").exists() else []
    merge_component_versions(lake, new_meta)
    merge_edges(lake, new_refs, new_xml)
    out = {"new_component_batches": len(new_meta), "new_reference_batches": len(new_refs),
           "new_xml_edge_batches": len(new_xml)}
    if lake.exists("silver_component_versions"):
        build_current_components(lake)
    if lake.exists("silver_component_edges"):
        from .lineage_gold import build_lineage_gold
        out.update(build_lineage_gold(lake))
    return out
