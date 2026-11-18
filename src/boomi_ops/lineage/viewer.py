"""Builds the Blast Radius viewer: one self-contained HTML page with the lineage graph baked in.

    python -m boomi_ops.run lineage viewer --out site/index.html

The page needs no server and no internet: no libraries, no CDN, just HTML, CSS and SVG drawn by a small script.
Open the file, or host it anywhere static (GitHub Pages in CI, from replay data).

Link options: ?component=<id or name>  ?mode=deps  ?theme=light|dark  ?tour=1 (steps through the riskiest
components, handy for screen recordings).
Only gold tables go in, and those already hold pseudonyms instead of emails.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from ..transform.lake import Lake

TEMPLATE = Path(__file__).with_name("viewer_template.html")
PLACEHOLDER = "/*__LINEAGE_DATA__*/null"

RECENT_FAILURES = 100  # per process, for "who failed at the same time?" in lineage.queries

KIND = {"connector-settings": "connection", "connector-action": "operation", "transform.map": "map",
        "process": "process"}


def _kind(m: dict) -> str:
    if m["type"] == "process":
        return "process" if m["is_top_level"] else "subprocess"
    if m["type"].startswith("profile."):
        return "profile"
    return KIND.get(m["type"], "other")


def _ts(v) -> str | None:
    return v.strftime("%Y-%m-%dT%H:%MZ") if v else None


def graph_data(lake: Lake, source: str) -> dict:
    mart = [r.asDict() for r in lake.read("gold_mart_blast_radius").collect()]
    edges = [r.asDict() for r in lake.read("gold_fct_lineage_edges").collect()]
    orphans = {r.component_id: r.reason for r in lake.read("gold_rpt_orphans").collect()}
    failures: dict[str, list[dict]] = {}
    if lake.exists("gold_fct_executions"):
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        w = Window.partitionBy("process_id").orderBy(F.col("execution_time").desc())
        recent = (lake.read("gold_fct_executions").filter("is_error")
                  .withColumn("_rn", F.row_number().over(w)).filter(f"_rn <= {RECENT_FAILURES}"))
        for r in recent.select("process_id", "execution_time", "error_fingerprint", "error_template").collect():
            failures.setdefault(r.process_id, []).append(
                {"at": _ts(r.execution_time), "fingerprint": r.error_fingerprint, "error": r.error_template})
    nodes = []
    for m in sorted(mart, key=lambda r: r["name"]):
        nodes.append({
            "id": m["component_id"], "name": m["name"], "type": m["type"], "subType": m["sub_type"],
            "kind": _kind(m), "folder": m["folder_name"], "version": m["version"],
            "modifiedAt": _ts(m["modified_at"]), "modifiedBy": m["modified_by"],
            "processesDirect": m["processes_direct"], "processesTotal": m["processes_total"],
            "deployedProcesses": m["deployed_processes"], "usedBy": m["used_by"] or [],
            "runs": m["runs_30d"], "failures": m["failures_30d"], "docs": m["docs_30d"],
            "lastRunAt": _ts(m["last_run_at"]), "risk": m["risk_score"], "deployedVersion": m["deployed_version"],
            "environments": sorted(set((m["environments"] or []) + (m["own_environments"] or []))),
            "isTopLevel": m["is_top_level"], "inCycle": m["in_cycle"], "orphan": orphans.get(m["component_id"]),
            "recentFailures": sorted(failures.get(m["component_id"], []), key=lambda f: f["at"], reverse=True),
        })
    known = {n["id"] for n in nodes}
    for e in edges:
        if e["child_id"] not in known:  # a link to a deleted component: draw a ghost node
            known.add(e["child_id"])
            nodes.append({"id": e["child_id"], "name": "deleted component", "type": "deleted", "kind": "deleted",
                          "usedBy": [], "environments": [], "runs": 0, "failures": 0, "processesTotal": 0})
    links = [{"id": f"{e['parent_id']}>{e['child_id']}", "source": e["parent_id"], "target": e["child_id"],
              "inApi": e["in_api"], "inXml": e["in_xml"], "shapes": e["shape_labels"] or [],
              "dangling": e["is_dangling"]} for e in edges]
    return {"source": source, "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "nodes": nodes, "edges": links}


def render(data: dict) -> str:
    html = TEMPLATE.read_text(encoding="utf-8")
    assert PLACEHOLDER in html, "viewer template lost its data placeholder"
    payload = json.dumps(data, separators=(",", ":")).replace("</", "<\\/")  # safe inside <script>
    return html.replace(PLACEHOLDER, payload)


def build_viewer(lake: Lake, out: Path, source: str) -> Path:
    """Writes the page and, next to it, lineage.json: the same graph for the MCP server (no Spark needed)."""
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    data = graph_data(lake, source)
    out.write_text(render(data), encoding="utf-8")
    (out.parent / "lineage.json").write_text(json.dumps(data, indent=1), encoding="utf-8")
    return out
