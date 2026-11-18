"""Terminal views of the lineage gold tables: `lineage show` and `lineage impact`.

The gold tables are small, so they're collected to Python and printed as plain text: no Spark show()
tables, so the output reads well in PowerShell too.
"""
from __future__ import annotations

from ..transform.lake import Lake


def _rows(lake: Lake, table: str) -> list[dict]:
    return [r.asDict() for r in lake.read(table).collect()] if lake.exists(table) else []


def _n(count: int, word: str) -> str:
    return f"{count} {word}" + ("" if count == 1 else "s")


def _fmt_component(m: dict) -> str:
    kind = m["type"] + (f"/{m['sub_type']}" if m.get("sub_type") else "")
    return f"{m['name']}  [{kind}]"


def find(mart: list[dict], query: str) -> list[dict]:
    q = query.strip().lower()
    exact = [m for m in mart if m["name"].lower() == q or m["component_id"] == q]
    return exact or [m for m in mart if q in m["name"].lower()]


def impact(lake: Lake, query: str) -> str:
    mart = _rows(lake, "gold_mart_blast_radius")
    if not mart:
        return "No lineage yet. Run a crawl first: python -m boomi_ops.run lineage crawl"
    hits = find(mart, query)
    if not hits:
        return f"No component matches '{query}'."
    if len(hits) > 1:
        return "Several components match. Use the full name:\n" + "\n".join(f"  {_fmt_component(m)}" for m in hits)
    m = hits[0]
    by_id = {r["component_id"]: r for r in mart}
    edges = _rows(lake, "gold_fct_lineage_edges")
    parents: dict[str, list[str]] = {}
    children: dict[str, list[dict]] = {}
    for e in edges:
        parents.setdefault(e["child_id"], []).append(e["parent_id"])
        children.setdefault(e["parent_id"], []).append(e)

    out = [_fmt_component(m)]
    if m["type"] == "process" and m["is_top_level"]:
        runs = f"{_n(m['runs_30d'], 'run')}, {_n(m['failures_30d'], 'failure')} in the last 30 days"
        dep = f"deployed v{m['deployed_version']}" if m.get("deployed_version") else "never deployed"
        out.append(f"  Top-level process, {dep}, {runs}")
    else:
        out.append(f"  Changing this affects {m['processes_total']} process"
                   f"{'' if m['processes_total'] == 1 else 'es'} ({m['deployed_processes']} deployed), "
                   f"{_n(m['runs_30d'], 'run')} and {_n(m['failures_30d'], 'failure')} in the last 30 days. "
                   f"Risk score {m['risk_score']}.")
    if m.get("modified_at"):
        out.append(f"  Last edited {m['modified_at']:%Y-%m-%d %H:%M} UTC by {m.get('modified_by') or '?'} (v{m['version']})")
    if m["in_cycle"]:
        out.append("  Warning: this component is part of a sub-process cycle")

    def up(cid: str, prefix: str, seen: frozenset) -> None:
        ps = sorted(set(parents.get(cid, [])), key=lambda p: by_id.get(p, {}).get("name", p))
        for i, p in enumerate(ps):
            last = i == len(ps) - 1
            info = by_id.get(p, {"name": p, "type": "?"})
            extra = ""
            if info.get("type") == "process" and info.get("is_top_level"):
                d = f"deployed v{info['deployed_version']}" if info.get("deployed_version") else "not deployed"
                extra = f"  ({d}, {_n(info['runs_30d'], 'run')}, {_n(info['failures_30d'], 'failure')})"
            loop = "  <- cycle" if p in seen else ""
            out.append(f"{prefix}{'└─ ' if last else '├─ '}{info['name']}{extra}{loop}")
            if p not in seen:
                up(p, prefix + ("   " if last else "│  "), seen | {p})

    if parents.get(m["component_id"]):
        out.append("  Used by:")
        up(m["component_id"], "    ", frozenset({m["component_id"]}))
    deps = sorted(children.get(m["component_id"], []), key=lambda e: e.get("child_name") or "")
    if deps:
        out.append("  Uses:")
        for e in deps:
            name = e["child_name"] or f"{e['child_id']} (deleted)"
            where = f"  in shape '{', '.join(e['shape_labels'])}'" if e.get("shape_labels") else ""
            out.append(f"    - {name} [{e.get('child_type') or '?'}]{where}")
    return "\n".join(out)


def show(lake: Lake, top: int = 10) -> str:
    mart = _rows(lake, "gold_mart_blast_radius")
    if not mart:
        return "No lineage yet. Run a crawl first: python -m boomi_ops.run lineage crawl"
    out = [f"== Lineage: {len(mart)} components, {len(_rows(lake, 'gold_fct_lineage_edges'))} links =="]
    out.append("\n== Highest blast radius (shared components) ==")
    for h in sorted(_rows(lake, "gold_rpt_shared_hotspots"), key=lambda r: -r["risk_score"])[:top]:
        out.append(f"  {h['risk_score']:>6}  {h['name']} [{h['type']}]: {h['processes_total']} processes, "
                   f"{_n(h['runs_30d'], 'run')} / {_n(h['failures_30d'], 'failure')} in 30 days")
    out.append("\n== Orphans ==")
    for o in sorted(_rows(lake, "gold_rpt_orphans"), key=lambda r: (r["reason"], r["name"])):
        out.append(f"  {o['name']} [{o['type']}]: {o['reason']}")
    out.append("\n== Drift ==")
    drift = _rows(lake, "gold_rpt_lineage_drift")
    for d in drift:
        child = f" -> {d['child_name'] or d['child_id']}" if d.get("child_id") else ""
        out.append(f"  {d['parent_name']}{child}: {d['issue']}")
    if not drift:
        out.append("  none: the API and the XML agree, and every deployed process is on its latest version")
    cycles = [m["name"] for m in mart if m["in_cycle"]]
    if cycles:
        out.append("\n== Sub-process cycles ==\n  " + ", ".join(sorted(cycles)))
    return "\n".join(out)
