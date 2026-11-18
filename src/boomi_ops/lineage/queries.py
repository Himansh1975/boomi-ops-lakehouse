"""Questions about the lineage graph, answered from lineage.json (written by `lineage viewer`).

Plain Python on purpose: the MCP server and the triage agent call these many times, and starting Spark
for each answer would take ~30 s. The heavy lifting (closure, run counts) already happened in gold.

The triage question this adds: "a run of process P failed: is the cause something P shares with other
processes?" Only failures at about the same time count (default: within an hour), because over a month every
busy process fails now and then for its own reasons. If others using the same connection failed in that
window, especially with the same error fingerprint, the shared part is the first suspect (upstream down,
expired credentials); if not, the problem is probably inside P.
"""
from __future__ import annotations

import json
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

RUN_WINDOW = "last 30 days"
DEFAULT_WINDOW_MINUTES = 60  # "at the same time" for the triage question
ONE_MIN = timedelta(minutes=1)
# A component used by most processes (an error-handler sub-process, the alert mail connection) is shared with
# every failing process, so it would top every list without telling us anything. Above this share of all
# top-level processes, in accounts with at least COMMON_MIN_PROCESSES of them, it's reported as common
# infrastructure instead of a suspect.
COMMON_SHARE = 0.75
COMMON_MIN_PROCESSES = 6


class Graph:
    def __init__(self, data: dict):
        self.data = data
        self.nodes = {n["id"]: n for n in data["nodes"]}
        self.children: dict[str, list[dict]] = {}
        self.parents: dict[str, list[str]] = {}
        for e in data["edges"]:
            self.children.setdefault(e["source"], []).append(e)
            self.parents.setdefault(e["target"], []).append(e["source"])

    @classmethod
    def load(cls, path: str | Path) -> "Graph":
        return cls(json.loads(Path(path).read_text(encoding="utf-8")))

    # ---- lookups ----------------------------------------------------------------------------------
    def find(self, query: str) -> list[dict]:
        q = query.strip().lower()
        real = [n for n in self.nodes.values() if n["kind"] != "deleted"]
        exact = [n for n in real if n["name"].lower() == q or n["id"] == q]
        return exact or sorted((n for n in real if q in n["name"].lower()), key=lambda n: n["name"])

    def _one(self, query: str) -> tuple[dict | None, dict | None]:
        hits = self.find(query)
        if len(hits) == 1:
            return hits[0], None
        if not hits:
            return None, {"error": f"no component matches '{query}'"}
        return None, {"error": f"'{query}' matches {len(hits)} components; use the full name",
                      "matches": [self._brief(n) for n in hits[:20]]}

    def _walk(self, start: str, up: bool) -> dict[str, int]:
        """Every node reachable from start (users if up, dependencies if not), with the shortest depth."""
        depth, todo = {}, deque([(start, 0)])
        while todo:
            node, d = todo.popleft()
            nxt = self.parents.get(node, []) if up else [e["target"] for e in self.children.get(node, [])]
            for n in nxt:
                if n not in depth and n != start:
                    depth[n] = d + 1
                    todo.append((n, d + 1))
        return depth

    @staticmethod
    def _brief(n: dict) -> dict:
        return {"name": n["name"], "kind": n["kind"], "id": n["id"]}

    def _process_stats(self, n: dict) -> dict:
        return {"name": n["name"], "deployed_version": n.get("deployedVersion"),
                "runs": n.get("runs", 0), "failures": n.get("failures", 0)}

    # ---- answers ----------------------------------------------------------------------------------
    def blast_radius(self, query: str) -> dict:
        n, err = self._one(query)
        if err:
            return err
        users = self._walk(n["id"], up=True)
        procs = [self.nodes[u] for u in users if self.nodes[u]["kind"] in ("process", "subprocess")]
        top = sorted((p for p in procs if p["kind"] == "process"), key=lambda p: (-p.get("runs", 0), p["name"]))
        uses = [{"name": self.nodes[e["target"]]["name"], "kind": self.nodes[e["target"]]["kind"],
                 "in_shapes": e["shapes"],
                 "drift": "deleted" if e["dangling"] else None if e["inApi"] == e["inXml"]
                 else ("API only (stale)" if e["inApi"] else "XML only")}
                for e in self.children.get(n["id"], [])]
        out = {"component": self._brief(n) | {"type": n["type"], "sub_type": n.get("subType"),
                                             "version": n.get("version"), "last_edited": n.get("modifiedAt"),
                                             "edited_by": n.get("modifiedBy")},
               "window": RUN_WINDOW,
               "processes_affected": len(procs), "deployed_processes": sum(1 for p in procs if p.get("deployedVersion")),
               "top_level_processes": [self._process_stats(p) for p in top],
               "sub_processes": sorted(p["name"] for p in procs if p["kind"] == "subprocess"),
               "runs": sum(p.get("runs", 0) for p in top), "failures": sum(p.get("failures", 0) for p in top),
               "uses": uses, "orphan": n.get("orphan"), "in_cycle": n.get("inCycle", False),
               "risk_score": n.get("risk")}
        if n["kind"] == "process" and n.get("isTopLevel"):
            out["self"] = self._process_stats(n)
        return out

    def shared_dependencies(self, process_query: str, at: str | None = None,
                            window_minutes: int = DEFAULT_WINDOW_MINUTES) -> dict:
        """For a failed run of a process: what it shares with other processes, and which of those failed
        at about the same time (within window_minutes of `at`, default the process's latest failure)."""
        p, err = self._one(process_query)
        if err:
            return err
        if p["kind"] not in ("process", "subprocess"):
            return {"error": f"'{p['name']}' is a {p['kind']}, not a process"}
        own = p.get("recentFailures") or []
        anchor = _parse(at) if at else (_parse(own[0]["at"]) if own else None)
        anchor_fp = next((f["fingerprint"] for f in own if anchor and abs(_parse(f["at"]) - anchor) <= ONE_MIN),
                         None)
        win = timedelta(minutes=window_minutes)

        def around(o: dict) -> list[dict]:
            if anchor is None:
                return []
            return [f for f in o.get("recentFailures") or [] if abs(_parse(f["at"]) - anchor) <= win]

        deps = self._walk(p["id"], up=False)
        n_top = sum(1 for n in self.nodes.values() if n["kind"] == "process")
        suspects, common = [], []
        for d, depth in deps.items():
            node = self.nodes[d]
            if node["kind"] == "deleted":
                continue
            others = [self.nodes[u] for u in self._walk(d, up=True)
                      if u != p["id"] and self.nodes[u]["kind"] == "process"]
            if not others:
                continue
            users_total = len(others) + (1 if p["kind"] == "process" else 0)
            if n_top >= COMMON_MIN_PROCESSES and users_total / n_top >= COMMON_SHARE:
                common.append({"component": node["name"], "kind": node["kind"], "used_by_processes": users_total})
                continue
            together = []
            for o in sorted(others, key=lambda o: o["name"]):
                hits = around(o)
                if hits:
                    together.append({"name": o["name"], "failures_in_window": len(hits),
                                     "same_error": bool(anchor_fp) and any(h["fingerprint"] == anchor_fp for h in hits),
                                     "example_error": hits[0]["error"]})
            suspects.append({
                "component": node["name"], "kind": node["kind"], "depth": depth,
                "shared_with": [self._process_stats(o) for o in sorted(others, key=lambda o: o["name"])],
                "failed_at_same_time": together,
                "same_time_share": round(len(together) / len(others), 2),
                "same_error": sum(1 for t in together if t["same_error"]),
            })
        kind_rank = {"connection": 0, "operation": 1, "subprocess": 2, "map": 3, "profile": 4}
        # most telling first: others failing with the same error, then the biggest share of other users
        # failing at the same time, then connections (outages and credentials live there) before the rest
        suspects.sort(key=lambda s: (-s["same_error"], -s["same_time_share"], kind_rank.get(s["kind"], 9),
                                     s["component"]))
        return {"process": self._process_stats(p) | {"kind": p["kind"]},
                "failure_at": anchor.strftime("%Y-%m-%dT%H:%MZ") if anchor else None,
                "window_minutes": window_minutes, "shared_components": suspects,
                "common_infrastructure": sorted(common, key=lambda c: c["component"]),
                "summary": triage_summary(p, suspects, anchor, window_minutes)}

    def health(self) -> dict:
        nodes = self.nodes.values()
        drift = [{"from": self.nodes[e["source"]]["name"],
                  "to": self.nodes.get(e["target"], {}).get("name", e["target"]),
                  "issue": "points at a deleted component" if e["dangling"]
                  else "API lists it, XML doesn't use it" if e["inApi"] else "XML uses it, API doesn't list it"}
                 for e in self.data["edges"] if e["dangling"] or e["inApi"] != e["inXml"]]
        hot = sorted((n for n in nodes if n["kind"] not in ("process", "deleted") and n.get("processesTotal", 0) > 0),
                     key=lambda n: (-(n.get("risk") or 0), n["name"]))[:10]
        return {"source": self.data["source"], "generated_at": self.data["generatedAt"],
                "components": sum(1 for n in nodes if n["kind"] != "deleted"), "links": len(self.data["edges"]),
                "orphans": [{"name": n["name"], "kind": n["kind"], "reason": n["orphan"]} for n in nodes if n.get("orphan")],
                "drift": drift, "cycles": sorted(n["name"] for n in nodes if n.get("inCycle")),
                "riskiest_shared": [{"name": n["name"], "kind": n["kind"], "processes": n["processesTotal"],
                                     "runs": n["runs"], "failures": n["failures"], "risk": n.get("risk")} for n in hot]}


def triage_summary(process: dict, suspects: list[dict], anchor: datetime | None, window_minutes: int) -> str:
    """One or two sentences a triage message can quote as is."""
    name = process["name"]
    if anchor is None:
        return (f"{name} has no recorded failure to compare against, so lineage can't tell a shared cause "
                "from a local one. Pass the failure time.")
    when = f"within {window_minutes} minutes of {anchor:%Y-%m-%d %H:%M} UTC"
    hit = next((s for s in suspects if s["failed_at_same_time"]), None)
    if hit:
        who = [t["name"] + (" (same error)" if t["same_error"] else "") for t in hit["failed_at_same_time"]]
        strength = "probably" if hit["same_error"] or len(who) >= 2 else "possibly"
        return (f"{_join(who)} also failed {when}, and {'they share' if len(who) > 1 else 'it shares'} "
                f"{hit['kind']} '{hit['component']}' with {name}. The cause is {strength} shared: "
                f"check '{hit['component']}' first.")
    if suspects:
        shared = sorted({s["component"] for s in suspects if s["kind"] == "connection"}) or [suspects[0]["component"]]
        return (f"No other process sharing components with {name} ({_join(shared)}) failed {when}, so the cause "
                f"is probably inside {name} (its data, mapping or script), not a shared connection.")
    return f"{name} shares no components with other processes, so the cause is inside {name}."


def _join(items: list[str]) -> str:
    return items[0] if len(items) == 1 else ", ".join(items[:-1]) + " and " + items[-1]


def _parse(text: str) -> datetime:
    return datetime.strptime(text.rstrip("Z"), "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)
