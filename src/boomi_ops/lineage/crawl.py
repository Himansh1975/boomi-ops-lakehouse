"""Crawl a Boomi account's components and references into landing files.

One crawl = one snapshot:
  1. ComponentMetadata/query (current, not deleted): every component, paged. Cheap, done every time.
  2. ComponentReference/query (parentComponentId + parentVersion) for each component **version not
     crawled before**. A version never changes once saved, so its links never change either: this is
     what makes the crawl incremental. A second crawl with no edits makes zero reference calls.
  3. GET /Component/{id}~{version} (XML) for the same new versions, parsed into edges, to check the API.

Why every component and not a walk from processes: references are one level deep (process -> map,
map -> profiles), so a walk has to visit maps and sub-processes anyway; visiting all components is as
cheap, finds orphans for free, and can't get stuck in a sub-process cycle.

Why we keep history ourselves: Boomi returns no references for old versions (seen 2026-11-18), so the
lineage of "Customer Master Sync v1" only exists if we stored it while v1 was current.

Privacy: createdBy/modifiedBy are user emails. They are replaced with a stable pseudonym (user-<hash>)
before anything is written, so the lake, fixtures and published viewer never hold an email.

Landing layout matches ingest.py, so the same bronze loader picks the files up:
  landing/<Object>/ingest_date=YYYY-MM-DD/<Object>-<snapshot>.jsonl
"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from ..client import BoomiApiError, BoomiClient
from ..timeutil import to_boomi
from .xml_edges import parse_xml_edges, redact_xml

OBJECTS = ("ComponentMetadata", "ComponentReference", "ComponentXmlEdge")
CURRENT_ONLY = {"QueryFilter": {"expression": {"operator": "and", "nestedExpression": [
    {"operator": "EQUALS", "property": "currentVersion", "argument": ["true"]},
    {"operator": "EQUALS", "property": "deleted", "argument": ["false"]},
]}}}


def pseudonym(email: str | None) -> str | None:
    if not email:
        return email
    return "user-" + hashlib.sha256(email.strip().lower().encode()).hexdigest()[:8]


def _flag(v) -> bool:
    """`currentVersion` / `deleted` arrive as JSON booleans or as "true"/"false" strings."""
    return v is True or str(v).lower() == "true"


def references_of(client: BoomiClient, component_id: str, version: int) -> list[dict]:
    q = {"QueryFilter": {"expression": {"operator": "and", "nestedExpression": [
        {"operator": "EQUALS", "property": "parentComponentId", "argument": [component_id]},
        {"operator": "EQUALS", "property": "parentVersion", "argument": [str(version)]},
    ]}}}
    # The query endpoint answers "no references" with 200 + empty result. (Only GET /ComponentReference/{id}
    # uses 400 for that, and also for unknown ids, which is why we never call it.)
    return [ref for res in client.query_all("ComponentReference", q) for ref in res.get("references", [])]


@dataclass
class CrawlResult:
    snapshot: str
    components: int
    new_versions: int
    api_edges: int
    xml_edges: int
    requests: int
    xml_failures: list[str] = field(default_factory=list)
    paths: dict[str, Path] = field(default_factory=dict)


def _state_path(landing: Path) -> Path:
    return landing / "_state" / "lineage_crawled.json"


def _write_jsonl(landing: Path, obj: str, snapshot: datetime, rows: list[dict]) -> Path:
    batch_id = f"{obj}-{snapshot:%Y%m%dT%H%M%SZ}"
    out = landing / obj / f"ingest_date={snapshot:%Y-%m-%d}" / f"{batch_id}.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp")
    ts = to_boomi(snapshot)
    with tmp.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps({"_batch_id": batch_id, "_object": obj, "_ingested_at": ts,
                                "_window_start": ts, "_window_end": ts,
                                "payload": json.dumps(r, sort_keys=True)}) + "\n")
    os.replace(tmp, out)
    return out


def crawl(client: BoomiClient, landing: Path, snapshot: datetime, fetch_xml: bool = True,
          xml_dir: Path | None = None) -> CrawlResult:
    landing = Path(landing)
    state_file = _state_path(landing)
    crawled: set[str] = set(json.loads(state_file.read_text())) if state_file.exists() else set()
    start_requests = client.requests_made

    components = []
    for c in client.query_all("ComponentMetadata", CURRENT_ONLY):
        if not _flag(c.get("currentVersion", True)) or _flag(c.get("deleted", False)):
            continue  # belt and braces: the filter should already do this
        c = dict(c, createdBy=pseudonym(c.get("createdBy")), modifiedBy=pseudonym(c.get("modifiedBy")))
        components.append(c)

    refs, xml_edges, failures, new_keys = [], [], [], []
    for c in components:
        cid, version = c["componentId"], int(c["version"])
        key = f"{cid}~{version}"
        if key in crawled:
            continue
        new_keys.append(key)
        for r in references_of(client, cid, version):
            refs.append({"parentComponentId": r.get("parentComponentId", cid),
                         "parentVersion": int(r.get("parentVersion", version)),
                         "componentId": r["componentId"], "type": r.get("type"),
                         "branchId": r.get("branchId")})
        if fetch_xml:
            try:
                xml = client.call("GET", f"/Component/{key}", None, "")
            except BoomiApiError as e:
                failures.append(f"{key}: HTTP {e.status}")
                continue
            clean = redact_xml(xml)
            if xml_dir:
                xml_dir.mkdir(parents=True, exist_ok=True)
                (xml_dir / f"{cid}~{version}.xml").write_text(clean, encoding="utf-8")
            for e in parse_xml_edges(clean, cid):
                xml_edges.append({"parentComponentId": cid, "parentVersion": version, "componentId": e["child_id"],
                                  "attr": e["attr"], "element": e["element"], "shapeName": e["shape_name"],
                                  "shapeLabel": e["shape_label"], "shapeType": e["shape_type"]})

    paths = {"ComponentMetadata": _write_jsonl(landing, "ComponentMetadata", snapshot, components)}
    if new_keys:
        paths["ComponentReference"] = _write_jsonl(landing, "ComponentReference", snapshot, refs)
        if fetch_xml:
            paths["ComponentXmlEdge"] = _write_jsonl(landing, "ComponentXmlEdge", snapshot, xml_edges)

    # state last: if we crash before this, the next crawl redoes these versions, which is harmless
    # (silver MERGEs on the edge key)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_file.with_suffix(".tmp")
    tmp.write_text(json.dumps(sorted(crawled | set(new_keys)), indent=0))
    os.replace(tmp, state_file)

    return CrawlResult(to_boomi(snapshot), len(components), len(new_keys), len(refs), len(xml_edges),
                       client.requests_made - start_requests, failures, paths)
