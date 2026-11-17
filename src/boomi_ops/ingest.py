"""Incremental pull from the Platform API into landing files.

Each object type keeps a watermark: the end of the last window it pulled. The next pull covers
[watermark - lookback, now]. The overlap catches runs that were still running (STARTED) last time and have
finished since; the duplicates it creates are removed later by the MERGE on executionId.

Crash safety: the landing file is written first (temp file + rename), the watermark last. If the job
dies in between, the next run repeats the window, which is harmless.

Idempotency: the batch id comes from the window end, so re-running the same window rewrites the
same file instead of adding a new one.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from .client import BoomiClient, between
from .timeutil import from_boomi, to_boomi

# object type -> the timestamp property the window filters on
OBJECTS = {
    "ExecutionRecord": "executionTime",
    "DeployedPackage": "deployedDate",
}
DEFAULT_LOOKBACK = timedelta(hours=2)


@dataclass
class BatchResult:
    object_type: str
    batch_id: str
    window_start: str
    window_end: str
    rows: int
    path: Path


def _state_path(landing: Path) -> Path:
    return landing / "_state" / "watermarks.json"


def load_state(landing: Path) -> dict:
    p = _state_path(landing)
    return json.loads(p.read_text()) if p.exists() else {}


def _save_state(landing: Path, state: dict) -> None:
    p = _state_path(landing)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True))
    os.replace(tmp, p)


def ingest(
    client: BoomiClient,
    landing: Path,
    now: datetime,
    first_window_start: datetime,
    lookback: timedelta = DEFAULT_LOOKBACK,
    objects: dict[str, str] = OBJECTS,
) -> list[BatchResult]:
    landing = Path(landing)
    state = load_state(landing)
    results = []
    for obj, prop in objects.items():
        wm = state.get(obj)
        start = (from_boomi(wm) - lookback) if wm else first_window_start
        start = max(start, first_window_start)
        end = now
        rows = client.query_all(obj, between(prop, to_boomi(start), to_boomi(end)))

        batch_id = f"{obj}-{end:%Y%m%dT%H%M%SZ}"
        out = landing / obj / f"ingest_date={end:%Y-%m-%d}" / f"{batch_id}.jsonl"
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps({
                    "_batch_id": batch_id,
                    "_object": obj,
                    "_ingested_at": to_boomi(end),
                    "_window_start": to_boomi(start),
                    "_window_end": to_boomi(end),
                    "payload": json.dumps(r, sort_keys=True),  # exactly as received, parsed in silver
                }) + "\n")
        os.replace(tmp, out)

        state[obj] = to_boomi(end)
        _save_state(landing, state)
        results.append(BatchResult(obj, batch_id, to_boomi(start), to_boomi(end), len(rows), out))
    return results
