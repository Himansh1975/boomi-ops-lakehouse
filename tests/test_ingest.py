import json
from datetime import datetime, timedelta, timezone

from boomi_ops.client import BoomiClient
from boomi_ops.ingest import ingest, load_state
from boomi_ops.simulator import ReplayTransport, World

START = datetime(2026, 11, 1, tzinfo=timezone.utc)


def setup():
    w = World(START, 1)
    clock = {"now": START}
    c = BoomiClient(ReplayTransport(w, lambda: clock["now"]), max_requests_per_second=0)
    return w, clock, c


def test_windows_overlap_by_the_lookback(tmp_path):
    _, clock, c = setup()
    for hour in (1, 2):
        clock["now"] = START + timedelta(hours=hour)
        res = ingest(c, tmp_path, clock["now"], START, lookback=timedelta(minutes=30))
    ex = res[0]
    assert ex.window_start == "2026-11-01T00:30:00Z" and ex.window_end == "2026-11-01T02:00:00Z"
    assert load_state(tmp_path)["ExecutionRecord"] == "2026-11-01T02:00:00Z"


def test_rerunning_a_window_rewrites_the_same_file(tmp_path):
    _, clock, c = setup()
    clock["now"] = START + timedelta(hours=3)
    first = ingest(c, tmp_path, clock["now"], START)[0]
    (tmp_path / "_state" / "watermarks.json").unlink()  # simulate a crash before the watermark save
    second = ingest(c, tmp_path, clock["now"], START)[0]
    assert first.path == second.path
    assert len(list((tmp_path / "ExecutionRecord").rglob("*.jsonl"))) == 1


def test_landing_rows_keep_raw_payload(tmp_path):
    w, clock, c = setup()
    clock["now"] = START + timedelta(hours=1)
    res = ingest(c, tmp_path, clock["now"], START)[0]
    line = json.loads(res.path.read_text().splitlines()[0])
    assert set(line) == {"_batch_id", "_object", "_ingested_at", "_window_start", "_window_end", "payload"}
    payload = json.loads(line["payload"])
    assert payload["executionId"] in w.by_id
