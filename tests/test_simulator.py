from datetime import datetime, timedelta, timezone

from boomi_ops.client import BoomiClient, between
from boomi_ops.simulator import ReplayTransport, World
from boomi_ops.timeutil import to_boomi

START = datetime(2026, 11, 1, tzinfo=timezone.utc)


def test_deterministic():
    a, b = World(START, 2), World(START, 2)
    assert [e.record for e in a.executions] == [e.record for e in b.executions]


def test_every_failure_has_a_category():
    w = World(START, 5)
    cats = {g["category"] for g in w.ground_truth()}
    assert {"auth", "data_quality", "timeout", "upstream_down", "code_bug"} <= cats
    assert all(g["status"] in ("ERROR", "ABORTED") for g in w.ground_truth())


def test_numbers_use_the_documented_long_array_form():
    rec = World(START, 1).executions[0].record
    assert rec["executionDuration"][0] == "Long" and isinstance(rec["executionDuration"][1], int)


def test_running_runs_show_as_started_then_finish():
    w = World(START, 1)
    e = max(w.executions, key=lambda x: (x.end - x.start))
    mid = e.start + (e.end - e.start) / 2
    seen_mid = {r["executionId"]: r for r in w.visible_executions(mid)}
    assert seen_mid[e.record["executionId"]]["status"] == "STARTED"
    seen_after = {r["executionId"]: r for r in w.visible_executions(e.end)}
    assert seen_after[e.record["executionId"]]["status"] == e.record["status"]


def test_bad_deploy_causes_code_bugs_only_between_deploy_and_rollback():
    w = World(START, 5)
    bugs = [x for x in w.executions if x.category == "code_bug"]
    assert bugs
    assert all(w._bad_deploy_at <= x.start < w._rollback_at for x in bugs)


def test_transport_pages_by_100_and_filters_by_window():
    w = World(START, 1)
    now = START + timedelta(days=1)
    c = BoomiClient(ReplayTransport(w, lambda: now, busy_every=3), base_delay=0,
                    max_requests_per_second=0, sleep=lambda s: None)
    rows = c.query_all("ExecutionRecord", between("executionTime", to_boomi(START), to_boomi(now)))
    assert len(rows) == len(w.executions) > 100
    assert c.retries_made > 0
    first_hour = c.query_all("ExecutionRecord", between(
        "executionTime", to_boomi(START), to_boomi(START + timedelta(hours=1))))
    assert 0 < len(first_hour) < len(rows)


def test_process_log_download():
    w = World(START, 1)
    exec_id = w.executions[0].record["executionId"]
    c = BoomiClient(ReplayTransport(w, lambda: START + timedelta(days=1)), max_requests_per_second=0)
    url = c.request_process_log(exec_id)
    assert exec_id in c.download(url).decode()
