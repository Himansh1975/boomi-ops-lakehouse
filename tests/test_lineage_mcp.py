"""Lineage queries and the MCP server, on two saved graphs:
graph_live.json   = the trial account on 2026-11-18 (real run counts, pseudonymised editors)
graph_replay.json = the synthetic account after a 4-day replay (planted orphans, drift, cycle)
"""
import asyncio
import json
from pathlib import Path

import pytest

from boomi_ops.lineage.queries import Graph

FIX = Path(__file__).parent / "fixtures" / "lineage"


@pytest.fixture(scope="module")
def live():
    return Graph.load(FIX / "graph_live.json")


@pytest.fixture(scope="module")
def replay():
    return Graph.load(FIX / "graph_replay.json")


def test_blast_radius_on_the_trial_account(live):
    r = live.blast_radius("partner apis")
    assert r["component"]["name"] == "Partner APIs (httpbin)"
    assert (r["processes_affected"], r["deployed_processes"], r["runs"], r["failures"]) == (3, 3, 29, 8)
    assert [p["name"] for p in r["top_level_processes"]] == ["Vendor Invoice Poller", "Benefits API Push",
                                                             "Payroll Export"]


def test_blast_radius_goes_through_sub_processes(live):
    r = live.blast_radius("Write file")
    assert r["sub_processes"] == ["Common - Archive to Disk"]
    assert {p["name"] for p in r["top_level_processes"]} >= {"Employee Directory Export", "HR Employee Sync"}
    assert r["processes_affected"] == 6 and r["runs"] == 27


def test_ambiguous_and_unknown_names(live):
    assert "matches" in live.blast_radius("GET")
    assert live.blast_radius("no such thing")["error"].startswith("no component")
    assert "not a process" in live.shared_dependencies("Partner APIs (httpbin)")["error"]


def test_shared_outage_on_the_trial_account(live):
    """2026-11-18 06:00: jsonplaceholder.typicode.com was unreachable and both processes using it failed."""
    r = live.shared_dependencies("Customer Master Sync")
    top = r["shared_components"][0]
    assert r["failure_at"] == "2026-11-18T06:00Z"
    assert top["component"] == "JSONPlaceholder HTTP" and top["same_error"] == 1
    assert top["failed_at_same_time"][0]["name"] == "HR Employee Sync"
    assert "probably shared" in r["summary"] and "check 'JSONPlaceholder HTTP' first" in r["summary"]


@pytest.mark.parametrize("process", ["Payroll Export", "Benefits API Push", "Vendor Invoice Poller"])
def test_local_failures_on_the_trial_account(live, process):
    """Timeouts (Payroll), a 401 (Benefits) and the 503 window (Vendor) are each that process's own problem,
    even though all three share Partner APIs (httpbin)."""
    assert f"probably inside {process}" in live.shared_dependencies(process)["summary"]


def test_a_given_failure_time_is_used(live):
    r = live.shared_dependencies("Vendor Invoice Poller", at="2026-11-18T06:00Z", window_minutes=5)
    # at 06:00 the JSONPlaceholder outage hit other processes, but they share nothing with the Vendor poller
    assert r["failure_at"] == "2026-11-18T06:00Z" and "probably inside" in r["summary"]
    assert live.shared_dependencies("Contractor Directory Export")["summary"].endswith("Pass the failure time.")


def test_common_infrastructure_is_not_blamed(replay):
    r = replay.shared_dependencies("Benefits API Push")
    common = {c["component"] for c in r["common_infrastructure"]}
    assert {"Common - Error Handler", "Notification SMTP"} <= common
    assert not common & {s["component"] for s in r["shared_components"]}


def test_bad_deploy_is_called_local(replay):
    """Customer Master Sync's failures in the replay come from its own bad deploy (a code bug)."""
    assert "probably inside Customer Master Sync" in replay.shared_dependencies("Customer Master Sync")["summary"]


def test_health_lists_the_planted_cases(replay):
    h = replay.health()
    assert {o["name"] for o in h["orphans"]} >= {"GET legacy employees", "Old Employee (XML)"}
    assert {d["issue"] for d in h["drift"]} == {"points at a deleted component", "API lists it, XML doesn't use it"}
    assert h["cycles"] == ["Retry Worker", "Retry Wrapper"]


def test_mcp_server_exposes_the_tools(tmp_path):
    from boomi_ops.lineage.mcp_server import build_server

    graph = tmp_path / "lineage.json"
    graph.write_text((FIX / "graph_live.json").read_text(encoding="utf-8"), encoding="utf-8")
    server = build_server(graph)

    async def go():
        names = {t.name for t in await server.list_tools()}
        res = await server.call_tool("get_blast_radius", {"component": "Partner APIs (httpbin)"})
        return names, res

    names, res = asyncio.run(go())
    assert names == {"find_component", "get_blast_radius", "get_shared_dependencies", "get_lineage_health"}
    assert not res.is_error
    assert json.loads(res.content[0].text)["failures"] == 8


def test_mcp_server_picks_up_a_new_crawl(tmp_path):
    from boomi_ops.lineage.mcp_server import build_server

    graph = tmp_path / "lineage.json"
    data = json.loads((FIX / "graph_live.json").read_text(encoding="utf-8"))
    graph.write_text(json.dumps(data), encoding="utf-8")
    server = build_server(graph)
    call = lambda: asyncio.run(server.call_tool("find_component", {"query": "Brand New"}))  # noqa: E731
    assert json.loads(call().content[0].text)["total"] == 0
    data["nodes"].append({"id": "x", "name": "Brand New Connection", "kind": "connection"})
    graph.write_text(json.dumps(data), encoding="utf-8")
    import os
    os.utime(graph, (1, 2))  # force a different mtime even on coarse file systems
    assert json.loads(call().content[0].text)["total"] == 1
