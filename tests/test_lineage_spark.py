"""Lineage bronze/silver on Spark + Delta. Need Java, so they run in Docker and CI (skipped elsewhere)."""
import shutil
from datetime import datetime, timedelta, timezone

import pytest

if not shutil.which("java"):
    pytest.skip("Spark tests need Java (run them in Docker)", allow_module_level=True)

from boomi_ops.client import BoomiClient  # noqa: E402
from boomi_ops.ingest import ingest  # noqa: E402
from boomi_ops.lineage import report  # noqa: E402
from boomi_ops.lineage.crawl import crawl  # noqa: E402
from boomi_ops.lineage.sim_account import DELETED_PROFILE_ID, SyntheticAccount  # noqa: E402
from boomi_ops.simulator import ReplayTransport, World  # noqa: E402
from boomi_ops.transform.lake import Lake, get_spark  # noqa: E402
from boomi_ops.transform.lineage import run_lineage_transforms  # noqa: E402
from boomi_ops.transform.lineage_gold import check_lineage_gold  # noqa: E402
from boomi_ops.transform.pipeline import run_transforms  # noqa: E402

START = datetime(2026, 11, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    root = tmp_path_factory.mktemp("lineage")
    w = World(START, 5)
    acct = SyntheticAccount(w.start, w._bad_deploy_at, w._rollback_at)
    clock = {"now": w.start}
    for comp in acct.deployable_extras():
        w.add_deployment(comp.id, comp.name)
    client = BoomiClient(ReplayTransport(w, lambda: clock["now"], account=acct), max_requests_per_second=0)
    lake = Lake(get_spark("tests-lineage"), root)
    stats = []
    # run history first (one big window), so the blast radius has runs to weigh
    clock["now"] = w.end + timedelta(hours=1)
    ingest(client, root / "landing", clock["now"], w.start - timedelta(days=1))
    run_transforms(lake)
    for day in (0, 2, 3):  # before the bad deploy, after it, after the rollback
        clock["now"] = w.start + timedelta(days=day, hours=23)
        crawl(client, root / "landing", clock["now"])
        stats.append(run_lineage_transforms(lake))
    return lake, acct, stats, clock["now"]


def count(lake, table):
    return lake.read(table).count()


def test_every_crawl_loaded_once(built):
    lake, _, stats, _ = built
    assert all(s["new_component_batches"] == 1 for s in stats)
    assert lake.read("bronze_component_metadata").select("_batch_id").distinct().count() == 3


def test_reloading_is_a_no_op(built):
    lake, _, _, _ = built
    before = {t: count(lake, t) for t in ("silver_component_versions", "silver_component_edges", "silver_components")}
    assert run_lineage_transforms(lake)["new_component_batches"] == 0
    assert {t: count(lake, t) for t in before} == before


def test_versions_keep_history_boomi_forgets(built):
    lake, acct, _, _ = built
    cms = acct.catalog["Customer Master Sync"].id
    formatter = acct.catalog["Customer Phone Formatter"].id
    versions = sorted(r.version for r in lake.read("silver_component_versions").filter(f"component_id = '{cms}'").collect())
    assert versions == [1, 2, 3]
    v2 = {r.child_id for r in lake.read("silver_component_edges")
          .filter(f"parent_id = '{cms}' AND parent_version = 2 AND source = 'api'").collect()}
    assert formatter in v2
    cur = lake.read("silver_components").filter(f"component_id = '{cms}'").first()
    assert cur.version == 3 and cur.versions_seen == 3 and cur.is_present


def test_api_and_xml_edges_both_land_with_shape_detail(built):
    lake, acct, _, _ = built
    e = lake.read("silver_component_edges")
    api = {(r.parent_id, r.parent_version, r.child_id) for r in e.filter("source = 'api'").collect()}
    xml = {(r.parent_id, r.parent_version, r.child_id) for r in e.filter("source = 'xml'").collect()}
    payroll = acct.catalog["Payroll Export SFTP"].id
    old_map = acct.catalog["Employee to Payroll Line (old)"].id
    assert api - xml == {(payroll, 1, old_map)}
    labels = {r.shape_label for r in e.filter(f"source = 'xml' AND parent_id = '{payroll}'").collect()}
    assert "Upload payroll file" in labels


def test_no_emails_in_silver(built):
    lake, _, _, _ = built
    users = {r.modified_by for r in lake.read("silver_component_versions").select("modified_by").distinct().collect()}
    assert users and all(u.startswith("user-") for u in users)


# ---- gold -----------------------------------------------------------------------------------------
def reference_graph(acct, now):
    """Plain-Python lineage of the synthetic account, to check Spark against."""
    meta = {m["componentId"]: m for m in acct.metadata(now)}
    edges = {(p, c) for p, m in meta.items() for src in ("api", "xml")
             for c in acct.children(acct.by_id[p], m["version"], src) if c in meta}
    kids = {}
    for p, c in edges:
        kids.setdefault(p, set()).add(c)
    depth = {}
    for a in meta:
        frontier, d, seen = {a}, 0, set()
        while frontier:
            d += 1
            frontier = {c for n in frontier for c in kids.get(n, ())} - seen
            for c in frontier:
                depth.setdefault((a, c), d)
            seen |= frontier
    return meta, depth


def test_closure_matches_plain_python(built):
    lake, acct, _, now = built
    _, expected = reference_graph(acct, now)
    got = {(r.ancestor_id, r.descendant_id): r.depth for r in lake.read("gold_fct_lineage_closure").collect()}
    assert got == expected


def test_blast_radius_counts_process_ancestors(built):
    lake, acct, _, now = built
    meta, depth = reference_graph(acct, now)
    mart = {r.component_id: r for r in lake.read("gold_mart_blast_radius").collect()}
    for cid in meta:
        users = {a for (a, d) in depth if d == cid and meta[a]["type"] == "process" and a != cid}
        assert mart[cid].processes_total == len(users), meta[cid]["name"]
    smtp = mart[acct.catalog["Notification SMTP"].id]
    assert smtp.processes_total >= 15 and smtp.deployed_processes >= 10


def test_runs_come_from_top_level_users(built):
    lake, acct, _, _ = built
    mart = {r.name: r for r in lake.read("gold_mart_blast_radius").collect()}
    top_runs = {n: r.runs_30d for n, r in mart.items() if r.type == "process" and r.is_top_level}
    assert top_runs["HR Employee Sync"] > 0 and top_runs["Department Sync"] == 0  # deployed, never runs
    for name in ("Common - Error Handler", "HR API (REST)", "Employee (JSON)"):
        m = mart[name]
        assert m.runs_30d == sum(top_runs.get(u, 0) for u in m.used_by), name
    assert mart["Common - Error Handler"].runs_30d == 0 or mart["Common - Error Handler"].risk_score > 0


def test_orphans_drift_and_cycles(built):
    lake, acct, _, _ = built
    orphans = {(r.name, r.reason) for r in lake.read("gold_rpt_orphans").collect()}
    assert orphans == {
        ("GET legacy employees", "nothing references it"), ("Scratch Profile (copy)", "nothing references it"),
        ("Employee to Old Employee", "nothing references it"),
        ("Customer Phone Formatter", "nothing references it"),
        ("Old Employee (XML)", "only used by other orphans"),
        ("Contractor Onboarding", "process never deployed"), ("Copy of HR Employee Sync", "process never deployed")}
    drift = {(r.parent_name, r.child_name or r.child_id, r.issue.split(" (")[0])
             for r in lake.read("gold_rpt_lineage_drift").collect()}
    assert drift == {("Payroll Export SFTP", "Employee to Payroll Line (old)", "listed by the API but not used in the XML"),
                     ("Benefits API Push", DELETED_PROFILE_ID, "references a component that no longer exists")}
    cycles = {r.name for r in lake.read("gold_mart_blast_radius").filter("in_cycle").collect()}
    assert cycles == {"Retry Wrapper", "Retry Worker"}


def test_data_checks_pass(built):
    lake, _, _, _ = built
    assert check_lineage_gold(lake) == []


def test_impact_report_reads_well(built):
    lake, _, _, _ = built
    text = report.impact(lake, "notification smtp")
    assert "Changing this affects" in text and "Common - Error Handler" in text and "Uses:" not in text
    assert "Several components match" in report.impact(lake, "Common")
    show = report.show(lake)
    assert "Orphans" in show and "Retry Wrapper" in show


def test_viewer_embeds_the_graph_without_emails(built, tmp_path):
    import json
    import re

    from boomi_ops.lineage.viewer import build_viewer

    lake, acct, _, _ = built
    html = build_viewer(lake, tmp_path / "index.html", "replay").read_text(encoding="utf-8")
    data = json.loads(re.search(r"const DATA = (\{.*?\});\n", html, re.S).group(1))
    mart = lake.read("gold_mart_blast_radius").count()
    assert len(data["nodes"]) == mart + 1  # plus one ghost node for the deleted profile
    assert {n["kind"] for n in data["nodes"]} >= {"process", "subprocess", "connection", "operation", "profile",
                                                  "map", "deleted"}
    smtp = next(n for n in data["nodes"] if n["name"] == "Notification SMTP")
    assert smtp["processesTotal"] >= 15 and smtp["runs"] > 0
    assert not re.search(r"[\w.]+@[\w.]+\.\w+", html.split("<script>")[-1])
