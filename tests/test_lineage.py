"""Lineage crawler, XML parser and synthetic account. Fixtures are real responses from the trial account
(2026-11-18, username replaced), so these tests pin the behaviour we actually saw."""
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from boomi_ops.client import BoomiClient
from boomi_ops.lineage.crawl import crawl, pseudonym
from boomi_ops.lineage.sim_account import DELETED_PROFILE_ID, SyntheticAccount
from boomi_ops.lineage.xml_edges import parse_xml_edges, redact_xml
from boomi_ops.simulator import PROCESSES, ReplayTransport, World

FIX = Path(__file__).parent / "fixtures" / "lineage"
NOW = datetime(2026, 11, 18, 12, tzinfo=timezone.utc)
PAYROLL = "175959a6-b737-4491-835b-a1134e636e99"
PARTNER_APIS = "7ec93641-a9a5-494e-a546-d58b15d8a577"
GET_PAYROLL_ACK = "f6a47e8a-3438-4a3f-bd6f-4637b4a4f9b9"


def load(name):
    return json.loads((FIX / name).read_text(encoding="utf-8"))


def fixture_xml() -> dict[str, str]:
    out = {}
    for f in FIX.glob("component_*.xml"):
        text = f.read_text(encoding="utf-8")
        out[ET.fromstring(text).get("componentId")] = text
    return out


def api_children(refs_body: dict) -> set[str]:
    return {r["componentId"] for res in refs_body.get("result", []) for r in res.get("references", [])}


class FixtureTransport:
    """Answers the three lineage calls from the saved trial responses."""

    def __init__(self):
        self.meta = load("component_metadata_query.json")
        self.refs = load("refs_downstream_by_component.json")
        self.xml = fixture_xml()
        self.paths = []

    def __call__(self, method, path, body, content_type):
        self.paths.append(path)
        if path == "/ComponentMetadata/query":
            return 200, self.meta
        if path == "/ComponentReference/query":
            props = {e["property"]: e["argument"][0] for e in body["QueryFilter"]["expression"]["nestedExpression"]}
            return 200, self.refs.get(props["parentComponentId"], {"result": [], "numberOfResults": 0})
        if method == "GET" and path.startswith("/Component/"):
            x = self.xml.get(path.split("/")[2].split("~")[0])
            return (200, x.encode()) if x else (400, {"message": "not in fixtures"})
        return 410, {"message": "unexpected call " + path}


# ---- XML parser ---------------------------------------------------------------------------------
def test_xml_edges_of_a_connector_shape():
    edges = parse_xml_edges((FIX / "component_payroll_export.xml").read_text(), PAYROLL)
    assert {e["child_id"] for e in edges} == {PARTNER_APIS, GET_PAYROLL_ACK}
    assert {e["shape_label"] for e in edges} == {"Send to payroll provider"}
    assert {e["attr"] for e in edges} == {"connectionId", "operationId"}


@pytest.mark.parametrize("component_id", sorted(fixture_xml()))
def test_xml_and_api_agree_on_the_live_account(component_id):
    """The finding from step 1, kept as a regression test: both sources give the same links."""
    xml_children = {e["child_id"] for e in parse_xml_edges(fixture_xml()[component_id], component_id)}
    assert xml_children == api_children(load("refs_downstream_by_component.json")[component_id])


def test_xml_covers_maps_and_sub_processes():
    xml = fixture_xml()
    attrs = {e["attr"] for cid, x in xml.items() for e in parse_xml_edges(x, cid)}
    assert {"mapId", "processId", "fromProfile", "toProfile"} <= attrs


def test_redact_xml_drops_user_attributes_and_secrets():
    xml = ('<bns:Component xmlns:bns="http://api.platform.boomi.com/" createdBy="a@b.com" modifiedBy="c@d.com" '
           'name="x"><bns:encryptedValues><bns:encryptedValue path="//pw" isSet="true"/></bns:encryptedValues>'
           '</bns:Component>')
    out = redact_xml(xml)
    assert "@" not in out and "encryptedValue " not in out and 'name="x"' in out


# ---- crawler on the trial-account fixtures ------------------------------------------------------
def fixture_client():
    t = FixtureTransport()
    return t, BoomiClient(t, max_requests_per_second=0)


def landing_rows(path: Path) -> list[dict]:
    return [json.loads(json.loads(line)["payload"]) for line in path.read_text().splitlines()]


def test_crawl_finds_every_component_and_link(tmp_path):
    _, client = fixture_client()
    res = crawl(client, tmp_path, NOW, fetch_xml=False)
    assert (res.components, res.new_versions, res.api_edges) == (21, 21, 28)
    refs = landing_rows(res.paths["ComponentReference"])
    assert {(r["parentComponentId"], r["componentId"]) for r in refs} >= {(PAYROLL, PARTNER_APIS)}


def test_crawl_cross_checks_xml_and_reports_missing_bodies(tmp_path):
    _, client = fixture_client()
    res = crawl(client, tmp_path, NOW, xml_dir=tmp_path / "xml")
    assert len(res.xml_failures) == 21 - len(fixture_xml())  # only some XML bodies are saved as fixtures
    assert all("HTTP 400" in f for f in res.xml_failures)
    assert len(list((tmp_path / "xml").glob("*.xml"))) == len(fixture_xml())


def test_second_crawl_without_edits_makes_one_call(tmp_path):
    t, client = fixture_client()
    crawl(client, tmp_path, NOW)
    t.paths.clear()
    res = crawl(client, tmp_path, NOW + timedelta(days=1))
    assert res.new_versions == 0 and t.paths == ["/ComponentMetadata/query"]
    assert "ComponentReference" not in res.paths  # no empty batch file


def test_no_email_reaches_landing(tmp_path):
    _, client = fixture_client()
    crawl(client, tmp_path, NOW, xml_dir=tmp_path / "xml")
    text = "".join(f.read_text(encoding="utf-8") for f in tmp_path.rglob("*") if f.is_file())
    assert not re.search(r"[\w.]+@[\w.]+\.\w+", text)
    assert pseudonym("Demo.User@example.com ") == pseudonym("demo.user@example.com")


# ---- synthetic account --------------------------------------------------------------------------
START = datetime(2026, 11, 1, tzinfo=timezone.utc)


def sim(days=5):
    w = World(START, days)
    acct = SyntheticAccount(w.start, w._bad_deploy_at, w._rollback_at)
    clock = {"now": w.end}
    client = BoomiClient(ReplayTransport(w, lambda: clock["now"], account=acct, busy_every=5),
                         base_delay=0, max_requests_per_second=0)
    return w, acct, clock, client


def graph(acct, now, source="api"):
    meta = {m["componentId"]: m for m in acct.metadata(now)}
    edges = {(p, c) for p, m in meta.items() for c in acct.children(acct.by_id[p], m["version"], source)}
    return meta, edges


def test_run_simulator_processes_have_the_same_ids():
    _, acct, _, _ = sim()
    for p in PROCESSES:
        assert acct.catalog[p.name].id == p.id and acct.catalog[p.name].type == "process"


def test_planted_cases_are_there():
    w, acct, _, _ = sim()
    meta, edges = graph(acct, w.end)
    name = {i: m["name"] for i, m in meta.items()}
    has_parent = {c for _, c in edges}
    orphans = {name[i] for i, m in meta.items() if m["type"] != "process" and i not in has_parent}
    assert orphans == {"GET legacy employees", "Scratch Profile (copy)", "Employee to Old Employee",
                       "Customer Phone Formatter"}  # the last one became an orphan at the rollback
    by = {m["name"]: i for i, m in meta.items()}
    assert (by["Retry Wrapper"], by["Retry Worker"]) in edges and (by["Retry Worker"], by["Retry Wrapper"]) in edges
    assert DELETED_PROFILE_ID not in meta and (by["Benefits API Push"], DELETED_PROFILE_ID) in edges
    _, xml_edges = graph(acct, w.end, "xml")
    assert edges - xml_edges == {(by["Payroll Export SFTP"], by["Employee to Payroll Line (old)"])}
    assert not xml_edges - edges
    assert 60 <= len(meta) <= 120


def test_synthetic_xml_parses_to_the_xml_view():
    w, acct, _, _ = sim()
    for cid, m in {m["componentId"]: m for m in acct.metadata(w.end)}.items():
        parsed = {e["child_id"] for e in parse_xml_edges(acct.xml(cid, m["version"]), cid)}
        assert parsed == set(acct.children(acct.by_id[cid], m["version"], "xml")), m["name"]


def test_crawls_over_time_keep_the_bad_deploy_lineage(tmp_path):
    w, acct, clock, client = sim()
    cms = acct.catalog["Customer Master Sync"].id
    formatter = acct.catalog["Customer Phone Formatter"].id
    seen = {}
    for day in range(5):
        clock["now"] = w.start + timedelta(days=day, hours=23)
        res = crawl(client, tmp_path, clock["now"])
        if "ComponentReference" in res.paths:
            for r in landing_rows(res.paths["ComponentReference"]):
                if r["parentComponentId"] == cms:
                    seen.setdefault(r["parentVersion"], set()).add(r["componentId"])
    assert sorted(seen) == [1, 2, 3]
    assert formatter in seen[2] and formatter not in seen[1] | seen[3]
    assert client.retries_made > 0  # busy_every made some calls answer 503
    # Boomi itself no longer knows v2's links:
    assert acct.references(cms, 2, w.end) == []


def test_viewer_data_cannot_break_out_of_the_script_tag():
    from boomi_ops.lineage.viewer import render

    html = render({"source": "t", "generatedAt": "now", "nodes": [{"name": "</script><b>x"}], "edges": []})
    assert "</script><b>" not in html and "/*__LINEAGE_DATA__*/" not in html
