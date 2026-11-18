"""A synthetic Boomi account for replay mode: components, references and XML that hang together.

The six processes that the run simulator executes (simulator.PROCESSES) are here with the *same ids*,
so lineage joins to run history. Around them sits a believable account: shared connections, operations,
profiles, maps, sub-processes, and a few processes the run simulator doesn't execute.

Planted cases (each one is a test):
  * orphans: an unused operation and profile, plus an orphan *chain* (an unused map whose profile is only
    used by that map)
  * a sub-process cycle: Retry Wrapper -> Retry Worker -> Retry Wrapper
  * a dangling reference: Benefits API Push still points at a deleted profile
  * API/XML drift: the API still lists a map that Payroll Export SFTP's XML no longer uses
  * version history: Customer Master Sync v2 (the bad deploy) adds a map that v3 (the rollback) drops,
    which leaves that map orphaned
  * never deployed: Contractor Onboarding and Copy of HR Employee Sync

Like a live account (seen 2026-11-18), references exist only for a component's current version.
Everything is deterministic.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from xml.sax.saxutils import quoteattr

from ..simulator import PROCESSES, _pid
from ..timeutil import to_boomi

FOLDERS = {"#Shared Connections": "Rjo1MDAwMDE=", "#Common": "Rjo1MDAwMDI=", "HR": "Rjo1MDAwMDM=",
           "Payroll": "Rjo1MDAwMDQ=", "Finance": "Rjo1MDAwMDU=", "CRM": "Rjo1MDAwMDY=", "Sandbox": "Rjo1MDAwMDc="}
USERS = ["dev.user@example.com", "integration.lead@example.com", "contractor.dev@example.com"]


def cid(ctype: str, name: str) -> str:
    """Processes use the run simulator's ids so executions join; everything else gets its own."""
    return _pid(name) if ctype == "process" else _pid(f"{ctype}/{name}")


@dataclass
class Comp:
    name: str
    type: str
    folder: str
    sub_type: str = ""
    shapes: list[tuple] = field(default_factory=list)  # processes: ("connector", conn, op, label) | ("map", m, label) | ("call", proc, label)
    map_profiles: tuple[str, str] | None = None         # maps: (from profile, to profile)

    @property
    def id(self) -> str:
        return cid(self.type, self.name)


def _catalog() -> dict[str, Comp]:
    c: dict[str, Comp] = {}

    def add(comp: Comp) -> Comp:
        c[comp.name] = comp
        return comp

    conns = {"HR API (REST)": "http", "Benefits Vendor REST": "http", "Payroll SFTP": "sftp",
             "Vendor API (REST)": "http", "CRM REST": "http", "Local Archive Disk": "disk-sdk",
             "Data Warehouse DB": "database", "Notification SMTP": "mail"}
    for n, st in conns.items():
        add(Comp(n, "connector-settings", "#Shared Connections", st))
    ops = {"GET employees": "HR API (REST)", "GET departments": "HR API (REST)", "GET positions": "HR API (REST)",
           "POST token": "HR API (REST)", "GET legacy employees": "HR API (REST)",
           "POST enrollments": "Benefits Vendor REST", "GET plans": "Benefits Vendor REST",
           "PUT payroll file": "Payroll SFTP", "GET payroll ack file": "Payroll SFTP",
           "GET timesheet file": "Payroll SFTP",
           "Listen invoices": "Vendor API (REST)", "POST invoice status": "Vendor API (REST)",
           "GET customers": "CRM REST", "UPSERT contacts": "CRM REST",
           "Write archive file": "Local Archive Disk",
           "INSERT staging rows": "Data Warehouse DB", "MERGE dimension": "Data Warehouse DB",
           "Send alert email": "Notification SMTP"}
    for n, conn in ops.items():
        add(Comp(n, "connector-action", "#Shared Connections", conns[conn]))
    profiles = {"Employee (JSON)": "profile.json", "Department (JSON)": "profile.json",
                "Position (JSON)": "profile.json", "Benefit Enrollment (JSON)": "profile.json",
                "Payroll Line (Flat File)": "profile.flatfile", "Timesheet Row (Flat File)": "profile.flatfile",
                "Vendor Invoice (XML)": "profile.xml", "Customer (JSON)": "profile.json",
                "CRM Contact (JSON)": "profile.json", "DW Employee Row (DB)": "profile.db",
                "DW Invoice Row (DB)": "profile.db", "Old Employee (XML)": "profile.xml",
                "Scratch Profile (copy)": "profile.json"}
    for n, t in profiles.items():
        add(Comp(n, t, "#Common"))
    maps = {"Employee to DW Employee Row": ("Employee (JSON)", "DW Employee Row (DB)"),
            "Department to DW Employee Row": ("Department (JSON)", "DW Employee Row (DB)"),
            "Position to DW Employee Row": ("Position (JSON)", "DW Employee Row (DB)"),
            "Employee to Benefit Enrollment": ("Employee (JSON)", "Benefit Enrollment (JSON)"),
            "Employee to Payroll Line": ("Employee (JSON)", "Payroll Line (Flat File)"),
            "Employee to Payroll Line (old)": ("Employee (JSON)", "Payroll Line (Flat File)"),
            "Timesheet Row to Payroll Line": ("Timesheet Row (Flat File)", "Payroll Line (Flat File)"),
            "Vendor Invoice to DW Invoice Row": ("Vendor Invoice (XML)", "DW Invoice Row (DB)"),
            "Customer to CRM Contact": ("Customer (JSON)", "CRM Contact (JSON)"),
            "Customer Phone Formatter": ("Customer (JSON)", "CRM Contact (JSON)"),
            "Employee to Old Employee": ("Employee (JSON)", "Old Employee (XML)")}
    for n, (a, b) in maps.items():
        add(Comp(n, "transform.map", "#Common", map_profiles=(a, b)))

    def conn(op: str, label: str) -> tuple:
        return ("connector", ops[op], op, label)

    err = ("call", "Common - Error Handler", "Handle errors")
    arc = ("call", "Common - Archive to Disk", "Archive payload")
    oauth = ("call", "Common - Get OAuth Token", "Get token")
    add(Comp("Common - Error Handler", "process", "#Common", shapes=[conn("Send alert email", "Email the on-call")]))
    add(Comp("Common - Archive to Disk", "process", "#Common", shapes=[conn("Write archive file", "Write archive")]))
    add(Comp("Common - Get OAuth Token", "process", "#Common", shapes=[conn("POST token", "Request token")]))
    add(Comp("Retry Wrapper", "process", "#Common",
             shapes=[("call", "Retry Worker", "Try once"), err]))
    add(Comp("Retry Worker", "process", "#Common",
             shapes=[conn("POST invoice status", "Send status"), ("call", "Retry Wrapper", "Retry on failure")]))

    names = {p.name for p in PROCESSES}
    assert names == {"HR Employee Sync", "Benefits API Push", "Timesheet Import", "Payroll Export SFTP",
                     "Vendor Invoice Listener", "Customer Master Sync"}, "keep in step with simulator.PROCESSES"
    add(Comp("HR Employee Sync", "process", "HR", shapes=[
        oauth, conn("GET employees", "Get employees"), ("map", "Employee to DW Employee Row", "Map to DW row"),
        conn("MERGE dimension", "Merge into dim_employee"), arc, err]))
    add(Comp("Benefits API Push", "process", "HR", shapes=[
        conn("GET employees", "Get employees"), ("map", "Employee to Benefit Enrollment", "Map to enrollment"),
        conn("POST enrollments", "Push enrollments"), err]))
    add(Comp("Timesheet Import", "process", "Payroll", shapes=[
        conn("GET timesheet file", "Pick up timesheet file"),
        ("map", "Timesheet Row to Payroll Line", "Map to payroll line"),
        conn("INSERT staging rows", "Load staging"), arc, err]))
    add(Comp("Payroll Export SFTP", "process", "Payroll", shapes=[
        conn("GET employees", "Get employees"), ("map", "Employee to Payroll Line", "Map to payroll line"),
        conn("PUT payroll file", "Upload payroll file"), arc, err]))
    add(Comp("Vendor Invoice Listener", "process", "Finance", shapes=[
        conn("Listen invoices", "Receive invoice"), ("map", "Vendor Invoice to DW Invoice Row", "Map to DW row"),
        conn("INSERT staging rows", "Load staging"), ("call", "Retry Wrapper", "Acknowledge with retry"), err]))
    add(Comp("Customer Master Sync", "process", "CRM", shapes=[
        conn("GET customers", "Get customers"), ("map", "Customer to CRM Contact", "Map to contact"),
        conn("UPSERT contacts", "Upsert contacts"), arc, err]))
    # processes the run simulator does not execute
    add(Comp("Department Sync", "process", "HR", shapes=[
        oauth, conn("GET departments", "Get departments"), ("map", "Department to DW Employee Row", "Map"),
        conn("MERGE dimension", "Merge"), err]))
    add(Comp("Position Sync", "process", "HR", shapes=[
        oauth, conn("GET positions", "Get positions"), ("map", "Position to DW Employee Row", "Map"),
        conn("MERGE dimension", "Merge"), err]))
    add(Comp("Benefits Plan Refresh", "process", "HR", shapes=[
        conn("GET plans", "Get plans"), conn("INSERT staging rows", "Load staging"), err]))
    add(Comp("Payroll Ack Poller", "process", "Payroll", shapes=[
        conn("GET payroll ack file", "Pick up ack file"), conn("INSERT staging rows", "Load staging"), err]))
    add(Comp("Vendor Invoice Status Push", "process", "Finance", shapes=[
        ("call", "Retry Wrapper", "Send status with retry"), err]))
    add(Comp("Nightly DW Reconciliation", "process", "Finance", shapes=[
        conn("INSERT staging rows", "Load counts"), conn("MERGE dimension", "Fix differences"), arc, err]))
    add(Comp("Employee Directory Export", "process", "HR", shapes=[
        oauth, conn("GET employees", "Get employees"), arc]))
    add(Comp("Contractor Onboarding", "process", "Sandbox", shapes=[
        conn("GET employees", "Get employees"), conn("UPSERT contacts", "Create CRM contact"), err]))
    add(Comp("Copy of HR Employee Sync", "process", "Sandbox", shapes=[
        oauth, conn("GET employees", "Get employees"), ("map", "Employee to DW Employee Row", "Map to DW row"),
        conn("MERGE dimension", "Merge into dim_employee")]))
    return c


# a profile that was deleted while Benefits API Push still points at it
DELETED_PROFILE_ID = cid("profile.json", "Benefit Plan (deleted)")
NEVER_DEPLOYED = ("Contractor Onboarding", "Copy of HR Employee Sync")


class SyntheticAccount:
    def __init__(self, start: datetime, bad_deploy_at: datetime | None = None,
                 rollback_at: datetime | None = None):
        self.catalog = _catalog()
        self.by_id = {c.id: c for c in self.catalog.values()}
        self.created = start - timedelta(days=60)
        self.v1_at = start - timedelta(days=1) + timedelta(hours=9)  # the simulator's initial deploy
        self.bad_deploy_at = bad_deploy_at
        self.rollback_at = rollback_at

    def deployable_extras(self) -> list[Comp]:
        """Top-level processes the run simulator doesn't execute but that are deployed (not sub-processes,
        not the never-deployed ones). Lets replay mode show 'deployed but no runs in 30 days'."""
        run = {p.name for p in PROCESSES}
        return [c for c in self.catalog.values() if c.type == "process" and c.folder != "#Common"
                and c.name not in run and c.name not in NEVER_DEPLOYED]

    # ---- versions -----------------------------------------------------------------------------
    def _versions(self, comp: Comp) -> list[tuple[int, datetime]]:
        out = [(1, self.v1_at)]
        if comp.name == "Customer Master Sync":
            if self.bad_deploy_at:
                out.append((2, self.bad_deploy_at))
            if self.rollback_at:
                out.append((3, self.rollback_at))
        return out

    def current_version(self, comp: Comp, now: datetime) -> tuple[int, datetime] | None:
        live = [v for v in self._versions(comp) if v[1] <= now]
        return live[-1] if live else None

    def shapes(self, comp: Comp, version: int) -> list[tuple]:
        if comp.name == "Customer Master Sync" and version == 2:  # the bad deploy adds a phone formatter
            s = list(comp.shapes)
            s.insert(2, ("map", "Customer Phone Formatter", "Format phone numbers"))
            return s
        return comp.shapes

    def children(self, comp: Comp, version: int, source: str) -> list[str]:
        """Child ids as the API (source='api') or the XML (source='xml') shows them."""
        ids: list[str] = []
        if comp.map_profiles:
            ids = [self.catalog[p].id for p in comp.map_profiles]
        for shape in self.shapes(comp, version):
            names = shape[1:3] if shape[0] == "connector" else shape[1:2]
            ids += [self.catalog[n].id for n in names]
        if comp.name == "Benefits API Push":
            ids.append(DELETED_PROFILE_ID)
        if comp.name == "Payroll Export SFTP" and source == "api":
            ids.append(self.catalog["Employee to Payroll Line (old)"].id)  # stale API reference
        return list(dict.fromkeys(ids))

    # ---- what the API returns -----------------------------------------------------------------
    def metadata(self, now: datetime) -> list[dict]:
        rows = []
        for i, comp in enumerate(sorted(self.catalog.values(), key=lambda c: (c.type, c.name))):
            cur = self.current_version(comp, now)
            if not cur:
                continue
            version, modified = cur
            flag = (lambda b: "true" if b else "false") if comp.type == "connector-settings" else (lambda b: b)
            rows.append({
                "@type": "ComponentMetadata", "componentId": comp.id, "version": version, "name": comp.name,
                "type": comp.type, "subType": comp.sub_type, "folderName": comp.folder,
                "folderId": FOLDERS[comp.folder], "createdDate": to_boomi(self.created + timedelta(hours=i)),
                "createdBy": USERS[i % len(USERS)], "modifiedDate": to_boomi(modified),
                "modifiedBy": USERS[(i + version) % len(USERS)],
                "deleted": flag(False), "currentVersion": flag(True),
            })
        return rows

    def references(self, component_id: str, version: int, now: datetime) -> list[dict]:
        comp = self.by_id.get(component_id)
        cur = comp and self.current_version(comp, now)
        if not cur or cur[0] != version:
            return []  # like a live account: old versions keep no references
        refs = [{"@type": "references", "parentComponentId": component_id, "parentVersion": version,
                 "componentId": child, "type": "DEPENDENT"} for child in self.children(comp, version, "api")]
        return [{"@type": "ComponentReference", "references": refs}] if refs else []

    def xml(self, component_id: str, version: int) -> str | None:
        comp = self.by_id.get(component_id)
        if not comp or version not in [v for v, _ in self._versions(comp)]:
            return None
        if comp.map_profiles:
            a, b = (self.catalog[p].id for p in comp.map_profiles)
            body = (f'<Map xmlns="" fromProfile="{a}" toProfile="{b}"><Mappings>'
                    '<Mapping fromKey="3" fromType="profile" toKey="3" toType="profile"/></Mappings>'
                    "<Functions/><Defaults/><DocumentCacheJoins/></Map>")
        elif comp.type == "process":
            shapes = ['<shape image="start" name="shape1" shapetype="start" userlabel="" x="96.0" y="94.0">'
                      '<configuration><noaction/></configuration><dragpoints/></shape>']
            for i, s in enumerate(self.shapes(comp, version), start=2):
                head = f'name="shape{i}" userlabel={quoteattr(s[-1])} x="{96 + 176 * (i - 1)}.0" y="94.0"'
                if s[0] == "connector":
                    cfg = (f'<connectoraction actionType="Get" connectionId="{self.catalog[s[1]].id}" '
                           f'operationId="{self.catalog[s[2]].id}"/>')
                    shapes.append(f'<shape image="connectoraction_icon" shapetype="connectoraction" {head}>'
                                  f"<configuration>{cfg}</configuration><dragpoints/></shape>")
                elif s[0] == "map":
                    shapes.append(f'<shape image="map_icon" shapetype="map" {head}><configuration>'
                                  f'<map mapId="{self.catalog[s[1]].id}"/></configuration><dragpoints/></shape>')
                else:
                    shapes.append(f'<shape image="processcall_icon" shapetype="processcall" {head}><configuration>'
                                  f'<processcall abort="true" processId="{self.catalog[s[1]].id}" wait="true">'
                                  "<parameters/></processcall></configuration><dragpoints/></shape>")
            if comp.name == "Benefits API Push":
                shapes.append('<shape image="message_icon" name="shape99" shapetype="message" userlabel="Plan lookup" '
                              'x="0.0" y="0.0"><configuration><message><msgParameters><parametervalue>'
                              f'<profileelement elementId="3" profileId="{DELETED_PROFILE_ID}"/></parametervalue>'
                              "</msgParameters></message></configuration><dragpoints/></shape>")
            body = f'<process xmlns="" workload="general"><shapes>{"".join(shapes)}</shapes></process>'
        else:
            body = "<Settings/>"
        return (f'<?xml version="1.0" encoding="UTF-8"?><bns:Component xmlns:bns="http://api.platform.boomi.com/" '
                f'componentId="{comp.id}" version="{version}" name={quoteattr(comp.name)} type="{comp.type}" '
                f'folderId="{FOLDERS[comp.folder]}" createdBy="{USERS[0]}" modifiedBy="{USERS[1]}">'
                f"<bns:encryptedValues/><bns:object>{body}</bns:object></bns:Component>")

    # ---- transport hook ------------------------------------------------------------------------
    def handle(self, method: str, path: str, body: Any, now: datetime, page) -> tuple[int, Any] | None:
        """Returns a response for the lineage endpoints, or None if the path isn't one of them."""
        if method == "POST" and path == "/ComponentMetadata/query":
            # only current, non-deleted components exist in this account's API, which is what the
            # crawler's filter asks for
            return 200, page(self.metadata(now))
        if method == "POST" and path == "/ComponentReference/query":
            props = {e["property"]: e["argument"][0]
                     for e in (body or {}).get("QueryFilter", {}).get("expression", {}).get("nestedExpression", [])}
            if "parentComponentId" not in props or "parentVersion" not in props:
                return 400, {"message": "parentComponentId and parentVersion are both required"}
            return 200, page(self.references(props["parentComponentId"], int(props["parentVersion"]), now))
        if method == "GET" and path.startswith("/Component/"):
            ref = path.split("/", 2)[2]
            comp_id, _, ver = ref.partition("~")
            comp = self.by_id.get(comp_id)
            if not comp:
                return 400, {"message": "Invalid component id"}
            version = int(ver) if ver else (self.current_version(comp, now) or (1, None))[0]
            xml = self.xml(comp_id, version)
            return (200, xml.encode()) if xml else (400, {"message": "Invalid version"})
        return None
