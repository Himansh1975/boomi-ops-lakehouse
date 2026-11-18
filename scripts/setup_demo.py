"""Build, deploy and schedule the six demo processes through the Platform API.

    python scripts/setup_demo.py                          # all processes (safe to re-run)
    python scripts/setup_demo.py --only "Timesheet Import" --run-now
    python scripts/setup_demo.py --bad-deploy             # ship Customer Master Sync v2 (with the bug)
    python scripts/setup_demo.py --rollback               # ship the v1 logic again

Each failing process fails for a real reason, at a known rate, so the triage agent can be scored:

| Process                 | Every   | Real failure                                   | Category      |
|-------------------------|---------|------------------------------------------------|---------------|
| HR Employee Sync        | 15 min  | none (happy path)                              | -             |
| Benefits API Push       | 30 min  | HTTP 401 from the API about 25% of the time    | auth          |
| Timesheet Import        | hourly  | about 10% of feeds carry an invalid date       | data_quality  |
| Payroll Export          | hourly  | upstream answers in 1-10 s, read timeout is 8 s | timeout       |
| Vendor Invoice Poller   | 10 min  | upstream returns 503 06:00-07:59 UTC daily     | upstream_down |
| Customer Master Sync    | 30 min  | after --bad-deploy, v2 crashes on phone numbers | code_bug      |

The trial allows 3 connections, so everything shares: jsonplaceholder (HTTP), httpbin (HTTP), local disk.
Component XML follows Boomi's open-source boomi-integration skill templates
(github.com/OfficialBoomi/boomi-integration, references/components and references/steps).
"""
from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from xml.sax.saxutils import escape, quoteattr

sys.path.insert(0, str(Path(__file__).parent))
from boomi_api import call  # noqa: E402

BNS = "http://api.platform.boomi.com/"
RUNTIME_NAME = "sim-runtime-01"
FOLDER_OF = "HR Employee Sync"  # the folder you created; all demo components go there
API = Path("data/api")


# ---- API helpers --------------------------------------------------------------------------------
def ok(r, what):
    if r.status_code >= 400:
        raise SystemExit(f"{what} failed: HTTP {r.status_code}\n{r.text[:2000]}")
    return r


def post_json(path: str, body: dict, what: str):
    API.mkdir(parents=True, exist_ok=True)
    f = API / "_body.json"
    f.write_text(json.dumps(body))
    return ok(call("POST", path, str(f)), what)


def query(obj: str, *conds: tuple[str, str]) -> list[dict]:
    exprs = [{"operator": "EQUALS", "property": p, "argument": [v]} for p, v in conds]
    expr = exprs[0] if len(exprs) == 1 else {"operator": "and", "nestedExpression": exprs}
    return post_json(f"/{obj}/query", {"QueryFilter": {"expression": expr}}, f"{obj} query").json().get("result", [])


def find_component(name: str, ctype: str) -> dict | None:
    rows = [r for r in query("ComponentMetadata", ("name", name), ("currentVersion", "true"),
                             ("deleted", "false")) if r["type"] == ctype]
    return rows[0] if rows else None


def save_component(name: str, ctype: str, subtype: str | None, folder_id: str, body: str,
                   create_only: bool = False) -> str:
    existing = find_component(name, ctype)
    if existing and create_only:
        return existing["componentId"]
    sub = f" subType={quoteattr(subtype)}" if subtype else ""
    cid = f" componentId={quoteattr(existing['componentId'])}" if existing else ""
    xml = (f'<?xml version="1.0" encoding="UTF-8"?>'
           f'<bns:Component xmlns:bns="{BNS}" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
           f'{cid} name={quoteattr(name)} type={quoteattr(ctype)}{sub} folderId={quoteattr(folder_id)}>'
           f'<bns:encryptedValues/><bns:object>{body}</bns:object></bns:Component>')
    API.mkdir(parents=True, exist_ok=True)
    f = API / f"{name.replace(' ', '_')}.xml"
    f.write_text(xml)
    path = f"/Component/{existing['componentId']}" if existing else "/Component"
    r = ok(call("POST", path, str(f)), f"save component {name}")
    new_id = ET.fromstring(r.content).get("componentId")
    print(f"{'updated' if existing else 'created'} {ctype:18} {name}")
    return new_id


# ---- connections and operations -----------------------------------------------------------------
def http_connection(url: str, read_timeout_ms: int | None = None) -> str:
    rt = f' connectTimeout="10000" readTimeout="{read_timeout_ms}"' if read_timeout_ms else ""
    return f"""<HttpSettings xmlns="" authenticationType="NONE" url={quoteattr(url)}{rt}>
<AuthSettings user=""/>
<OAuthSettings requestTokenURL="" accessTokenURL="" authorizationURL="" consumerKey="" accessToken=""
 realm="" signatureMethod="SHA256" suppressBlankAccessToken="false"/>
<OAuth2Settings grantType="code" refreshAuthScheme="req_body_params_auth">
<credentials clientId="" accessToken="" accessTokenKey=""/>
<authorizationTokenEndpoint url=""><sslOptions/></authorizationTokenEndpoint>
<authorizationParameters/>
<accessTokenEndpoint url=""><sslOptions/></accessTokenEndpoint>
<accessTokenParameters/><scope/></OAuth2Settings>
<AwsSettings><credentials><accessKeyId/><awsService>s3</awsService><customService/>
<awsRegion>ap-northeast-1</awsRegion><customRegion/></credentials></AwsSettings>
<SSLOptions clientauth="false" clientsslalias="" trustServerCert="false" trustedcertalias=""/>
</HttpSettings>"""


def http_get(*path: tuple[str, bool]) -> str:
    """path parts: (literal, False) or (DDP_NAME, True) for a replacement variable."""
    els = "".join(
        f'<element{" isVariable=" + chr(34) + "true" + chr(34) if var else ""} key="{2000000 + i}" name={quoteattr(p)}/>'
        for i, (p, var) in enumerate(path))
    return f"""<Operation xmlns=""><Archiving directory="" enabled="false"/><Configuration>
<HttpGetAction dataContentType="application/json" followRedirects="false" methodType="GET"
 requestProfileType="NONE" responseProfileType="NONE" returnErrors="false" returnMimeResponse="false">
<requestHeaders/><pathElements>{els}</pathElements>
<responseHeaderMapping/><reflectHeaders/></HttpGetAction>
</Configuration><Tracking><TrackedFields/></Tracking><Caching/></Operation>"""


DISK_CONNECTION = """<GenericConnectionConfig>
<field id="directory" type="string" value="work/out"/>
<field id="pollingInterval" type="integer" value="10000"/>
</GenericConnectionConfig>"""

DISK_CREATE = """<Operation returnApplicationErrors="false" trackResponse="false">
<Archiving directory="" enabled="false"/><Configuration>
<GenericOperationConfig objectTypeId="FILE_CREATE_UPSERT" operationType="CREATE"
 requestProfileType="binary" responseProfileType="json">
<field id="createDir" type="boolean" value="true"/>
<field id="includeAll" type="boolean" value="false"/>
<field id="actionIfFileExists" type="string" value="FORCE_UNIQUE_NAMES"/>
<Options><QueryOptions><Fields><ConnectorObject name="File"><FieldList>
<ConnectorField filterable="true" name="createdDate" selectable="true" selected="true" sortable="true"/>
<ConnectorField filterable="false" name="directory" selectable="true" selected="true" sortable="false"/>
<ConnectorField filterable="true" name="fileName" selectable="true" selected="true" sortable="true"/>
<ConnectorField filterable="true" name="fileSize" selectable="true" selected="true" sortable="true"/>
<ConnectorField filterable="true" name="isDirectory" selectable="true" selected="true" sortable="true"/>
<ConnectorField filterable="true" name="modifiedDate" selectable="true" selected="true" sortable="true"/>
</FieldList></ConnectorObject></Fields><Inputs/></QueryOptions></Options>
</GenericOperationConfig></Configuration><Tracking><TrackedFields/></Tracking><Caching/></Operation>"""


# ---- process shapes -----------------------------------------------------------------------------
class Flow:
    """Builds a straight-line process: each added shape connects to the next."""

    def __init__(self):
        self.shapes: list[str] = []

    def _add(self, image: str, shapetype: str, label: str, config: str) -> None:
        n = len(self.shapes) + 1
        x = 96 + 176 * (n - 1)
        drag = (f'<dragpoints><dragpoint name="shape{n}.dragpoint1" toShape="shape{n + 1}" '
                f'x="{x + 136}.0" y="106.0"/></dragpoints>') if shapetype != "stop" else "<dragpoints/>"
        self.shapes.append(f'<shape image="{image}" name="shape{n}" shapetype="{shapetype}" '
                           f'userlabel={quoteattr(label)} x="{x}.0" y="94.0">'
                           f'<configuration>{config}</configuration>{drag}</shape>')

    def start(self):
        self._add("start", "start", "", "<noaction/>")
        return self

    def http(self, label: str, conn: str, op: str):
        self._add("connectoraction_icon", "connectoraction", label,
                  f'<connectoraction actionType="Get" allowDynamicCredentials="NONE" connectionId="{conn}" '
                  f'connectorType="http" hideSettings="false" operationId="{op}" '
                  f'parameter-profile="EMBEDDED|HttpParameterChooser|{op}"><parameters/><dynamicProperties/>'
                  f'</connectoraction>')
        return self

    def script(self, label: str, groovy: str):
        self._add("dataprocess_icon", "dataprocess", label,
                  '<dataprocess><step index="1" key="1" name="Custom Scripting" processtype="12">'
                  f'<dataprocessscript language="groovy2" useCache="true"><script><![CDATA[{groovy}]]></script>'
                  '</dataprocessscript></step></dataprocess>')
        return self

    def write_file(self, prefix: str, conn: str, op: str):
        self._add("documentproperties_icon", "documentproperties", "Set file name",
                  '<documentproperties><documentproperty defaultValue="" isDynamicCredential="false" '
                  'isTradingPartner="false" name="Disk v2 - File Name" persist="false" '
                  'propertyId="connector.disk-sdk.fileName" shouldEncrypt="false"><sourcevalues>'
                  f'<parametervalue key="0" valueType="static"><staticparameter staticproperty={quoteattr(prefix)}/></parametervalue>'
                  '<parametervalue key="1" valueType="execution"><executionparameter executionproperty="Execution Id"/></parametervalue>'
                  '<parametervalue key="2" valueType="static"><staticparameter staticproperty=".txt"/></parametervalue>'
                  '</sourcevalues></documentproperty></documentproperties>')
        self._add("connectoraction_icon", "connectoraction", "Write file",
                  f'<connectoraction actionType="CREATE" allowDynamicCredentials="NONE" connectionId="{conn}" '
                  f'connectorType="disk-sdk" hideSettings="false" operationId="{op}"><parameters/>'
                  '<dynamicProperties/></connectoraction>')
        return self

    def stop(self):
        self._add("stop_icon", "stop", "", '<stop continue="true"/>')
        return self

    def xml(self) -> str:
        return ('<process xmlns="" allowSimultaneous="false" enableUserLog="false" processLogOnErrorOnly="false" '
                'purgeDataImmediately="false" stopProcessingIfZeroDocuments="true" updateRunDates="true" '
                f'workload="general"><shapes>{"".join(self.shapes)}</shapes></process>')


# ---- groovy -------------------------------------------------------------------------------------
LOOP_HEAD = """import java.util.Properties
import java.io.InputStream
import java.io.ByteArrayInputStream
"""

TIMESHEET = LOOP_HEAD + r"""import java.text.SimpleDateFormat
Random rnd = new Random()
SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd")
fmt.setLenient(false)
for (int i = 0; i < dataContext.getDataCount(); i++) {
    Properties props = dataContext.getProperties(i)
    List rows = (1..40).collect { r -> [r, "EMP" + (1000 + rnd.nextInt(500)),
        "2026-11-" + String.format("%02d", 1 + rnd.nextInt(28)), String.valueOf(4 + rnd.nextInt(6))] }
    if (rnd.nextInt(100) < 10) {   /* about 10% of feeds arrive with one invalid date */
        rows[rnd.nextInt(40)][2] = ["31-02-2026", "2026/13/01", ""][rnd.nextInt(3)]
    }
    StringBuilder sb = new StringBuilder("row,employee_id,punch_date,hours\n")
    rows.each { r ->
        try { fmt.parse(r[2]) } catch (Exception e) {
            throw new Exception("Validation failed at row " + r[0] + ": Unparseable date: \"" + r[2] +
                "\" for field PunchDate (expected yyyy-MM-dd)")
        }
        sb.append(r.join(",")).append("\n")
    }
    dataContext.storeStream(new ByteArrayInputStream(sb.toString().getBytes("UTF-8")), props)
}
"""

PAYROLL_DELAY = LOOP_HEAD + r"""Random rnd = new Random()
for (int i = 0; i < dataContext.getDataCount(); i++) {
    InputStream is = dataContext.getStream(i)
    Properties props = dataContext.getProperties(i)
    /* the payroll provider answers in 1-10 s; the connection gives up after 8 s */
    props.setProperty("document.dynamic.userdefined.DELAY", String.valueOf(1 + rnd.nextInt(10)))
    dataContext.storeStream(is, props)
}
"""

VENDOR_STATUS = LOOP_HEAD + r"""Calendar now = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
int hour = now.get(Calendar.HOUR_OF_DAY)
for (int i = 0; i < dataContext.getDataCount(); i++) {
    InputStream is = dataContext.getStream(i)
    Properties props = dataContext.getProperties(i)
    /* the vendor's API is down for maintenance 06:00-07:59 UTC every day */
    props.setProperty("document.dynamic.userdefined.CODE", (hour == 6 || hour == 7) ? "503" : "200")
    dataContext.storeStream(is, props)
}
"""

PHONE_V1 = LOOP_HEAD + r"""import groovy.json.JsonSlurper
import groovy.json.JsonOutput
for (int i = 0; i < dataContext.getDataCount(); i++) {
    InputStream is = dataContext.getStream(i)
    Properties props = dataContext.getProperties(i)
    List users = new JsonSlurper().parse(is)
    List out = users.collect { u -> [id: u.id, name: u.name, email: u.email,
        phone: u.phone.split(" x")[0].replaceAll("[^0-9]", "")] }
    dataContext.storeStream(new ByteArrayInputStream(JsonOutput.toJson(out).getBytes("UTF-8")), props)
}
"""

# v2 "adds the phone extension" but assumes every number has one: crashes on the ones that don't.
PHONE_V2_BUG = PHONE_V1.replace(
    'phone: u.phone.split(" x")[0].replaceAll("[^0-9]", "")] }',
    'phone: u.phone.split(" x")[0].replaceAll("[^0-9]", ""),\n'
    '        extension: u.phone.split(" x")[1].trim()] }')
assert PHONE_V2_BUG != PHONE_V1


@dataclass
class Demo:
    name: str
    minutes: str  # ProcessSchedules minutes field, e.g. "0-59/15"
    build: callable  # (ids) -> process XML


def build_all(folder: str) -> dict[str, str]:
    """Shared connections/operations: created once, never overwritten."""
    c = {}
    c["jp"] = save_component("JSONPlaceholder HTTP", "connector-settings", "http", folder,
                             http_connection("https://jsonplaceholder.typicode.com"), create_only=True)
    c["users"] = save_component("GET users", "connector-action", "http", folder,
                                http_get(("users", False)), create_only=True)
    c["hb"] = save_component("Partner APIs (httpbin)", "connector-settings", "http", folder,
                             http_connection("https://httpbin.org", read_timeout_ms=8000), create_only=True)
    c["auth"] = save_component("GET benefits status", "connector-action", "http", folder,
                               http_get(("status/200:0.75,401:0.25", False)), create_only=True)
    c["delay"] = save_component("GET payroll ack", "connector-action", "http", folder,
                                http_get(("delay/", False), ("DELAY", True)), create_only=True)
    c["vendor"] = save_component("GET vendor invoices", "connector-action", "http", folder,
                                 http_get(("status/", False), ("CODE", True)), create_only=True)
    c["disk"] = save_component("Local Out Disk", "connector-settings", "disk-sdk", folder,
                               DISK_CONNECTION, create_only=True)
    c["write"] = save_component("Write file", "connector-action", "disk-sdk", folder, DISK_CREATE,
                                create_only=True)
    return c


def demos(phone_script: str) -> list[Demo]:
    return [
        Demo("HR Employee Sync", "0-59/15",
             lambda c: Flow().start().http("Get users", c["jp"], c["users"])
             .write_file("users_", c["disk"], c["write"]).stop().xml()),
        Demo("Benefits API Push", "0-59/30",
             lambda c: Flow().start().http("Push to benefits API", c["hb"], c["auth"]).stop().xml()),
        Demo("Timesheet Import", "5",
             lambda c: Flow().start().script("Build and validate feed", TIMESHEET)
             .write_file("timesheet_", c["disk"], c["write"]).stop().xml()),
        Demo("Payroll Export", "20",
             lambda c: Flow().start().script("Set provider delay", PAYROLL_DELAY)
             .http("Send to payroll provider", c["hb"], c["delay"]).stop().xml()),
        Demo("Vendor Invoice Poller", "0-59/10",
             lambda c: Flow().start().script("Vendor availability", VENDOR_STATUS)
             .http("Get vendor invoices", c["hb"], c["vendor"]).stop().xml()),
        Demo("Customer Master Sync", "0-59/30",
             lambda c: Flow().start().http("Get customers", c["jp"], c["users"])
             .script("Format phone numbers", phone_script)
             .write_file("customers_", c["disk"], c["write"]).stop().xml()),
    ]


def runtime() -> tuple[dict, str]:
    atoms = query("Atom", ("name", RUNTIME_NAME))
    if not atoms:
        raise SystemExit(f"runtime {RUNTIME_NAME} not found")
    att = query("EnvironmentAtomAttachment", ("atomId", atoms[0]["id"]))
    if not att:
        raise SystemExit(f"{RUNTIME_NAME} is not attached to an environment")
    return atoms[0], att[0]["environmentId"]


def deploy(pid: str, env_id: str, notes: str) -> str:
    meta = query("ComponentMetadata", ("componentId", pid), ("currentVersion", "true"))[0]
    pkg = post_json("/PackagedComponent", {"componentId": pid, "packageVersion": f"1.0.{meta['version']}",
                                           "notes": notes}, "package").json()
    post_json("/DeployedPackage", {"packageId": pkg["packageId"], "environmentId": env_id, "notes": notes},
              "deploy")
    return pkg["packageVersion"]


def schedule(atom_id: str, pid: str, minutes: str) -> bool:
    sched = query("ProcessSchedules", ("atomId", atom_id), ("processId", pid))
    if not sched:
        return False
    s = sched[0]
    post_json(f"/ProcessSchedules/{s['id']}", {
        "@type": "ProcessSchedules", "id": s["id"], "atomId": atom_id, "processId": pid,
        "Schedule": [{"@type": "Schedule", "years": "*", "months": "*", "daysOfMonth": "*",
                      "daysOfWeek": "*", "hours": "*", "minutes": minutes}]}, "schedule")
    return True


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", action="append", help="process name (repeatable); default all")
    ap.add_argument("--run-now", action="store_true", help="start one execution of each right away")
    ap.add_argument("--bad-deploy", action="store_true", help="deploy Customer Master Sync v2 (buggy)")
    ap.add_argument("--rollback", action="store_true", help="deploy Customer Master Sync v1 logic again")
    a = ap.parse_args()

    anchor = find_component(FOLDER_OF, "process")
    if not anchor:
        raise SystemExit(f"process '{FOLDER_OF}' not found (its folder holds the demo components)")
    folder = anchor["folderId"]
    ids = build_all(folder)
    atom, env_id = runtime()

    if a.bad_deploy or a.rollback:
        a.only = ["Customer Master Sync"]
    phone = PHONE_V2_BUG if a.bad_deploy else PHONE_V1
    notes = ("add phone extension to customer records" if a.bad_deploy else
             "rollback: phone formatting back to v1 logic" if a.rollback else "initial release")

    for d in demos(phone):
        if a.only and d.name not in a.only:
            continue
        pid = save_component(d.name, "process", None, folder, d.build(ids))
        version = deploy(pid, env_id, notes)
        scheduled = schedule(atom["id"], pid, d.minutes)
        print(f"  deployed {d.name} {version}; schedule minutes={d.minutes} "
              f"{'set' if scheduled else 'NOT set yet (re-run in a minute)'}")
        if a.run_now:
            post_json("/ExecutionRequest", {"@type": "ExecutionRequest", "atomId": atom["id"],
                                            "processId": pid}, "execute")
            print("  started one run now")


if __name__ == "__main__":
    main()
