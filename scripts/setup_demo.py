"""Build, deploy and schedule demo process 1 (HR Employee Sync) through the Platform API.

    python scripts/setup_demo.py                 # do everything (safe to re-run)
    python scripts/setup_demo.py --run-now       # also start one execution right away

Process: Start (no data) -> HTTP GET jsonplaceholder /users -> Set file name -> Disk v2 write -> Stop

Component XML follows the templates in Boomi's open-source boomi-integration skill
(github.com/OfficialBoomi/boomi-integration, references/components/*). Re-running finds existing
components by name and updates them instead of creating duplicates.
"""
from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from xml.sax.saxutils import quoteattr

sys.path.insert(0, str(Path(__file__).parent))
from boomi_api import call  # noqa: E402

BNS = "http://api.platform.boomi.com/"
PROCESS_NAME = "HR Employee Sync"
RUNTIME_NAME = "sim-runtime-01"


def ok(r, what):
    if r.status_code >= 400:
        raise SystemExit(f"{what} failed: HTTP {r.status_code}\n{r.text[:2000]}")
    return r


def query(obj: str, *conds: tuple[str, str]) -> list[dict]:
    exprs = [{"operator": "EQUALS", "property": p, "argument": [v]} for p, v in conds]
    expr = exprs[0] if len(exprs) == 1 else {"operator": "and", "nestedExpression": exprs}
    body = Path("data/api/_q.json")
    body.parent.mkdir(parents=True, exist_ok=True)
    body.write_text(json.dumps({"QueryFilter": {"expression": expr}}))
    return ok(call("POST", f"/{obj}/query", str(body)), f"{obj} query").json().get("result", [])


def find_component(name: str, ctype: str) -> str | None:
    rows = [r for r in query("ComponentMetadata", ("name", name), ("currentVersion", "true"),
                             ("deleted", "false")) if r["type"] == ctype]
    return rows[0]["componentId"] if rows else None


def upsert_component(name: str, ctype: str, subtype: str | None, folder_id: str, body: str) -> str:
    existing = find_component(name, ctype)
    sub = f" subType={quoteattr(subtype)}" if subtype else ""
    cid = f" componentId={quoteattr(existing)}" if existing else ""
    xml = (f'<?xml version="1.0" encoding="UTF-8"?>'
           f'<bns:Component xmlns:bns="{BNS}" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
           f'{cid} name={quoteattr(name)} type={quoteattr(ctype)}{sub} folderId={quoteattr(folder_id)}>'
           f'<bns:encryptedValues/><bns:object>{body}</bns:object></bns:Component>')
    f = Path(f"data/api/{name.replace(' ', '_')}.xml")
    f.write_text(xml)
    path = f"/Component/{existing}" if existing else "/Component"
    r = ok(call("POST", path, str(f)), f"save component {name}")
    new_id = ET.fromstring(r.content).get("componentId")
    print(f"{'updated' if existing else 'created'} {ctype:20} {name}  ({new_id})")
    return new_id


# ---- component bodies ---------------------------------------------------------------------------
HTTP_CONNECTION = """<HttpSettings xmlns="" authenticationType="NONE" url="https://jsonplaceholder.typicode.com">
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

HTTP_GET_USERS = """<Operation xmlns=""><Archiving directory="" enabled="false"/><Configuration>
<HttpGetAction dataContentType="application/json" followRedirects="false" methodType="GET"
 requestProfileType="NONE" responseProfileType="NONE" returnErrors="false" returnMimeResponse="false">
<requestHeaders/><pathElements><element key="2000000" name="users"/></pathElements>
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


def process_body(http_conn: str, http_op: str, disk_conn: str, disk_op: str) -> str:
    return f"""<process xmlns="" allowSimultaneous="false" enableUserLog="false" processLogOnErrorOnly="false"
 purgeDataImmediately="false" stopProcessingIfZeroDocuments="true" updateRunDates="true" workload="general">
<shapes>
<shape image="start" name="shape1" shapetype="start" userlabel="" x="96.0" y="94.0">
 <configuration><noaction/></configuration>
 <dragpoints><dragpoint name="shape1.dragpoint1" toShape="shape2" x="232.0" y="106.0"/></dragpoints></shape>
<shape image="connectoraction_icon" name="shape2" shapetype="connectoraction" userlabel="Get users" x="256.0" y="94.0">
 <configuration><connectoraction actionType="Get" allowDynamicCredentials="NONE" connectionId="{http_conn}"
  connectorType="http" hideSettings="false" operationId="{http_op}"
  parameter-profile="EMBEDDED|HttpParameterChooser|{http_op}"><parameters/><dynamicProperties/></connectoraction></configuration>
 <dragpoints><dragpoint name="shape2.dragpoint1" toShape="shape3" x="408.0" y="106.0"/></dragpoints></shape>
<shape image="documentproperties_icon" name="shape3" shapetype="documentproperties" userlabel="Set file name" x="432.0" y="94.0">
 <configuration><documentproperties>
  <documentproperty defaultValue="" isDynamicCredential="false" isTradingPartner="false" name="Disk v2 - File Name"
   persist="false" propertyId="connector.disk-sdk.fileName" shouldEncrypt="false"><sourcevalues>
   <parametervalue key="0" valueType="static"><staticparameter staticproperty="users_"/></parametervalue>
   <parametervalue key="1" valueType="execution"><executionparameter executionproperty="Execution Id"/></parametervalue>
   <parametervalue key="2" valueType="static"><staticparameter staticproperty=".json"/></parametervalue>
  </sourcevalues></documentproperty></documentproperties></configuration>
 <dragpoints><dragpoint name="shape3.dragpoint1" toShape="shape4" x="584.0" y="106.0"/></dragpoints></shape>
<shape image="connectoraction_icon" name="shape4" shapetype="connectoraction" userlabel="Write file" x="608.0" y="94.0">
 <configuration><connectoraction actionType="CREATE" allowDynamicCredentials="NONE" connectionId="{disk_conn}"
  connectorType="disk-sdk" hideSettings="false" operationId="{disk_op}"><parameters/><dynamicProperties/></connectoraction></configuration>
 <dragpoints><dragpoint name="shape4.dragpoint1" toShape="shape5" x="760.0" y="106.0"/></dragpoints></shape>
<shape image="stop_icon" name="shape5" shapetype="stop" userlabel="" x="784.0" y="94.0">
 <configuration><stop continue="true"/></configuration><dragpoints/></shape>
</shapes></process>"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-now", action="store_true")
    args = ap.parse_args()

    proc = query("ComponentMetadata", ("name", PROCESS_NAME), ("currentVersion", "true"), ("deleted", "false"))
    proc = [p for p in proc if p["type"] == "process"]
    if not proc:
        raise SystemExit(f"process '{PROCESS_NAME}' not found")
    folder = proc[0]["folderId"]

    http_conn = upsert_component("JSONPlaceholder HTTP", "connector-settings", "http", folder, HTTP_CONNECTION)
    http_op = upsert_component("GET users", "connector-action", "http", folder, HTTP_GET_USERS)
    disk_conn = upsert_component("Local Out Disk", "connector-settings", "disk-sdk", folder, DISK_CONNECTION)
    disk_op = upsert_component("Write file", "connector-action", "disk-sdk", folder, DISK_CREATE)
    pid = upsert_component(PROCESS_NAME, "process", None, folder,
                           process_body(http_conn, http_op, disk_conn, disk_op))

    atoms = query("Atom", ("name", RUNTIME_NAME))
    if not atoms:
        raise SystemExit(f"runtime {RUNTIME_NAME} not found")
    atom = atoms[0]
    att = query("EnvironmentAtomAttachment", ("atomId", atom["id"]))
    if not att:
        envs = ok(call("POST", "/Environment/query"), "Environment query").json().get("result", [])
        raise SystemExit(f"{RUNTIME_NAME} is not attached to an environment. Environments: "
                         f"{[(e['name'], e['id']) for e in envs]}")
    env_id = att[0]["environmentId"]
    print(f"runtime {RUNTIME_NAME} ({atom['id']}, status {atom.get('status')}) in environment {env_id}")

    meta = query("ComponentMetadata", ("componentId", pid), ("currentVersion", "true"))[0]
    pkg_req = Path("data/api/_pkg.json")
    pkg_req.write_text(json.dumps({"componentId": pid, "packageVersion": f"1.0.{meta['version']}",
                                   "notes": "initial release (setup_demo.py)"}))
    pkg = ok(call("POST", "/PackagedComponent", str(pkg_req)), "package").json()
    print(f"packaged {PROCESS_NAME} as {pkg['packageVersion']} ({pkg['packageId']})")

    dep_req = Path("data/api/_dep.json")
    dep_req.write_text(json.dumps({"packageId": pkg["packageId"], "environmentId": env_id,
                                   "notes": "initial release"}))
    dep = ok(call("POST", "/DeployedPackage", str(dep_req)), "deploy").json()
    print(f"deployed: deploymentId {dep.get('deploymentId')}")

    sched = query("ProcessSchedules", ("atomId", atom["id"]), ("processId", pid))
    if sched:
        s = sched[0]
        body = {"@type": "ProcessSchedules", "id": s["id"], "atomId": atom["id"], "processId": pid,
                "Schedule": [{"@type": "Schedule", "years": "*", "months": "*", "daysOfMonth": "*",
                              "daysOfWeek": "*", "hours": "*", "minutes": "0-59/15"}]}
        f = Path("data/api/_sched.json")
        f.write_text(json.dumps(body))
        ok(call("POST", f"/ProcessSchedules/{s['id']}", str(f)), "schedule")
        print("scheduled every 15 minutes")
    else:
        print("WARNING: no ProcessSchedules object found yet; re-run in a minute to set the schedule")

    if args.run_now:
        f = Path("data/api/_exec.json")
        f.write_text(json.dumps({"@type": "ExecutionRequest", "atomId": atom["id"], "processId": pid}))
        r = ok(call("POST", "/ExecutionRequest", str(f)), "execute")
        print(f"started one run now: {r.json().get('requestId')}")


if __name__ == "__main__":
    main()
