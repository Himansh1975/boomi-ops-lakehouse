"""Add the components the lineage map needs: shared profiles, a map, a sub-process and an orphan.

    python scripts/setup_lineage_demo.py            # create or update everything (safe to re-run)
    python scripts/setup_lineage_demo.py --run-now  # also start one run of Employee Directory Export

What gets built (no new connections: the trial allows only 3, so these reuse the existing ones):

| Component                       | Type          | Planted lineage case                              |
|---------------------------------|---------------|---------------------------------------------------|
| User Record (JSON)              | profile.json  | shared: source profile of the map                 |
| Directory Entry (JSON)          | profile.json  | shared: target profile of the map                 |
| User to Directory Entry         | transform.map | used by two processes; references two profiles    |
| Common - Archive to Disk        | process       | sub-process called by two parents                 |
| Employee Directory Export       | process       | deployed, NOT scheduled (deployed but never runs) |
| Contractor Directory Export     | process       | built but never deployed                          |
| Legacy Employee Profile (JSON)  | profile.json  | orphan: nothing references it                     |

Component XML follows the templates in Boomi's open-source boomi-integration skill
(references/components/json_profile_component.md, map_component.md; references/steps/map_step.md,
process_call_step.md, start_step.md, return_documents_step.md).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from xml.sax.saxutils import quoteattr

sys.path.insert(0, str(Path(__file__).parent))
import setup_demo as sd  # noqa: E402

SUB_RETURN_SHAPE = "shape4"  # name of the Return Documents shape in the sub-process


def json_profile(fields: list[tuple[str, str]]) -> str:
    """fields: (name, 'character' | 'number'). Keys: Root=1, Object=2, fields from 3."""
    fmt = {"character": "<ProfileCharacterFormat/>", "number": '<ProfileNumberFormat numberFormat=""/>'}
    entries = "".join(
        f'<JSONObjectEntry dataType="{t}" isMappable="true" isNode="true" key="{3 + i}" name={quoteattr(n)}>'
        f"<DataFormat>{fmt[t]}</DataFormat></JSONObjectEntry>"
        for i, (n, t) in enumerate(fields))
    return ('<JSONProfile xmlns="" strict="false"><DataElements>'
            '<JSONRootValue dataType="character" isMappable="true" isNode="true" key="1" name="Root">'
            "<DataFormat><ProfileCharacterFormat/></DataFormat>"
            f'<JSONObject isMappable="false" isNode="true" key="2" name="Object">{entries}</JSONObject>'
            "<Qualifiers><QualifierList/></Qualifiers></JSONRootValue></DataElements><tagLists/></JSONProfile>")


def map_xml(from_profile: str, to_profile: str, pairs: list[tuple[int, int]]) -> str:
    m = "".join(f'<Mapping fromKey="{a}" fromType="profile" toKey="{b}" toType="profile"/>' for a, b in pairs)
    return (f'<Map xmlns="" fromProfile="{from_profile}" toProfile="{to_profile}">'
            f"<Mappings>{m}</Mappings><Functions/><Defaults/><DocumentCacheJoins/></Map>")


class Flow(sd.Flow):
    """Adds passthrough start, map, process call and return documents to the straight-line builder."""

    def passthrough_start(self):
        self._add("start", "start", "", "<passthroughaction/>")
        return self

    def map(self, label: str, map_id: str):
        self._add("map_icon", "map", label, f'<map mapId="{map_id}"/>')
        return self

    def process_call(self, label: str, process_id: str, return_shape: str):
        n = len(self.shapes) + 1
        x = 96 + 176 * (n - 1)
        self.shapes.append(
            f'<shape image="processcall_icon" name="shape{n}" shapetype="processcall" userlabel={quoteattr(label)} '
            f'x="{x}.0" y="94.0"><configuration><processcall abort="true" processId="{process_id}" wait="true">'
            f'<parameters/><returnpaths><returnpaths childShapeName="{return_shape}"/></returnpaths></processcall>'
            f'</configuration><dragpoints><dragpoint identifier="{return_shape}" name="shape{n}.dragpoint1" '
            f'toShape="shape{n + 1}" x="{x + 136}.0" y="106.0"/></dragpoints></shape>')
        return self

    def return_documents(self):
        n = len(self.shapes) + 1
        x = 96 + 176 * (n - 1)
        self.shapes.append(f'<shape image="returndocuments_icon" name="shape{n}" shapetype="returndocuments" '
                           f'x="{x}.0" y="94.0"><configuration><returndocuments/></configuration>'
                           f'<dragpoints/></shape>')
        return self

    def xml(self, subprocess: bool = False) -> str:
        x = super().xml()
        return x.replace('updateRunDates="true"', 'updateRunDates="false"') if subprocess else x


SPLIT_USERS = sd.LOOP_HEAD + r"""import groovy.json.JsonSlurper
import groovy.json.JsonOutput
for (int i = 0; i < dataContext.getDataCount(); i++) {
    InputStream is = dataContext.getStream(i)
    Properties props = dataContext.getProperties(i)
    List users = new JsonSlurper().parse(is)
    /* one document per user, so the map works on a single record */
    users.each { u ->
        String one = JsonOutput.toJson([id: u.id, name: u.name, email: u.email, phone: u.phone])
        dataContext.storeStream(new ByteArrayInputStream(one.getBytes("UTF-8")), props)
    }
}
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-now", action="store_true", help="start one run of Employee Directory Export")
    a = ap.parse_args()

    anchor = sd.find_component(sd.FOLDER_OF, "process")
    if not anchor:
        raise SystemExit(f"process '{sd.FOLDER_OF}' not found (its folder holds the demo components)")
    folder = anchor["folderId"]
    c = sd.build_all(folder)  # existing shared connections/operations (create_only, never overwritten)

    user = sd.save_component("User Record (JSON)", "profile.json", None, folder, json_profile(
        [("id", "number"), ("name", "character"), ("email", "character"), ("phone", "character")]))
    entry = sd.save_component("Directory Entry (JSON)", "profile.json", None, folder, json_profile(
        [("employeeId", "character"), ("fullName", "character"), ("email", "character")]))
    sd.save_component("Legacy Employee Profile (JSON)", "profile.json", None, folder, json_profile(
        [("emp_no", "character"), ("emp_name", "character"), ("dept_code", "character")]))
    mp = sd.save_component("User to Directory Entry", "transform.map", None, folder,
                           map_xml(user, entry, [(3, 3), (4, 4), (5, 5)]))

    sub_flow = Flow().passthrough_start().write_file("directory_", c["disk"], c["write"]).return_documents()
    assert f'name="{SUB_RETURN_SHAPE}" shapetype="returndocuments"' in sub_flow.xml(), "return shape name moved"
    sub = sd.save_component("Common - Archive to Disk", "process", None, folder, sub_flow.xml(subprocess=True))

    def parent() -> str:
        return (Flow().start().http("Get users", c["jp"], c["users"])
                .script("Split users into documents", SPLIT_USERS)
                .map("Map to directory entry", mp)
                .process_call("Archive to disk", sub, SUB_RETURN_SHAPE).stop().xml())

    emp = sd.save_component("Employee Directory Export", "process", None, folder, parent())
    sd.save_component("Contractor Directory Export", "process", None, folder, parent())

    atom, env_id = sd.runtime()
    version = sd.deploy(emp, env_id, "lineage demo: deployed, not scheduled")
    print(f"  deployed Employee Directory Export {version} (no schedule on purpose)")
    if a.run_now:
        sd.post_json("/ExecutionRequest", {"@type": "ExecutionRequest", "atomId": atom["id"], "processId": emp},
                     "execute")
        print("  started one run now")


if __name__ == "__main__":
    main()
