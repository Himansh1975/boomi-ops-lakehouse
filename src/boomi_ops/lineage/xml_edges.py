"""Edges parsed from component XML: the second, independent source of lineage.

Every attribute holding a component GUID is a link (connectionId, operationId, mapId, processId,
fromProfile, toProfile, ...), except the component's own id on the root element. Checked on a live
trial on 2026-11-18: this gives exactly the same links as the ComponentReference API (28 of 28).

The parser also remembers *where* the link sits (shape name, label, type) so the viewer can say
"Payroll Export uses Partner APIs (httpbin) in shape 'Send to payroll provider'".
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET

GUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
PRIVATE_ATTRS = ("createdBy", "modifiedBy")


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def parse_xml_edges(xml: bytes | str, component_id: str) -> list[dict]:
    root = ET.fromstring(xml)
    edges: list[dict] = []
    seen: set[tuple] = set()

    def walk(el: ET.Element, shape: ET.Element | None) -> None:
        if _local(el.tag) == "shape":
            shape = el
        if el is not root:
            for attr, value in el.attrib.items():
                if GUID.fullmatch(value) and value != component_id:
                    key = (value, attr, shape.get("name") if shape is not None else "")
                    if key in seen:
                        continue
                    seen.add(key)
                    edges.append({
                        "child_id": value,
                        "attr": attr,
                        "element": _local(el.tag),
                        "shape_name": shape.get("name", "") if shape is not None else "",
                        "shape_label": shape.get("userlabel", "") if shape is not None else "",
                        "shape_type": shape.get("shapetype", "") if shape is not None else "",
                    })
        for child in el:
            walk(child, shape)

    walk(root, None)
    return edges


def redact_xml(xml: bytes | str) -> str:
    """Drops user emails and anything encrypted before the XML is stored or shown."""
    text = xml.decode("utf-8") if isinstance(xml, bytes) else xml
    for attr in PRIVATE_ATTRS:
        text = re.sub(rf'\s{attr}="[^"]*"', "", text)
    return re.sub(r"<bns:encryptedValues>.*?</bns:encryptedValues>", "<bns:encryptedValues/>", text, flags=re.S)
