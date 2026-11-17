"""Raw Platform API calls for setting up the demo account (components are XML, most else JSON).

  python scripts/boomi_api.py GET  /ComponentMetadata/<id>
  python scripts/boomi_api.py POST /ComponentMetadata/query  query.json
  python scripts/boomi_api.py POST /Component  component.xml          # body type from file extension
"""
import base64
import os
import sys
from pathlib import Path

import requests

base = os.environ.get("BOOMI_BASE_URL", "https://api.boomi.com/api/rest/v1").rstrip("/")
root = f"{base}/{os.environ['BOOMI_ACCOUNT_ID']}"
auth = base64.b64encode(f"BOOMI_TOKEN.{os.environ['BOOMI_USERNAME']}:{os.environ['BOOMI_API_TOKEN']}".encode()).decode()


def call(method: str, path: str, body_file: str | None = None, accept: str | None = None) -> requests.Response:
    headers = {"Authorization": f"Basic {auth}"}
    data = None
    is_xml = path.startswith("/Component") and not path.startswith("/ComponentMetadata")
    if body_file:
        data = Path(body_file).read_bytes()
        headers["Content-Type"] = "application/xml" if body_file.endswith(".xml") else "application/json"
        is_xml = body_file.endswith(".xml")
    headers["Accept"] = accept or ("application/xml" if is_xml else "application/json")
    return requests.request(method, root + path, data=data, headers=headers, timeout=60)


if __name__ == "__main__":
    r = call(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else None)
    print(f"HTTP {r.status_code}", file=sys.stderr)
    print(r.text)
    sys.exit(0 if r.status_code < 400 else 1)
