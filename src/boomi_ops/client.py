"""Small Boomi Platform API client: query + queryMore paging, retries, a request-rate cap.

The client talks to a *transport*: a function (method, path, body, content_type) -> (status, payload).
Live mode uses `http_transport` (real HTTPS). Replay mode uses the simulator's transport, so the same
paging and retry code runs in tests and CI without a Boomi account.

API facts this relies on are in docs/boomi_api_notes.md.
"""
from __future__ import annotations

import base64
import json
import random
import time
from typing import Any, Callable, Iterator

Transport = Callable[[str, str, Any, str], "tuple[int, Any]"]

RETRYABLE = {503, 504}  # 503 = busy or rate limit exceeded, 504 = runtime unreachable


class BoomiApiError(RuntimeError):
    def __init__(self, status: int, path: str, payload: Any):
        super().__init__(f"Boomi API {status} on {path}: {payload}")
        self.status = status
        self.path = path
        self.payload = payload


class BoomiClient:
    def __init__(
        self,
        transport: Transport,
        max_retries: int = 5,
        base_delay: float = 1.0,
        max_requests_per_second: float = 4.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._transport = transport
        self._max_retries = max_retries
        self._base_delay = base_delay
        self._min_interval = 1.0 / max_requests_per_second if max_requests_per_second else 0.0
        self._sleep = sleep
        self._clock = clock
        self._last_request = float("-inf")
        self.requests_made = 0
        self.retries_made = 0

    # ---- low level -------------------------------------------------------------------------
    def _throttle(self) -> None:
        wait = self._last_request + self._min_interval - self._clock()
        if wait > 0:
            self._sleep(wait)
        self._last_request = self._clock()

    def call(self, method: str, path: str, body: Any = None, content_type: str = "application/json") -> Any:
        for attempt in range(self._max_retries + 1):
            self._throttle()
            self.requests_made += 1
            status, payload = self._transport(method, path, body, content_type)
            if status < 400:
                return payload
            if status in RETRYABLE and attempt < self._max_retries:
                self.retries_made += 1
                # exponential backoff with full jitter, capped at 60 s
                self._sleep(min(60.0, self._base_delay * 2**attempt) * random.uniform(0.5, 1.0))
                continue
            raise BoomiApiError(status, path, payload)
        raise AssertionError("unreachable")

    # ---- queries ---------------------------------------------------------------------------
    def query_pages(self, object_type: str, query: dict) -> Iterator[list[dict]]:
        """Yields one list of records per page (max 100 per page, per the docs)."""
        page = self.call("POST", f"/{object_type}/query", query)
        while True:
            yield page.get("result", []) or []
            token = page.get("queryToken")
            if not token:
                return
            # queryMore takes ONLY the token as a text/plain body, and tokens expire:
            # always use the newest one.
            page = self.call("POST", f"/{object_type}/queryMore", token, "text/plain")

    def query_all(self, object_type: str, query: dict) -> list[dict]:
        return [row for page in self.query_pages(object_type, query) for row in page]

    def request_process_log(self, execution_id: str, log_level: str = "ALL") -> str | None:
        """Returns the download URL, or None when the runtime is offline (statusCode 504)."""
        resp = self.call("POST", "/ProcessLog", {"executionId": execution_id, "logLevel": log_level})
        if str(resp.get("statusCode")) == "202":
            return resp.get("url")
        return None

    def download(self, url: str) -> bytes:
        return self.call("GET", url, None, "")


def between(prop: str, start: str, end: str) -> dict:
    return {"QueryFilter": {"expression": {"operator": "BETWEEN", "property": prop, "argument": [start, end]}}}


def http_transport(base_url: str, account_id: str, username: str, token: str, timeout: float = 60.0) -> Transport:
    import requests  # imported here so replay mode and unit tests don't need it

    session = requests.Session()
    auth = base64.b64encode(f"BOOMI_TOKEN.{username}:{token}".encode()).decode()
    session.headers.update({"Authorization": f"Basic {auth}", "Accept": "application/json"})
    root = f"{base_url.rstrip('/')}/{account_id}"

    def send(method: str, path: str, body: Any, content_type: str) -> tuple[int, Any]:
        url = path if path.startswith("http") else root + path  # log downloads come back as full URLs
        data = None
        headers = {}
        if body is not None:
            data = body if content_type == "text/plain" else json.dumps(body)
            headers["Content-Type"] = content_type
        try:
            r = session.request(method, url, data=data, headers=headers, timeout=timeout)
        except requests.ConnectionError as exc:
            return 503, {"message": f"connection error: {exc}"}
        except requests.Timeout:
            return 504, {"message": "timeout"}
        if method == "GET" and not r.headers.get("Content-Type", "").startswith("application/json"):
            return r.status_code, r.content
        try:
            return r.status_code, r.json()
        except ValueError:
            return r.status_code, r.text

    return send
