"""Replay mode: a fake Boomi account that answers the same API calls as the real one.

Six demo processes, each failing in a known way (the same six planned for the trial account).
Because every failure is caused on purpose, each one has a ground-truth category, which the
triage agent's eval set uses later.

Records follow the documented ExecutionRecord shape, including the quirk that number fields come
back as ["Long", 3832]. A run that has started but not finished at the query time is returned as
INPROCESS, so the pipeline has to handle a status that changes later (late-arriving updates).

Everything is deterministic for a given (start, days, seed).
"""
from __future__ import annotations

import hashlib
import json
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from .timeutil import from_boomi, to_boomi

ACCOUNT = "sim-account-001"
ATOM_ID = str(uuid.uuid5(uuid.NAMESPACE_DNS, "sim-atom-01"))
ATOM_NAME = "Sim-Atom-01"
FINAL = {"COMPLETE", "COMPLETE_WARN", "ERROR", "ABORTED", "DISCARDED"}


def _pid(name: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"boomi-sim/{name}"))


@dataclass(frozen=True)
class Process:
    name: str
    every_minutes: int | None  # None = listener (random arrivals)
    duration_s: tuple[int, int]
    docs: tuple[int, int]

    @property
    def id(self) -> str:
        return _pid(self.name)


PROCESSES = [
    Process("HR Employee Sync", 15, (3, 20), (20, 200)),
    Process("Benefits API Push", 30, (2, 15), (5, 50)),
    Process("Timesheet Import", 60, (20, 120), (100, 900)),
    Process("Payroll Export SFTP", 60, (240, 1200), (50, 400)),
    Process("Vendor Invoice Listener", None, (1, 6), (1, 3)),
    Process("Customer Master Sync", 30, (5, 40), (10, 120)),
]
BY_NAME = {p.name: p for p in PROCESSES}


@dataclass
class Execution:
    record: dict  # the final API record
    start: datetime
    end: datetime
    category: str | None  # ground truth for failures, None for successes
    log: str


@dataclass
class Deployment:
    record: dict
    deployed: datetime


def _rng(*parts: Any) -> random.Random:
    digest = hashlib.sha256("|".join(map(str, parts)).encode()).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


def _exec_id(rng: random.Random, start: datetime) -> str:
    return f"execution-{uuid.UUID(int=rng.getrandbits(128), version=4)}-{start:%Y.%m.%d}"


@dataclass
class World:
    start: datetime
    days: int
    seed: int = 7
    executions: list[Execution] = field(init=False)
    deployments: list[Deployment] = field(init=False)

    def __post_init__(self) -> None:
        self.start = self.start.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        self.end = self.start + timedelta(days=self.days)
        self.deployments = self._make_deployments()
        self.executions = sorted(self._make_executions(), key=lambda e: e.start)
        self.by_id = {e.record["executionId"]: e for e in self.executions}

    # ---- deployments -------------------------------------------------------------------------
    def _make_deployments(self) -> list[Deployment]:
        out = []

        def deploy(proc: Process, when: datetime, version: int, notes: str) -> None:
            rng = _rng(self.seed, "deploy", proc.name, version)
            out.append(
                Deployment(
                    {
                        "@type": "DeployedPackage",
                        "deploymentId": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
                        "componentId": proc.id,
                        "componentName": proc.name,
                        "componentType": "process",
                        "componentVersion": version,
                        "packageVersion": f"1.{version - 1}",
                        "version": version,
                        "deployedDate": to_boomi(when),
                        "deployedBy": "dev.user@example.com",
                        "environmentId": "env-sim-prod",
                        "environmentName": "Production",
                        "active": True,
                        "notes": notes,
                    },
                    when,
                )
            )

        day0 = self.start - timedelta(days=1)
        for p in PROCESSES:
            deploy(p, day0 + timedelta(hours=9), 1, "initial release")
        cms = BY_NAME["Customer Master Sync"]
        if self.days >= 3:
            deploy(cms, self._bad_deploy_at, 2, "phone number formatting")
        if self.days >= 4:
            deploy(cms, self._rollback_at, 3, "rollback to 1.0 logic")
        return out

    @property
    def _bad_deploy_at(self) -> datetime:
        return self.start + timedelta(days=2, hours=10, minutes=12)

    @property
    def _rollback_at(self) -> datetime:
        return self.start + timedelta(days=3, hours=9, minutes=40)

    # ---- executions --------------------------------------------------------------------------
    def _slots(self, p: Process) -> list[tuple[datetime, str]]:
        if p.every_minutes:
            step = timedelta(minutes=p.every_minutes)
            t, out = self.start, []
            while t < self.end:
                out.append((t, "exec_sched"))
                t += step
            return out
        rng = _rng(self.seed, "listener", p.name)
        t, out = self.start, []
        while True:
            t += timedelta(seconds=rng.expovariate(6 / 3600))  # about 6 calls per hour
            if t >= self.end:
                return out
            out.append((t, "exec_listener"))

    def _make_executions(self) -> list[Execution]:
        out = []
        for p in PROCESSES:
            for slot, exec_type in self._slots(p):
                out.append(self._one(p, slot, exec_type))
        return out

    def _one(self, p: Process, slot: datetime, exec_type: str) -> Execution:
        rng = _rng(self.seed, p.name, slot.isoformat())
        start = slot + timedelta(seconds=rng.randint(1, 540) if exec_type == "exec_sched" else 0)
        start = start.replace(microsecond=0)
        dur = rng.randint(*p.duration_s)
        docs_in = rng.randint(*p.docs)
        status, category, message, err_docs = "COMPLETE", None, "", 0
        outcome = self._failure(p, start, rng)
        if outcome:
            status, category, message = outcome
            if category == "timeout":
                dur = rng.randint(300, 330)
            if category in ("auth", "upstream_down"):
                dur = rng.randint(1, 4)
        elif p.name == "Timesheet Import" and rng.random() < 0.12:
            status, err_docs = "COMPLETE_WARN", rng.randint(1, 6)
            message = f"{err_docs} document(s) failed validation in Data Process shape"
        if p.name == "Payroll Export SFTP":  # slow upstream: duration drifts up through the period
            dur = int(dur * (1 + 0.15 * (start - self.start).days))
        end = start + timedelta(seconds=dur)
        exec_id = _exec_id(rng, start)
        docs_out = 0 if status == "ERROR" else docs_in - err_docs
        record = {
            "@type": "ExecutionRecord",
            "executionId": exec_id,
            "originalExecutionId": exec_id,
            "account": ACCOUNT,
            "executionTime": to_boomi(start),
            "status": status,
            "executionType": exec_type,
            "processName": p.name,
            "processId": p.id,
            "atomName": ATOM_NAME,
            "atomId": ATOM_ID,
            "inboundDocumentCount": docs_in if status != "ERROR" or category == "data_quality" else 0,
            "inboundErrorDocumentCount": err_docs,
            "outboundDocumentCount": docs_out,
            "executionDuration": ["Long", dur * 1000],
            "inboundDocumentSize": ["Long", docs_in * rng.randint(400, 2500)],
            "outboundDocumentSize": ["Long", docs_out * rng.randint(400, 2500)],
            "message": message,
            "topLevelExecutionId": exec_id,
            "recordedDate": to_boomi(end),
        }
        return Execution(record, start, end, category, self._log(p, record, category))

    def _failure(self, p: Process, t: datetime, rng: random.Random) -> tuple[str, str, str] | None:
        hour = t.hour
        day = (t - self.start).days
        r = rng.random()
        if p.name == "HR Employee Sync" and r < 0.005:
            return ("ERROR", "upstream_down", "Connection reset by peer while reading from hr-api.example.com")
        if p.name == "Benefits API Push" and day % 3 == 1 and 2 <= hour < 6:
            # token is rotated every 3 days at 02:00 and only fixed at 06:00
            return ("ERROR", "auth", "HTTP 401 Unauthorized: The access token expired or is invalid "
                    "(connection 'Benefits Vendor REST')")
        if p.name == "Timesheet Import" and r < 0.05:
            bad = rng.choice(["31-02-2026", "2026/13/01", ""])
            row = rng.randint(2, 900)
            return ("ERROR", "data_quality", f'Map shape failed at row {row}: Unparseable date: "{bad}" '
                    "for field PunchDate (expected yyyy-MM-dd)")
        if p.name == "Payroll Export SFTP" and r < 0.03 + 0.01 * day:
            return ("ERROR", "timeout", "java.net.SocketTimeoutException: Read timed out after 300000 ms "
                    "(connection 'Payroll SFTP')")
        if p.name == "Vendor Invoice Listener" and hour == 1:
            return ("ERROR", "upstream_down", "Connection refused (Connection refused) to host "
                    "vendor-api.example.com:443")
        if p.name == "Customer Master Sync" and self._bad_deploy_at <= t < self._rollback_at and r < 0.6:
            return ("ERROR", "code_bug", "Map function 'formatPhone' failed: java.lang.NullPointerException "
                    "at line 14 of script")
        if r > 0.9985:
            return ("ABORTED", "runtime_restart", "Process was cancelled: runtime restarted during execution")
        return None

    def _log(self, p: Process, rec: dict, category: str | None) -> str:
        lines = [
            f"{rec['executionTime']} INFO  [{p.name}] Process execution started (id {rec['executionId']})",
            f"{rec['executionTime']} INFO  [{p.name}] Atom {ATOM_NAME}; execution type {rec['executionType']}",
            f"{rec['executionTime']} INFO  [{p.name}] Connector get: {rec['inboundDocumentCount']} document(s)",
        ]
        if category:
            lines.append(f"{rec['recordedDate']} SEVERE [{p.name}] {rec['message']}")
            lines.append(f"{rec['recordedDate']} SEVERE [{p.name}] Process execution failed")
        else:
            lines.append(f"{rec['recordedDate']} INFO  [{p.name}] Process execution completed: {rec['status']}")
        return "\n".join(lines) + "\n"

    # ---- what the API shows at a given moment -------------------------------------------------
    def visible_executions(self, now: datetime) -> list[dict]:
        out = []
        for e in self.executions:
            if e.start > now:
                break
            if e.end <= now:
                out.append(e.record)
            else:
                running = dict(e.record)
                running.update(status="INPROCESS", message="", inboundErrorDocumentCount=0,
                               outboundDocumentCount=0, executionDuration=["Long", 0],
                               outboundDocumentSize=["Long", 0])
                running.pop("recordedDate")
                out.append(running)
        return out

    def visible_deployments(self, now: datetime) -> list[dict]:
        return [d.record for d in self.deployments if d.deployed <= now]

    def ground_truth(self) -> list[dict]:
        return [
            {"executionId": e.record["executionId"], "processName": e.record["processName"],
             "status": e.record["status"], "category": e.category}
            for e in self.executions if e.category
        ]


def _matches(record: dict, expr: dict | None) -> bool:
    if not expr:
        return True
    op = expr["operator"]
    if op in ("and", "or"):
        results = [_matches(record, sub) for sub in expr.get("nestedExpression", [])]
        return all(results) if op == "and" else any(results)
    value = record.get(expr["property"])
    args = expr.get("argument", [])
    if op == "EQUALS":
        return value == args[0]
    if op == "BETWEEN":
        if value is None:
            return False
        if expr["property"] in ("executionTime", "deployedDate", "recordedDate"):
            return from_boomi(args[0]) <= from_boomi(value) <= from_boomi(args[1])
        return args[0] <= value <= args[1]
    if op == "GREATER_THAN_OR_EQUAL":
        return value is not None and value >= args[0]
    raise NotImplementedError(f"simulator does not support operator {op}")


class ReplayTransport:
    """Answers query / queryMore / ProcessLog like the Platform API, against a World at a moving clock."""

    def __init__(self, world: World, clock: Callable[[], datetime], page_size: int = 100,
                 busy_every: int = 0):
        self.world = world
        self.clock = clock
        self.page_size = page_size
        self.busy_every = busy_every  # return a 503 on every Nth call, to exercise retries
        self.calls = 0
        self._cursors: dict[str, list[dict]] = {}

    def __call__(self, method: str, path: str, body: Any, content_type: str) -> tuple[int, Any]:
        self.calls += 1
        if self.busy_every and self.calls % self.busy_every == 0:
            return 503, {"message": "The Boomi server is currently unavailable or your account's rate "
                                    "limits have been exceeded. Retry later."}
        now = self.clock()
        if method == "POST" and path.endswith("/queryMore"):
            if content_type != "text/plain" or not isinstance(body, str):
                return 400, {"message": "queryMore expects the token as a text/plain body"}
            rows = self._cursors.pop(body, None)
            if rows is None:
                return 400, {"message": "query token expired or unknown"}
            return 200, self._page(rows)
        if method == "POST" and path.endswith("/query"):
            obj = path.strip("/").split("/")[0]
            source = {"ExecutionRecord": self.world.visible_executions,
                      "DeployedPackage": self.world.visible_deployments}.get(obj)
            if source is None:
                return 410, {"message": "Endpoint is invalid or no longer exists."}
            expr = (body or {}).get("QueryFilter", {}).get("expression")
            rows = [json.loads(json.dumps(r)) for r in source(now) if _matches(r, expr)]
            return 200, self._page(rows)
        if method == "POST" and path == "/ProcessLog":
            exec_id = body["executionId"]
            if exec_id not in self.world.by_id:
                return 403, {"message": "Access denied due to insufficient permissions."}
            return 200, {"@type": "LogDownload", "url": f"replay://ProcessLog/{exec_id}",
                         "message": "Beginning download.", "statusCode": 202}
        if method == "GET" and path.startswith("replay://ProcessLog/"):
            return 200, self.world.by_id[path.rsplit("/", 1)[1]].log.encode()
        return 410, {"message": "Endpoint is invalid or no longer exists."}

    def _page(self, rows: list[dict]) -> dict:
        head, rest = rows[: self.page_size], rows[self.page_size:]
        page = {"@type": "QueryResult", "result": head, "numberOfResults": len(head)}
        if rest:
            token = uuid.uuid4().hex
            self._cursors[token] = rest
            page["queryToken"] = token
        return page
