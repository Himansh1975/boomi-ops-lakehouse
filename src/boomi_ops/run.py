"""Command line entry point.

  python -m boomi_ops.run replay --start 2026-11-01 --days 7    # no Boomi account needed
  python -m boomi_ops.run check                                   # test the API token, read-only
  python -m boomi_ops.run live --since 2026-11-16                 # needs .env (see .env.example)
  python -m boomi_ops.run lineage crawl                           # crawl components + references (live)
  python -m boomi_ops.run lineage show                            # hotspots, orphans, drift
  python -m boomi_ops.run lineage impact "Partner APIs (httpbin)" # what breaks if I change this?
  python -m boomi_ops.run lineage viewer                          # the clickable graph, as one HTML file
  python -m boomi_ops.run transform                               # landing -> bronze/silver/gold only
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .client import BoomiClient, http_transport
from .ingest import ingest
from .lineage.crawl import crawl
from .lineage.sim_account import SyntheticAccount
from .simulator import ReplayTransport, World
from .timeutil import parse_cli


def _run_all(lake) -> dict:
    from .transform.lineage import run_lineage_transforms
    from .transform.pipeline import run_transforms

    return {**run_transforms(lake), **run_lineage_transforms(lake)}


def _transform(data_root: Path) -> dict:
    from .transform.lake import Lake, get_spark

    return _run_all(Lake(get_spark(), data_root))


def _print_crawl(res) -> None:
    print(f"lineage crawl {res.snapshot}: {res.components} components, {res.new_versions} new versions, "
          f"{res.api_edges} API links, {res.xml_edges} XML links, {res.requests} API calls")
    for f in res.xml_failures:
        print(f"  XML not fetched: {f}")


def replay(start: datetime, days: int, data_root: Path, ingest_every_minutes: int = 60,
           transform: str = "daily", busy_every: int = 7) -> None:
    world = World(start, days)
    clock = {"now": world.start}
    account = SyntheticAccount(world.start, world._bad_deploy_at if days >= 3 else None,
                               world._rollback_at if days >= 4 else None)
    transport = ReplayTransport(world, lambda: clock["now"], busy_every=busy_every, account=account)
    for comp in account.deployable_extras():
        world.add_deployment(comp.id, comp.name)
    client = BoomiClient(transport, base_delay=0.0, max_requests_per_second=0)

    truth = data_root / "sim" / "ground_truth.jsonl"
    truth.parent.mkdir(parents=True, exist_ok=True)
    truth.write_text("".join(json.dumps(r) + "\n" for r in world.ground_truth()))
    print(f"simulated {len(world.executions)} runs, {len(world.ground_truth())} failures "
          f"({world.start:%Y-%m-%d} + {days} days)")

    lake = None
    t = world.start + timedelta(minutes=10)
    end = world.end + timedelta(hours=1)  # one extra pull so runs near midnight finish
    rows = batches = 0
    while t <= end:
        clock["now"] = t
        for b in ingest(client, data_root / "landing", t, world.start - timedelta(days=1)):
            rows += b.rows
            batches += 1
        nxt = t + timedelta(minutes=ingest_every_minutes)
        day_done = nxt.date() != t.date() or nxt > end
        if day_done:  # one lineage crawl a day, like a nightly job
            _print_crawl(crawl(client, data_root / "landing", t, xml_dir=data_root / "lineage_xml"))
        if transform == "daily" and day_done:
            if lake is None:
                from .transform.lake import Lake, get_spark
                lake = Lake(get_spark(), data_root)
            t0 = time.time()
            stats = _run_all(lake)
            print(f"{t:%Y-%m-%d %H:%M} transformed {stats} in {time.time() - t0:.1f}s")
        t = nxt
    print(f"ingested {rows} rows in {batches} batches with {client.requests_made} API calls "
          f"({client.retries_made} retried after 503)")
    if transform == "end":
        print(_transform(data_root))


def _live_client() -> BoomiClient:
    missing = [k for k in ("BOOMI_ACCOUNT_ID", "BOOMI_USERNAME", "BOOMI_API_TOKEN") if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"missing in .env: {', '.join(missing)} (see .env.example)")
    return BoomiClient(http_transport(
        os.environ.get("BOOMI_BASE_URL", "https://api.boomi.com/api/rest/v1"),
        os.environ["BOOMI_ACCOUNT_ID"], os.environ["BOOMI_USERNAME"], os.environ["BOOMI_API_TOKEN"]))


def check(hours: int) -> None:
    """One small read-only call per object type: proves the token works and shows what is there."""
    from collections import Counter

    from .client import BoomiApiError, between
    from .timeutil import to_boomi

    client = _live_client()
    end = datetime.now(timezone.utc).replace(microsecond=0)
    start = end - timedelta(hours=hours)
    for obj, prop in (("ExecutionRecord", "executionTime"), ("DeployedPackage", "deployedDate")):
        try:
            rows = client.query_all(obj, between(prop, to_boomi(start), to_boomi(end)))
        except BoomiApiError as e:
            hint = {401: "token or username wrong (username must be your Boomi login email)",
                    403: "your user lacks the API privilege, or the account has no Platform API access"}
            raise SystemExit(f"{obj}: HTTP {e.status} - {hint.get(e.status, e.payload)}")
        print(f"{obj}: {len(rows)} in the last {hours}h")
        if obj == "ExecutionRecord" and rows:
            print("  by status:", dict(Counter(r.get("status") for r in rows)))
            print("  processes:", sorted({r.get("processName") for r in rows}))
            print("  duration field looks like:", rows[0].get("executionDuration"))
    print(f"OK: {client.requests_made} API calls, {client.retries_made} retries")


def live(since: datetime, data_root: Path) -> None:
    client = _live_client()
    for b in ingest(client, data_root / "landing", datetime.now(timezone.utc).replace(microsecond=0), since):
        print(f"{b.object_type}: {b.rows} rows for {b.window_start} .. {b.window_end}")
    print(_transform(data_root))


def lineage(data_root: Path, fetch_xml: bool) -> None:
    client = _live_client()
    res = crawl(client, data_root / "landing", datetime.now(timezone.utc).replace(microsecond=0),
                fetch_xml=fetch_xml, xml_dir=data_root / "lineage_xml")
    _print_crawl(res)
    print(_transform(data_root))


def main() -> None:
    p = argparse.ArgumentParser(prog="boomi_ops.run")
    p.add_argument("--data-root", default=os.environ.get("DATA_ROOT", "data"), type=Path)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("replay", help="simulate a Boomi account and run the whole pipeline")
    r.add_argument("--start", required=True, type=parse_cli)
    r.add_argument("--days", type=int, default=7)
    r.add_argument("--ingest-every-minutes", type=int, default=60)
    r.add_argument("--transform", choices=["daily", "end", "none"], default="daily")
    lv = sub.add_parser("live", help="pull from a real Boomi account (see .env.example)")
    lv.add_argument("--since", type=parse_cli, default=None,
                    help="first window start (default: 1 day ago); later runs use the watermark")
    ln = sub.add_parser("lineage", help="component lineage: crawl a real account, or report on the lake")
    lsub = ln.add_subparsers(dest="lineage_cmd", required=True)
    lc = lsub.add_parser("crawl", help="crawl components and references from a real account, then transform")
    lc.add_argument("--no-xml", action="store_true", help="skip the XML cross-check (half the API calls)")
    lsub.add_parser("show", help="print hotspots, orphans and drift")
    li = lsub.add_parser("impact", help="what uses this component, and what breaks if it changes")
    li.add_argument("component", help="component name (or part of it) or id")
    lv2 = lsub.add_parser("viewer", help="write the clickable lineage graph as one HTML file")
    lv2.add_argument("--out", type=Path, default=None, help="default: <data-root>/viewer/index.html")
    lv2.add_argument("--source", default="live", help="label shown on the page, e.g. live or replay")
    sub.add_parser("transform", help="landing -> bronze/silver/gold only")
    ck = sub.add_parser("check", help="test the API token with one read-only call per object")
    ck.add_argument("--hours", type=int, default=24 * 7)
    a = p.parse_args()
    if a.cmd == "check":
        check(a.hours)
    elif a.cmd == "replay":
        replay(a.start, a.days, a.data_root, a.ingest_every_minutes, a.transform)
    elif a.cmd == "lineage" and a.lineage_cmd == "crawl":
        lineage(a.data_root, not a.no_xml)
    elif a.cmd == "lineage":
        from .lineage import report
        from .transform.lake import Lake, get_spark

        lake = Lake(get_spark("lineage-report"), a.data_root)
        if a.lineage_cmd == "viewer":
            from .lineage.viewer import build_viewer

            out = build_viewer(lake, a.out or a.data_root / "viewer" / "index.html", a.source)
            print(f"wrote {out} (open it in a browser)")
        else:
            print(report.show(lake) if a.lineage_cmd == "show" else report.impact(lake, a.component))
    elif a.cmd == "live":
        live(a.since or datetime.now(timezone.utc) - timedelta(days=1), a.data_root)
    else:
        print(_transform(a.data_root))


if __name__ == "__main__":
    main()
