"""End-to-end Spark + Delta tests. Need Java, so they run in Docker and CI (skipped elsewhere)."""
import shutil
from datetime import datetime, timedelta, timezone

import pytest

if not shutil.which("java"):
    pytest.skip("Spark tests need Java (run them in Docker)", allow_module_level=True)

from pyspark.sql import functions as F  # noqa: E402

from boomi_ops.client import BoomiClient  # noqa: E402
from boomi_ops.fingerprint import template  # noqa: E402
from boomi_ops.ingest import ingest  # noqa: E402
from boomi_ops.simulator import ReplayTransport, World  # noqa: E402
from boomi_ops.transform.lake import Lake, get_spark  # noqa: E402
from boomi_ops.transform.pipeline import boomi_long, error_template, run_transforms  # noqa: E402

START = datetime(2026, 11, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def spark():
    return get_spark("tests")


def pull(world, client, clock, root, hours):
    for h in hours:
        clock["now"] = START + timedelta(hours=h, minutes=10)
        ingest(client, root / "landing", clock["now"], START - timedelta(days=1))


def test_parses_both_number_forms(spark):
    df = spark.createDataFrame([("3832",), ('["Long",3832]',), ('["Long", 0]',), (None,)], ["v"])
    assert [r[0] for r in df.select(boomi_long("v")).collect()] == [3832, 3832, 0, None]


def test_spark_and_python_fingerprint_rules_agree(spark):
    msgs = [e.record["message"] for e in World(START, 5).executions if e.record["message"]]
    msgs = sorted(set(msgs))[:200]
    got = spark.createDataFrame([(m,) for m in msgs], ["m"]).select("m", error_template(F.col("m"))).collect()
    for m, t in got:
        assert t == template(m), m


def test_incremental_runs_converge_and_never_regress(spark, tmp_path):
    world = World(START, 1)
    clock = {"now": START}
    client = BoomiClient(ReplayTransport(world, lambda: clock["now"]), max_requests_per_second=0)
    lake = Lake(spark, tmp_path)

    pull(world, client, clock, tmp_path, range(0, 12))
    run_transforms(lake)
    pull(world, client, clock, tmp_path, range(12, 26))
    run_transforms(lake)

    silver = lake.read("silver_executions")
    assert silver.count() == silver.select("execution_id").distinct().count() == len(world.executions)
    assert silver.filter("NOT is_final").count() == 0  # every running row got its final status
    truth = {e.record["executionId"]: e.record["status"] for e in world.executions}
    assert {r.execution_id: r.status for r in silver.select("execution_id", "status").collect()} == truth

    # Re-load an early batch (it still has running rows) through MERGE: nothing may go back to running.
    from boomi_ops.transform.pipeline import merge_executions
    early = sorted(r[0] for r in lake.read("bronze_execution_records").select("_batch_id").distinct().collect())[:3]
    merge_executions(lake, early)
    assert lake.read("silver_executions").filter("NOT is_final").count() == 0

    # Running again with no new files changes nothing.
    assert run_transforms(lake) == {"new_execution_batches": 0, "new_deployment_batches": 0}


def test_gold_marts(spark, tmp_path):
    world = World(START, 4)
    clock = {"now": START}
    client = BoomiClient(ReplayTransport(world, lambda: clock["now"]), max_requests_per_second=0)
    lake = Lake(spark, tmp_path)
    pull(world, client, clock, tmp_path, range(0, 4 * 24 + 1))
    run_transforms(lake)

    sla = lake.read("gold_mart_process_sla_daily")
    assert sla.select("process_name").distinct().count() == 6
    errors = sum(r.errors for r in sla.collect())
    assert errors == sum(1 for g in world.ground_truth())

    clusters = lake.read("gold_mart_error_clusters")
    assert clusters.agg(F.sum("runs")).first()[0] == errors
    assert clusters.count() < errors / 3  # many failures collapse into few incidents

    rpt = {r.package_version: r for r in lake.read("gold_rpt_failures_after_deploy")
           .filter("component_name = 'Customer Master Sync'").collect()}
    assert rpt["1.1"].is_suspicious  # the bad deploy is flagged
    assert not rpt["1.2"].is_suspicious  # the rollback is not
