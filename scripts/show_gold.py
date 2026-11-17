"""Print the gold tables after a run:  python scripts/show_gold.py [data_root]"""
import sys

from boomi_ops.transform.lake import Lake, get_spark

lake = Lake(get_spark("show-gold"), sys.argv[1] if len(sys.argv) > 1 else "data")

print("== Error clusters (one row = one incident type) ==")
lake.read("gold_mart_error_clusters").orderBy("runs", ascending=False) \
    .select("process_name", "runs", "days_seen", "error_template").show(20, truncate=90)

print("== Worst process-days by success rate ==")
lake.read("gold_mart_process_sla_daily").orderBy("success_rate") \
    .select("process_name", "run_date", "runs", "errors", "success_rate", "p50_duration_s", "p95_duration_s") \
    .show(10, truncate=False)

print("== Payroll Export SFTP p95 duration by day (slow-upstream drift) ==")
lake.read("gold_mart_process_sla_daily").filter("process_name = 'Payroll Export SFTP'").orderBy("run_date") \
    .select("run_date", "p50_duration_s", "p95_duration_s", "errors").show(truncate=False)

print("== Deployments: error rate 24h before vs after ==")
lake.read("gold_rpt_failures_after_deploy").filter("runs_before > 0").orderBy("deployed_at") \
    .select("component_name", "package_version", "deployed_at", "error_rate_before", "error_rate_after",
            "is_suspicious").show(truncate=False)
