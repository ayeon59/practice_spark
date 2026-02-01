import json
import os
import sys
import time
from pyspark.sql import SparkSession, functions as F

from ingestion import load_raw
from quality import run_quality_checks
from transform import build_metrics, build_user_metrics
from load import save_metrics
from catalog import build_catalog
from rdb import load_users_pg
from state import load_watermark, save_watermark

NULL_RATE_THRESHOLD = 0.005  # 0.5%

def _run_step(name, fn):
    start = time.time()
    print(f"[START] {name}")
    result = fn()
    elapsed = round(time.time() - start, 2)
    print(f"[END] {name} ({elapsed}s)")
    return result

def _evaluate_quality(report: dict) -> dict:
    failed = []

    if not report.get("schema_ok", False):
        failed.append("schema_ok")

    dq = report.get("dq", {})
    total = dq.get("total_rows", 0) or 1
    nulls = dq.get("null_counts", {})

    service_null_rate = nulls.get("service_nulls", 0) / total
    response_null_rate = nulls.get("response_time_ms_nulls", 0) / total

    if service_null_rate > NULL_RATE_THRESHOLD:
        failed.append("service_nulls_rate")
    if response_null_rate > NULL_RATE_THRESHOLD:
        failed.append("response_time_ms_nulls_rate")

    if dq.get("negative_response_time_count", 0) > 0:
        failed.append("negative_response_time_count")
    if dq.get("response_time_over_10s_count", 0) > 0:
        failed.append("response_time_over_10s_count")
    if dq.get("invalid_level_count", 0) > 0:
        failed.append("invalid_level_count")

    return {
        "quality_passed": len(failed) == 0,
        "failed_checks": failed,
        "null_rate_threshold": NULL_RATE_THRESHOLD,
        "service_null_rate": round(service_null_rate, 6),
        "response_time_ms_null_rate": round(response_null_rate, 6),
    }

def run_pipeline(
    input_path: str = "data/raw/logs.jsonl",
    output_path: str = "data/processed/service_metrics",
    user_output_path: str = "data/processed/user_metrics",
    schema_path: str = "schemas/expected_schema.json",
    reports_dir: str = "reports",
    state_path: str = "state/last_run.json",
) -> int:
    os.makedirs(reports_dir, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("spark-log-pipeline")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        last_ts = load_watermark(state_path)
        df = _run_step("ingestion", lambda: load_raw(spark, input_path, since_ts=last_ts))

        if df.rdd.isEmpty():
            print("No new data. Exiting.")
            return 0

        report = _run_step("quality", lambda: run_quality_checks(df, schema_path))
        gate = _evaluate_quality(report)
        report.update(gate)

        with open(os.path.join(reports_dir, "dq_report.json"), "w") as f:
            json.dump(report, f, indent=2)

        if not report["quality_passed"]:
            print(f"QUALITY GATE FAILED: {report['failed_checks']}")
            return 1

        metrics = _run_step("transform", lambda: build_metrics(df))
        _run_step("load", lambda: save_metrics(metrics, output_path))
        users_df = _run_step("rdb_load", lambda: load_users_pg(spark))
        user_metrics = _run_step("user_transform", lambda: build_user_metrics(df, users_df))
        _run_step("user_load", lambda: save_metrics(user_metrics, user_output_path))

        catalog = _run_step(
            "catalog",
            lambda: build_catalog(
                df,
                metrics,
                input_path,
                output_path,
                schema_version=report.get("schema_version", "unknown"),
                user_metrics_df=user_metrics,
                user_output_path=user_output_path,
            ),
        )
        with open(os.path.join(reports_dir, "catalog.json"), "w") as f:
            json.dump(catalog, f, indent=2)

        # update watermark to max timestamp in this batch
        max_ts_row = df.select(F.max("timestamp").alias("max_ts")).collect()[0]
        max_ts = max_ts_row["max_ts"]
        if max_ts:
            save_watermark(state_path, max_ts)

        return 0
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(run_pipeline())
