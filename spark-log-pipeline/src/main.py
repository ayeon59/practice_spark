import json
import sys
from pyspark.sql import SparkSession

from ingestion import load_raw
from quality import run_quality_checks
from transform import build_metrics
from load import save_metrics
from catalog import build_catalog

NULL_RATE_THRESHOLD = 0.005  # 0.5%

def _evaluate_quality(report: dict) -> dict:
    failed = []

    if not report.get("schema_ok", False):
        failed.append("schema_ok")

    dq = report.get("dq", {})
    total = dq.get("total_rows", 0) or 1  # avoid division by zero
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

def main():
    spark = (
        SparkSession.builder
        .appName("spark-log-pipeline")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    input_path = "data/raw/logs.jsonl"
    output_path = "data/processed/service_metrics"

    df = load_raw(spark, input_path)

    report = run_quality_checks(df, "schemas/expected_schema.json")
    gate = _evaluate_quality(report)
    report.update(gate)

    with open("reports/dq_report.json", "w") as f:
        json.dump(report, f, indent=2)

    if not report["quality_passed"]:
        print(f"QUALITY GATE FAILED: {report['failed_checks']}")
        spark.stop()
        sys.exit(1)

    metrics = build_metrics(df)
    save_metrics(metrics, output_path)

    catalog = build_catalog(df, metrics, input_path, output_path, schema_version=report.get("schema_version", "unknown"))
    with open("reports/catalog.json", "w") as f:
        json.dump(catalog, f, indent=2)

    spark.stop()

if __name__ == "__main__":
    main()
