import json
from pyspark.sql import SparkSession

from ingestion import load_raw
from quality import run_quality_checks
from transform import build_metrics
from load import save_metrics

def main():
    spark = (
        SparkSession.builder
        .appName("spark-log-pipeline")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = load_raw(spark, "data/raw/logs.jsonl")

    report = run_quality_checks(df, "schemas/expected_schema.json")
    with open("reports/dq_report.json", "w") as f:
        json.dump(report, f, indent=2)

    metrics = build_metrics(df)
    save_metrics(metrics, "data/processed/service_metrics")

    spark.stop()

if __name__ == "__main__":
    main()
