from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = (
        SparkSession.builder
        .appName("spark-log-pipeline")
        .getOrCreate()
    )

    # 로그를 너무 많이 찍지 않게
    spark.sparkContext.setLogLevel("WARN")

    # 1) Load
    df = spark.read.json("data/raw/logs.jsonl")

    # 2) Basic schema / quality checks
    print("=== SCHEMA ===")
    df.printSchema()

    print("=== ROW COUNT (ACTION) ===")
    total = df.count()
    print(f"total_rows={total}")

    print("=== NULL COUNTS ===")
    null_counts = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns
    ])
    null_counts.show(truncate=False)

    # 3) Clean (minimum)
    df_clean = df.dropna(subset=["service", "response_time_ms"])

    # 4) Aggregate (Transformation)
    metrics = (
        df_clean.groupBy("service")
        .agg(
            F.count("*").alias("total_logs"),
            F.avg("response_time_ms").alias("avg_response_time_ms"),
            F.expr("percentile_approx(response_time_ms, 0.95)").alias("p95_response_time_ms"),
            F.sum((F.col("level") == "ERROR").cast("int")).alias("error_count"),
        )
        .withColumn("error_rate", F.col("error_count") / F.col("total_logs"))
        .orderBy(F.desc("total_logs"))
    )

    print("=== METRICS BY SERVICE ===")
    metrics.show(truncate=False)

    # 5) Save (Action)
    metrics.write.mode("overwrite").parquet("data/processed/service_metrics")
    print("Saved: data/processed/service_metrics")

    spark.stop()

if __name__ == "__main__":
    main()
