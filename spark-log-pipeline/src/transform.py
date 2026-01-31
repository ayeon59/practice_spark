from pyspark.sql import functions as F

def build_metrics(df):
    """Transform raw logs into service-level metrics."""
    df_clean = df.dropna(subset=["service", "response_time_ms"])

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

    return metrics

def build_user_metrics(df, users_df):
    """Join logs with users and aggregate by user plan."""
    joined = df.join(users_df, on="user_id", how="left")

    user_metrics = (
        joined.groupBy("plan")
        .agg(
            F.count("*").alias("total_logs"),
            F.avg("response_time_ms").alias("avg_response_time_ms"),
            F.sum((F.col("level") == "ERROR").cast("int")).alias("error_count"),
        )
        .withColumn("error_rate", F.col("error_count") / F.col("total_logs"))
        .orderBy(F.desc("total_logs"))
    )

    return user_metrics
