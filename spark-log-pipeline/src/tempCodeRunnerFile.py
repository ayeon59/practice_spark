

    # null_counts = df.select([
    # F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns
    # ])
    # print("=== NULL COUNTS ===")
    # null_counts.show(truncate=False)


    # df_auth = df.filter(df.service == "auth")
    # metrics = (
    #     df.groupBy("service")
    #       .agg(
    #           F.count("*").alias("total_logs"),
    #           F.avg("response_time_ms").alias("avg_response_time_ms")
    #       )
    #       .orderBy(F.desc("total_logs"))
    # )

    # print("=== AUTH ONLY ===")
    # df_auth.show(truncate=False)

    # print("=== METRICS BY SERVICE ===")
    # metrics.show(truncate=False)
    # metrics.write.mode("overwrite").parquet("data/processed/service_metrics")
    # print("Saved to data/processed/service_metrics")
