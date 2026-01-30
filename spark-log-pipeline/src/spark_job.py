from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    #spark session 생성
    #Driver 생성
    #클러스터 연결
    spark = (
        SparkSession.builder
        .appName("spark-log-pipeline")
        .getOrCreate()
    )
    #jsonl 파일 읽기, 자동스키마추론, 분산로딩, 문자열->구조화 데이터, 결과는 DF
    df = spark.read.json("data/raw/logs.jsonl")
    df.printSchema()



    null_counts = df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns
    ])
    print("=== NULL COUNTS ===")
    null_counts.show(truncate=False)


    df_auth = df.filter(df.service == "auth")
    metrics = (
        df.groupBy("service")
          .agg(
              F.count("*").alias("total_logs"),
              F.avg("response_time_ms").alias("avg_response_time_ms")
          )
          .orderBy(F.desc("total_logs"))
    )

    print("=== AUTH ONLY ===")
    df_auth.show(truncate=False)

    print("=== METRICS BY SERVICE ===")
    metrics.show(truncate=False)
    metrics.write.mode("overwrite").parquet("data/processed/service_metrics")
    print("Saved to data/processed/service_metrics")


    spark.stop()

if __name__ == "__main__":
    main()
