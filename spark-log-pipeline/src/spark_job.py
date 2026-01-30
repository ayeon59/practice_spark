"""Spark 로그 분석 메인 작업"""
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def create_spark_session(app_name: str = "LogAnalysisPipeline") -> SparkSession:
    """Spark 세션 생성"""
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()


def get_log_schema() -> StructType:
    """로그 데이터 스키마 정의"""
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("level", StringType(), True),
        StructField("method", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("response_time_ms", IntegerType(), True),
        StructField("user_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("message", StringType(), True),
    ])


def analyze_logs(spark: SparkSession, input_path: str, output_path: str) -> None:
    """로그 데이터 분석 및 결과 저장"""

    # 데이터 로드
    df = spark.read.json(input_path, schema=get_log_schema())

    print(f"Total log entries: {df.count()}")
    df.printSchema()

    # 1. 로그 레벨별 집계
    level_stats = df.groupBy("level").count().orderBy(F.desc("count"))
    print("\n=== Log Level Distribution ===")
    level_stats.show()

    # 2. 엔드포인트별 평균 응답 시간
    endpoint_stats = df.groupBy("endpoint").agg(
        F.count("*").alias("request_count"),
        F.avg("response_time_ms").alias("avg_response_time"),
        F.max("response_time_ms").alias("max_response_time")
    ).orderBy(F.desc("request_count"))
    print("\n=== Endpoint Statistics ===")
    endpoint_stats.show()

    # 3. HTTP 상태 코드 분포
    status_stats = df.groupBy("status_code").count().orderBy("status_code")
    print("\n=== Status Code Distribution ===")
    status_stats.show()

    # 4. 에러 로그 필터링 (ERROR 레벨 또는 5xx 상태 코드)
    error_logs = df.filter(
        (F.col("level") == "ERROR") | (F.col("status_code") >= 500)
    )
    print(f"\n=== Error Logs Count: {error_logs.count()} ===")

    # 결과 저장
    output_dir = Path(output_path)

    level_stats.write.mode("overwrite").json(str(output_dir / "level_stats"))
    endpoint_stats.write.mode("overwrite").json(str(output_dir / "endpoint_stats"))
    status_stats.write.mode("overwrite").json(str(output_dir / "status_stats"))
    error_logs.write.mode("overwrite").json(str(output_dir / "error_logs"))

    print(f"\nResults saved to {output_path}")


def main():
    """메인 실행 함수"""
    base_path = Path(__file__).parent.parent
    input_path = str(base_path / "data" / "raw" / "logs.json")
    output_path = str(base_path / "data" / "processed")

    spark = create_spark_session()

    try:
        analyze_logs(spark, input_path, output_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
