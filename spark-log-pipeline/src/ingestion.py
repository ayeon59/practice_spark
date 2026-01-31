from pyspark.sql import functions as F

def load_raw(spark, path: str, since_ts: str | None = None):
    """Load raw logs as DataFrame. Optionally filter by watermark."""
    df = spark.read.json(path)

    if since_ts:
        df = df.filter(
            F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss") >
            F.to_timestamp(F.lit(since_ts), "yyyy-MM-dd'T'HH:mm:ss")
        )

    return df
