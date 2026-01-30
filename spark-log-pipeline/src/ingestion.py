def load_raw(spark, path: str):
    """Load raw logs as DataFrame."""
    return spark.read.json(path)
