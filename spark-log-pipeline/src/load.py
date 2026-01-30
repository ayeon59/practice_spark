def save_metrics(df, path: str):
    """Persist metrics DataFrame."""
    df.write.mode("overwrite").parquet(path)
