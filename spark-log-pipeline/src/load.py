def save_metrics(df, path: str, fmt: str = "parquet", mode: str = "overwrite"):
    """Persist DataFrame."""
    df.write.format(fmt).mode(mode).save(path)
