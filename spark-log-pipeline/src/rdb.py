import os

def _jdbc_url():
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "spark_logs")
    return f"jdbc:postgresql://{host}:{port}/{db}"

def load_users_pg(spark, table: str = "users"):
    """Load users table from PostgreSQL via JDBC."""
    user = os.getenv("PGUSER", os.getenv("USER", "postgres"))
    password = os.getenv("PGPASSWORD", "")

    reader = (
        spark.read.format("jdbc")
        .option("url", _jdbc_url())
        .option("dbtable", table)
        .option("user", user)
        .option("driver", "org.postgresql.Driver")
    )
    if password:
        reader = reader.option("password", password)

    return reader.load()
