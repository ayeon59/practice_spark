from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

# repo root = .../spark-log-pipeline
BASE_DIR = Path(__file__).resolve().parents[2]

with DAG(
    dag_id="spark_log_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["spark", "quality", "catalog"],
) as dag:
    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command=f"cd {BASE_DIR} && python src/main.py",
    )

    run_pipeline
