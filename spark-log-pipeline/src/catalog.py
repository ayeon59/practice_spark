from datetime import datetime

def _schema_to_list(schema):
    return [
        {
            "name": f.name,
            "type": f.dataType.simpleString(),
            "nullable": f.nullable,
        }
        for f in schema.fields
    ]

def build_catalog(raw_df, metrics_df, input_path: str, output_path: str, schema_version: str = "unknown") -> dict:
    return {
        "dataset_name": "service_metrics",
        "description": "Service-level aggregated metrics from raw application logs.",
        "created_at": datetime.utcnow().isoformat() + "Z",
        "schema_version": schema_version,
        "input": {
            "path": input_path,
            "format": "jsonl"
        },
        "output": {
            "path": output_path,
            "format": "parquet"
        },
        "raw_schema": _schema_to_list(raw_df.schema),
        "processed_schema": _schema_to_list(metrics_df.schema),
        "metrics": [
            {"name": "total_logs", "description": "Count of logs per service"},
            {"name": "avg_response_time_ms", "description": "Average response time per service"},
            {"name": "p95_response_time_ms", "description": "95th percentile response time per service"},
            {"name": "error_count", "description": "Number of ERROR logs per service"},
            {"name": "error_rate", "description": "Error count divided by total logs per service"}
        ],
        "lineage": {
            "inputs": [{"name": "raw_logs", "path": input_path}],
            "outputs": [{"name": "service_metrics", "path": output_path}],
            "transformations": [
                "drop rows with null service/response_time_ms",
                "group by service and aggregate metrics"
            ]
        }
    }
