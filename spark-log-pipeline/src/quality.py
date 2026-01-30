import json
from pyspark.sql.types import StructType
from pyspark.sql import functions as F

#schemas/expected_schema.json 같은 파일을 열어서 dict로 반환
def _load_expected_schema(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)

#Spark 스키마(StructType)를 비교하기 쉬운 dict로 바꿈:
def _schema_to_dict(schema: StructType) -> dict:
    return {
        field.name: {
            "type": field.dataType.simpleString(),
            "nullable": field.nullable,
        }
        for field in schema.fields
    }

def run_quality_checks(df, schema_path: str = "schemas/expected_schema.json") -> dict:
    """Run schema validation + DQ checks. Return a report dict."""
    expected = _load_expected_schema(schema_path)
    expected_fields = {
        f["name"]: {"type": f["type"], "nullable": f["nullable"]}
        for f in expected.get("fields", [])
    }

    actual_fields = _schema_to_dict(df.schema)

    missing_fields = [name for name in expected_fields if name not in actual_fields]
    extra_fields = [name for name in actual_fields if name not in expected_fields]

    type_mismatches = []
    nullable_mismatches = []

    for name, exp in expected_fields.items():
        if name not in actual_fields:
            continue
        act = actual_fields[name]
        if act["type"] != exp["type"]:
            type_mismatches.append({
                "field": name,
                "expected": exp["type"],
                "actual": act["type"],
            })
        if act["nullable"] != exp["nullable"]:
            nullable_mismatches.append({
                "field": name,
                "expected": exp["nullable"],
                "actual": act["nullable"],
            })

    schema_ok = (
        len(missing_fields) == 0 and
        len(type_mismatches) == 0 and
        len(nullable_mismatches) == 0
    )

    # DQ checks
    total_rows = df.count()

    null_counts = df.select(
        F.sum(F.col("service").isNull().cast("int")).alias("service_nulls"),
        F.sum(F.col("response_time_ms").isNull().cast("int")).alias("response_time_ms_nulls"),
    ).collect()[0].asDict()

    negative_response_time = df.filter(F.col("response_time_ms") < 0).count()
    over_10s = df.filter(F.col("response_time_ms") > 10000).count()

    valid_levels = ["INFO", "WARN", "ERROR"]
    invalid_level = df.filter(~F.col("level").isin(valid_levels)).count()

    dq = {
        "total_rows": total_rows,
        "null_counts": null_counts,
        "negative_response_time_count": negative_response_time,
        "response_time_over_10s_count": over_10s,
        "invalid_level_count": invalid_level,
    }

    return {
        "schema_ok": schema_ok,
        "missing_fields": missing_fields,
        "extra_fields": extra_fields,
        "type_mismatches": type_mismatches,
        "nullable_mismatches": nullable_mismatches,
        "dq": dq,
    }
