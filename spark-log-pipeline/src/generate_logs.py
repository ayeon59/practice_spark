"""더미 로그 데이터 생성 스크립트"""
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()

LOG_LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]
ENDPOINTS = ["/api/users", "/api/products", "/api/orders", "/api/auth/login", "/api/health"]
HTTP_METHODS = ["GET", "POST", "PUT", "DELETE"]
STATUS_CODES = [200, 201, 400, 401, 403, 404, 500]


def generate_log_entry() -> dict:
    """단일 로그 엔트리 생성"""
    timestamp = datetime.now() - timedelta(
        hours=random.randint(0, 72),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )

    return {
        "timestamp": timestamp.isoformat(),
        "level": random.choice(LOG_LEVELS),
        "method": random.choice(HTTP_METHODS),
        "endpoint": random.choice(ENDPOINTS),
        "status_code": random.choice(STATUS_CODES),
        "response_time_ms": random.randint(10, 2000),
        "user_id": fake.uuid4()[:8],
        "ip_address": fake.ipv4(),
        "message": fake.sentence()
    }


def generate_logs(num_entries: int = 1000, output_path: str = None) -> None:
    """로그 파일 생성"""
    if output_path is None:
        output_path = Path(__file__).parent.parent / "data" / "raw" / "logs.json"

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logs = [generate_log_entry() for _ in range(num_entries)]

    with open(output_path, "w", encoding="utf-8") as f:
        for log in logs:
            f.write(json.dumps(log) + "\n")

    print(f"Generated {num_entries} log entries to {output_path}")


if __name__ == "__main__":
    generate_logs(1000)
