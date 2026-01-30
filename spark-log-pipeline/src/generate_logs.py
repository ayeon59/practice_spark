import json
import random
from datetime import datetime, timedelta

SERVICES = ["auth", "payment", "search", "feed", "profile"]
LEVELS = ["INFO", "WARN", "ERROR"]

def main(n: int = 1_000_000, out_path: str = "data/raw/logs.jsonl"):
    start = datetime.now() - timedelta(days=7)

    with open(out_path, "w") as f:
        for i in range(n):
            ts = start + timedelta(seconds=i % (7 * 24 * 3600))
            row = {
                "timestamp": ts.isoformat(timespec="seconds"),
                "service": random.choice(SERVICES),
                "level": random.choices(LEVELS, weights=[0.92, 0.06, 0.02])[0],
                "response_time_ms": max(1, int(random.gauss(180, 60))),
                "user_id": random.randint(1, 200_000),
                "request_id": f"req_{i}",
            }

            # 데이터 품질 체크용: 일부 결측/이상치 섞기
            if random.random() < 0.002:
                row["response_time_ms"] = None
            if random.random() < 0.001:
                row["service"] = None
            if random.random() < 0.001:
                row["response_time_ms"] = 5000

            f.write(json.dumps(row) + "\n")

if __name__ == "__main__":
    main()
