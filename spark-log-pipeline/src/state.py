import json
from pathlib import Path

def load_watermark(path: str) -> str:
    p = Path(path)
    if not p.exists():
        return "1970-01-01T00:00:00"
    with p.open("r") as f:
        data = json.load(f)
    return data.get("last_processed_timestamp", "1970-01-01T00:00:00")

def save_watermark(path: str, timestamp: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w") as f:
        json.dump({"last_processed_timestamp": timestamp}, f, indent=2)
