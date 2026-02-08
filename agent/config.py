import json
import re
from dataclasses import dataclass
from pathlib import Path


PAIR_CODE_PATTERN = re.compile(r"^[A-Z0-9]{6}$")
DEFAULT_CONFIG = {
    "master_url": "http://127.0.0.1:8000",
    "pair_code": "ABC123",
    "pair_retry_seconds": 5,
    "heartbeat_interval_seconds": 10,
    "heartbeat_timeout_seconds": 5,
}


@dataclass(frozen=True)
class AgentConfig:
    master_url: str
    pair_code: str
    pair_retry_seconds: int
    heartbeat_interval_seconds: int
    heartbeat_timeout_seconds: int


def _require_int(payload: dict, key: str, min_value: int = 1) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or value < min_value:
        raise ValueError(f"{key} must be an integer >= {min_value}")
    return value


def load_config(path: Path) -> AgentConfig:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(DEFAULT_CONFIG, indent=2) + "\n", encoding="utf-8")

    payload = json.loads(path.read_text(encoding="utf-8"))
    master_url = str(payload.get("master_url", "")).strip().rstrip("/")
    pair_code = str(payload.get("pair_code", "")).strip().upper()

    if not master_url.startswith("http://") and not master_url.startswith("https://"):
        raise ValueError("master_url must start with http:// or https://")
    if not PAIR_CODE_PATTERN.fullmatch(pair_code):
        raise ValueError("pair_code must be 6 alphanumeric characters")

    return AgentConfig(
        master_url=master_url,
        pair_code=pair_code,
        pair_retry_seconds=_require_int(payload, "pair_retry_seconds", min_value=1),
        heartbeat_interval_seconds=_require_int(payload, "heartbeat_interval_seconds", min_value=1),
        heartbeat_timeout_seconds=_require_int(payload, "heartbeat_timeout_seconds", min_value=1),
    )
