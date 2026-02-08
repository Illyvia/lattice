import json
import random
import re
import secrets
import sqlite3
import string
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PAIR_CODE_REGEX = re.compile(r"^[A-Z0-9]{6}$")
NODE_STATE_PENDING = "pending"
NODE_STATE_PAIRED = "paired"

_ADJECTIVES = [
    "friendly",
    "resourceful",
    "steady",
    "bright",
    "nimble",
    "curious",
    "solid",
    "brisk",
    "keen",
    "calm",
]

_NOUNS = [
    "badger",
    "otter",
    "falcon",
    "lynx",
    "beacon",
    "compass",
    "harbor",
    "keyboard",
    "lantern",
    "quartz",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_pair_code(value: str) -> str:
    return (value or "").strip().upper()


def is_valid_pair_code(value: str) -> bool:
    return bool(PAIR_CODE_REGEX.fullmatch(normalize_pair_code(value)))


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, column_def: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    if any(row["name"] == column for row in rows):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_def};")


def _as_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except ValueError:
            return None
    return None


def _normalize_runtime_metrics(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None

    metrics: dict[str, Any] = {}
    cpu_percent = _as_float(value.get("cpu_percent"))
    memory_percent = _as_float(value.get("memory_percent"))
    memory_used_bytes = _as_int(value.get("memory_used_bytes"))
    memory_total_bytes = _as_int(value.get("memory_total_bytes"))
    storage_percent = _as_float(value.get("storage_percent"))
    storage_used_bytes = _as_int(value.get("storage_used_bytes"))
    storage_total_bytes = _as_int(value.get("storage_total_bytes"))

    if cpu_percent is not None:
        metrics["cpu_percent"] = max(0.0, min(100.0, round(cpu_percent, 2)))
    if memory_percent is not None:
        metrics["memory_percent"] = max(0.0, min(100.0, round(memory_percent, 2)))
    if memory_used_bytes is not None:
        metrics["memory_used_bytes"] = max(0, memory_used_bytes)
    if memory_total_bytes is not None:
        metrics["memory_total_bytes"] = max(0, memory_total_bytes)
    if storage_percent is not None:
        metrics["storage_percent"] = max(0.0, min(100.0, round(storage_percent, 2)))
    if storage_used_bytes is not None:
        metrics["storage_used_bytes"] = max(0, storage_used_bytes)
    if storage_total_bytes is not None:
        metrics["storage_total_bytes"] = max(0, storage_total_bytes)

    return metrics or None


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with _connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS nodes (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                pair_code TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL CHECK (state IN ('pending', 'paired')),
                pair_token TEXT UNIQUE,
                created_at TEXT NOT NULL,
                paired_at TEXT,
                last_heartbeat_at TEXT,
                agent_hostname TEXT,
                agent_info_json TEXT,
                agent_commit TEXT,
                last_metrics_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_nodes_state ON nodes (state);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_nodes_pair_code ON nodes (pair_code);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_nodes_pair_token ON nodes (pair_token);

            CREATE TABLE IF NOT EXISTS node_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                meta_json TEXT,
                FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_node_logs_node_id_id ON node_logs (node_id, id);
            """
        )
        _ensure_column(conn, "nodes", "last_metrics_json", "TEXT")
        _ensure_column(conn, "nodes", "agent_commit", "TEXT")


def _node_exists_by_name(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute("SELECT 1 FROM nodes WHERE name = ? LIMIT 1;", (name,)).fetchone()
    return row is not None


def _generate_friendly_name(conn: sqlite3.Connection) -> str:
    base = f"{random.choice(_ADJECTIVES)}-{random.choice(_NOUNS)}"
    if not _node_exists_by_name(conn, base):
        return base

    suffix = 2
    while True:
        candidate = f"{base}-{suffix}"
        if not _node_exists_by_name(conn, candidate):
            return candidate
        suffix += 1


def _generate_unique_pair_code(conn: sqlite3.Connection) -> str:
    alphabet = string.ascii_uppercase + string.digits
    for _ in range(64):
        code = "".join(secrets.choice(alphabet) for _ in range(6))
        exists = conn.execute("SELECT 1 FROM nodes WHERE pair_code = ? LIMIT 1;", (code,)).fetchone()
        if exists is None:
            return code
    raise RuntimeError("Unable to generate unique pair code")


def _generate_unique_pair_token(conn: sqlite3.Connection) -> str:
    for _ in range(64):
        token = secrets.token_urlsafe(32)
        exists = conn.execute("SELECT 1 FROM nodes WHERE pair_token = ? LIMIT 1;", (token,)).fetchone()
        if exists is None:
            return token
    raise RuntimeError("Unable to generate unique pair token")


def _to_public_node(row: sqlite3.Row) -> dict[str, Any]:
    payload = dict(row)
    agent_info_raw = payload.pop("agent_info_json", None)
    metrics_raw = payload.pop("last_metrics_json", None)
    payload["agent_info"] = json.loads(agent_info_raw) if agent_info_raw else None
    payload["runtime_metrics"] = json.loads(metrics_raw) if metrics_raw else None
    payload.pop("pair_token", None)
    return payload


def _to_public_log(row: sqlite3.Row) -> dict[str, Any]:
    payload = dict(row)
    meta_raw = payload.pop("meta_json", None)
    payload["meta"] = json.loads(meta_raw) if meta_raw else None
    return payload


def _insert_node_log(
    conn: sqlite3.Connection,
    node_id: str,
    level: str,
    message: str,
    meta: dict[str, Any] | None = None,
    created_at: str | None = None,
) -> None:
    clean_message = (message or "").strip()
    if not clean_message:
        return
    conn.execute(
        """
        INSERT INTO node_logs (node_id, created_at, level, message, meta_json)
        VALUES (?, ?, ?, ?, ?);
        """,
        (
            node_id,
            created_at or utc_now(),
            (level or "info").strip().lower(),
            clean_message,
            json.dumps(meta) if meta else None,
        ),
    )


def list_nodes(db_path: Path) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        rows = conn.execute("SELECT * FROM nodes ORDER BY created_at DESC;").fetchall()
        return [_to_public_node(row) for row in rows]


def list_node_logs(
    db_path: Path,
    node_id: str,
    limit: int = 200,
    since_id: int | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    clean_node_id = (node_id or "").strip()
    if not clean_node_id:
        return "not_found", []

    try:
        clean_limit = int(limit)
    except (TypeError, ValueError):
        clean_limit = 200
    clean_limit = max(1, min(clean_limit, 500))

    with _connect(db_path) as conn:
        node_row = conn.execute(
            "SELECT 1 FROM nodes WHERE id = ? LIMIT 1;",
            (clean_node_id,),
        ).fetchone()
        if node_row is None:
            return "not_found", []

        if since_id is not None:
            rows = conn.execute(
                """
                SELECT id, node_id, created_at, level, message, meta_json
                FROM node_logs
                WHERE node_id = ? AND id > ?
                ORDER BY id ASC
                LIMIT ?;
                """,
                (clean_node_id, since_id, clean_limit),
            ).fetchall()
        else:
            latest_rows = conn.execute(
                """
                SELECT id, node_id, created_at, level, message, meta_json
                FROM node_logs
                WHERE node_id = ?
                ORDER BY id DESC
                LIMIT ?;
                """,
                (clean_node_id, clean_limit),
            ).fetchall()
            rows = list(reversed(latest_rows))

    return "ok", [_to_public_log(row) for row in rows]


def append_node_log(
    db_path: Path,
    node_id: str,
    level: str,
    message: str,
    meta: dict[str, Any] | None = None,
    timestamp: str | None = None,
) -> str:
    clean_node_id = (node_id or "").strip()
    if not clean_node_id:
        return "not_found"

    with _connect(db_path) as conn:
        node_row = conn.execute(
            "SELECT 1 FROM nodes WHERE id = ? LIMIT 1;",
            (clean_node_id,),
        ).fetchone()
        if node_row is None:
            return "not_found"
        _insert_node_log(
            conn,
            node_id=clean_node_id,
            level=level,
            message=message,
            meta=meta,
            created_at=timestamp,
        )
    return "ok"


def is_valid_node_token(db_path: Path, node_id: str, pair_token: str) -> bool:
    clean_node_id = (node_id or "").strip()
    clean_token = (pair_token or "").strip()
    if not clean_node_id or not clean_token:
        return False

    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM nodes WHERE id = ? AND pair_token = ? LIMIT 1;",
            (clean_node_id, clean_token),
        ).fetchone()
        return row is not None


def create_node(db_path: Path, name: str | None) -> dict[str, Any]:
    with _connect(db_path) as conn:
        clean_name = (name or "").strip()
        if not clean_name:
            clean_name = _generate_friendly_name(conn)

        node_id = str(uuid.uuid4())
        pair_code = _generate_unique_pair_code(conn)
        created_at = utc_now()
        conn.execute(
            """
            INSERT INTO nodes (
                id, name, pair_code, state, pair_token, created_at, paired_at,
                last_heartbeat_at, agent_hostname, agent_info_json, last_metrics_json
            )
            VALUES (?, ?, ?, ?, NULL, ?, NULL, NULL, NULL, NULL, NULL);
            """,
            (node_id, clean_name, pair_code, NODE_STATE_PENDING, created_at),
        )
        _insert_node_log(
            conn,
            node_id=node_id,
            level="info",
            message="Node created and waiting for pairing",
            meta={"pair_code": pair_code},
            created_at=created_at,
        )
        row = conn.execute("SELECT * FROM nodes WHERE id = ?;", (node_id,)).fetchone()
        return _to_public_node(row)


def pair_node(db_path: Path, pair_code: str, agent_info: dict[str, Any] | None) -> tuple[str, dict[str, Any] | None]:
    normalized_code = normalize_pair_code(pair_code)
    if not is_valid_pair_code(normalized_code):
        return "invalid_code", None

    with _connect(db_path) as conn:
        row = conn.execute("SELECT * FROM nodes WHERE pair_code = ? LIMIT 1;", (normalized_code,)).fetchone()
        if row is None:
            return "not_found", None
        if row["state"] != NODE_STATE_PENDING:
            return "already_paired", None

        token = _generate_unique_pair_token(conn)
        paired_at = utc_now()
        agent_hostname = (agent_info or {}).get("hostname")
        conn.execute(
            """
            UPDATE nodes
            SET state = ?, pair_token = ?, paired_at = ?, agent_hostname = ?, agent_info_json = ?
            WHERE id = ?;
            """,
            (
                NODE_STATE_PAIRED,
                token,
                paired_at,
                agent_hostname,
                json.dumps(agent_info or {}),
                row["id"],
            ),
        )
        _insert_node_log(
            conn,
            node_id=row["id"],
            level="info",
            message="Node paired with agent",
            meta={"hostname": agent_hostname} if agent_hostname else None,
            created_at=paired_at,
        )
        return "paired", {
            "node_id": row["id"],
            "node_name": row["name"],
            "pair_token": token,
            "state": NODE_STATE_PAIRED,
        }


def record_heartbeat(
    db_path: Path,
    pair_token: str,
    node_id: str,
    payload: dict[str, Any] | None,
) -> tuple[str, dict[str, Any] | None]:
    token = (pair_token or "").strip()
    if not token:
        return "missing_token", None

    with _connect(db_path) as conn:
        row = conn.execute("SELECT * FROM nodes WHERE pair_token = ? LIMIT 1;", (token,)).fetchone()
        if row is None:
            return "invalid_token", None
        if row["id"] != (node_id or "").strip():
            return "node_mismatch", None

        last_heartbeat_at = (
            (payload or {}).get("timestamp")
            if isinstance((payload or {}).get("timestamp"), str)
            else utc_now()
        )
        status_value = (payload or {}).get("status")
        hostname_value = (payload or {}).get("hostname")
        extra_value = (payload or {}).get("extra")
        usage_value = extra_value.get("usage") if isinstance(extra_value, dict) else None
        commit_value = extra_value.get("git_commit") if isinstance(extra_value, dict) else None
        if not isinstance(commit_value, str) or not commit_value.strip():
            commit_value = None
        else:
            commit_value = commit_value.strip()
        runtime_metrics = _normalize_runtime_metrics(usage_value)
        if runtime_metrics:
            runtime_metrics["updated_at"] = last_heartbeat_at
        metrics_json = json.dumps(runtime_metrics) if runtime_metrics else None
        conn.execute(
            """
            UPDATE nodes
            SET
                last_heartbeat_at = ?,
                last_metrics_json = COALESCE(?, last_metrics_json),
                agent_commit = COALESCE(?, agent_commit)
            WHERE id = ?;
            """,
            (last_heartbeat_at, metrics_json, commit_value, row["id"]),
        )
        log_meta: dict[str, Any] = {}
        if isinstance(hostname_value, str) and hostname_value.strip():
            log_meta["hostname"] = hostname_value.strip()
        if isinstance(extra_value, dict) and extra_value:
            log_meta["extra"] = extra_value
        _insert_node_log(
            conn,
            node_id=row["id"],
            level="info",
            message=f"Heartbeat {status_value}" if isinstance(status_value, str) else "Heartbeat received",
            meta=log_meta or None,
            created_at=last_heartbeat_at,
        )
        updated = conn.execute("SELECT * FROM nodes WHERE id = ? LIMIT 1;", (row["id"],)).fetchone()
        return "ok", _to_public_node(updated)


def delete_node(db_path: Path, node_id: str) -> dict[str, Any] | None:
    with _connect(db_path) as conn:
        clean_id = (node_id or "").strip()
        row = conn.execute("SELECT * FROM nodes WHERE id = ? LIMIT 1;", (clean_id,)).fetchone()
        if row is None:
            return None

        conn.execute("DELETE FROM nodes WHERE id = ?;", (clean_id,))
        return _to_public_node(row)


def rename_node(db_path: Path, node_id: str, name: str) -> tuple[str, dict[str, Any] | None]:
    clean_id = (node_id or "").strip()
    clean_name = (name or "").strip()
    if not clean_id:
        return "not_found", None
    if not clean_name:
        return "invalid_name", None

    with _connect(db_path) as conn:
        row = conn.execute("SELECT * FROM nodes WHERE id = ? LIMIT 1;", (clean_id,)).fetchone()
        if row is None:
            return "not_found", None

        old_name = str(row["name"])
        if old_name != clean_name:
            conn.execute("UPDATE nodes SET name = ? WHERE id = ?;", (clean_name, clean_id))
            _insert_node_log(
                conn,
                node_id=clean_id,
                level="info",
                message="Node renamed",
                meta={"from": old_name, "to": clean_name},
            )

        updated = conn.execute("SELECT * FROM nodes WHERE id = ? LIMIT 1;", (clean_id,)).fetchone()
        return "ok", _to_public_node(updated)
