import json
import random
import re
import secrets
import sqlite3
import string
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


PAIR_CODE_REGEX = re.compile(r"^[A-Z0-9]{6}$")
NODE_STATE_PENDING = "pending"
NODE_STATE_PAIRED = "paired"
VM_NAME_REGEX = re.compile(r"^[a-z0-9-]{3,32}$")

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


def _safe_json_loads(value: Any) -> Any:
    if not isinstance(value, str) or not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


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


def _seed_vm_images(conn: sqlite3.Connection) -> None:
    row = conn.execute("SELECT COUNT(*) AS count FROM vm_images;").fetchone()
    if row and int(row["count"] or 0) > 0:
        return

    created_at = utc_now()
    defaults = [
        {
            "id": "ubuntu-24-04",
            "name": "Ubuntu 24.04 LTS",
            "os_family": "linux",
            "source_url": "https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img",
            "sha256": None,
            "default_username": "ubuntu",
        },
        {
            "id": "debian-12",
            "name": "Debian 12",
            "os_family": "linux",
            "source_url": "https://cloud.debian.org/images/cloud/bookworm/latest/debian-12-genericcloud-amd64.qcow2",
            "sha256": None,
            "default_username": "debian",
        },
    ]
    for image in defaults:
        conn.execute(
            """
            INSERT INTO vm_images (
                id, name, os_family, source_url, sha256,
                default_username, cloud_init_enabled, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, 1, ?);
            """,
            (
                image["id"],
                image["name"],
                image["os_family"],
                image["source_url"],
                image["sha256"],
                image["default_username"],
                created_at,
            ),
        )


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
                last_metrics_json TEXT,
                capabilities_json TEXT
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

            CREATE TABLE IF NOT EXISTS vm_images (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                os_family TEXT NOT NULL,
                source_url TEXT NOT NULL,
                sha256 TEXT,
                default_username TEXT NOT NULL,
                cloud_init_enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS node_vms (
                id TEXT PRIMARY KEY,
                node_id TEXT NOT NULL,
                name TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('creating', 'running', 'stopped', 'rebooting', 'deleting', 'error', 'unknown')),
                provider TEXT NOT NULL,
                domain_name TEXT NOT NULL UNIQUE,
                domain_uuid TEXT,
                image_id TEXT NOT NULL,
                vcpu INTEGER NOT NULL,
                memory_mb INTEGER NOT NULL,
                disk_gb INTEGER NOT NULL,
                bridge TEXT NOT NULL,
                ip_address TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE,
                FOREIGN KEY (image_id) REFERENCES vm_images(id)
            );

            CREATE INDEX IF NOT EXISTS idx_node_vms_node_id ON node_vms (node_id);
            CREATE INDEX IF NOT EXISTS idx_node_vms_state ON node_vms (state);

            CREATE TABLE IF NOT EXISTS vm_operations (
                id TEXT PRIMARY KEY,
                node_id TEXT NOT NULL,
                vm_id TEXT,
                operation_type TEXT NOT NULL CHECK (operation_type IN ('create', 'start', 'stop', 'reboot', 'delete', 'sync')),
                status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
                request_json TEXT,
                result_json TEXT,
                error TEXT,
                created_at TEXT NOT NULL,
                started_at TEXT,
                ended_at TEXT,
                FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE,
                FOREIGN KEY (vm_id) REFERENCES node_vms(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_vm_operations_node_vm_created ON vm_operations (node_id, vm_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_vm_operations_status ON vm_operations (status);
            """
        )
        _ensure_column(conn, "nodes", "last_metrics_json", "TEXT")
        _ensure_column(conn, "nodes", "agent_commit", "TEXT")
        _ensure_column(conn, "nodes", "capabilities_json", "TEXT")
        _seed_vm_images(conn)


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
    capabilities_raw = payload.pop("capabilities_json", None)
    payload["agent_info"] = _safe_json_loads(agent_info_raw)
    payload["runtime_metrics"] = _safe_json_loads(metrics_raw)
    payload["capabilities"] = _safe_json_loads(capabilities_raw)
    payload.pop("pair_token", None)
    return payload


def _to_public_log(row: sqlite3.Row) -> dict[str, Any]:
    payload = dict(row)
    meta_raw = payload.pop("meta_json", None)
    payload["meta"] = _safe_json_loads(meta_raw)
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


def get_node_by_id(db_path: Path, node_id: str) -> dict[str, Any] | None:
    clean_node_id = (node_id or "").strip()
    if not clean_node_id:
        return None
    with _connect(db_path) as conn:
        row = conn.execute("SELECT * FROM nodes WHERE id = ? LIMIT 1;", (clean_node_id,)).fetchone()
        if row is None:
            return None
        return _to_public_node(row)


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
                last_heartbeat_at, agent_hostname, agent_info_json, last_metrics_json, capabilities_json
            )
            VALUES (?, ?, ?, ?, NULL, ?, NULL, NULL, NULL, NULL, NULL, NULL);
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
        vm_capability = extra_value.get("vm") if isinstance(extra_value, dict) else None
        commit_value = extra_value.get("git_commit") if isinstance(extra_value, dict) else None
        if not isinstance(commit_value, str) or not commit_value.strip():
            commit_value = None
        else:
            commit_value = commit_value.strip()
        runtime_metrics = _normalize_runtime_metrics(usage_value)
        if runtime_metrics:
            runtime_metrics["updated_at"] = last_heartbeat_at
        metrics_json = json.dumps(runtime_metrics) if runtime_metrics else None
        capabilities_json = None
        if isinstance(vm_capability, dict):
            capabilities_json = json.dumps({"vm": vm_capability})
        conn.execute(
            """
            UPDATE nodes
            SET
                last_heartbeat_at = ?,
                last_metrics_json = COALESCE(?, last_metrics_json),
                agent_commit = COALESCE(?, agent_commit),
                capabilities_json = COALESCE(?, capabilities_json)
            WHERE id = ?;
            """,
            (last_heartbeat_at, metrics_json, commit_value, capabilities_json, row["id"]),
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


def _to_public_vm_image(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    payload["cloud_init_enabled"] = bool(payload.get("cloud_init_enabled", 0))
    return payload


def _to_public_vm_operation(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    payload["request"] = _safe_json_loads(payload.pop("request_json", None))
    payload["result"] = _safe_json_loads(payload.pop("result_json", None))
    return payload


def _to_public_vm(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    last_op_raw = payload.pop("last_operation_json", None)
    payload["last_operation"] = _safe_json_loads(last_op_raw)
    return payload


def _fetch_latest_vm_operation(conn: sqlite3.Connection, vm_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT id, operation_type, status, created_at, started_at, ended_at, error
        FROM vm_operations
        WHERE vm_id = ?
        ORDER BY created_at DESC
        LIMIT 1;
        """,
        (vm_id,),
    ).fetchone()
    if row is None:
        return None
    return dict(row)


def _node_vm_capability_ready(node_row: sqlite3.Row) -> bool:
    capabilities = _safe_json_loads(node_row["capabilities_json"])
    if not isinstance(capabilities, dict):
        return False
    vm_capability = capabilities.get("vm")
    if not isinstance(vm_capability, dict):
        return False
    return bool(vm_capability.get("ready") is True)


def list_vm_images(db_path: Path) -> list[dict[str, Any]]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT id, name, os_family, source_url, sha256, default_username, cloud_init_enabled, created_at
            FROM vm_images
            ORDER BY name ASC;
            """
        ).fetchall()
        return [_to_public_vm_image(row) for row in rows]


def list_node_vms(db_path: Path, node_id: str) -> tuple[str, list[dict[str, Any]]]:
    clean_node_id = (node_id or "").strip()
    if not clean_node_id:
        return "not_found", []

    with _connect(db_path) as conn:
        node_row = conn.execute("SELECT * FROM nodes WHERE id = ? LIMIT 1;", (clean_node_id,)).fetchone()
        if node_row is None:
            return "not_found", []

        rows = conn.execute(
            """
            SELECT
                nv.*,
                vi.name AS image_name
            FROM node_vms nv
            INNER JOIN vm_images vi ON vi.id = nv.image_id
            WHERE nv.node_id = ?
            ORDER BY nv.created_at DESC;
            """,
            (clean_node_id,),
        ).fetchall()

        result: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            latest_operation = _fetch_latest_vm_operation(conn, payload["id"])
            payload["last_operation_json"] = json.dumps(latest_operation) if latest_operation else None
            result.append(_to_public_vm(payload))

        return "ok", result


def get_node_vm(db_path: Path, node_id: str, vm_id: str) -> tuple[str, dict[str, Any] | None]:
    clean_node_id = (node_id or "").strip()
    clean_vm_id = (vm_id or "").strip()
    if not clean_node_id:
        return "not_found", None
    if not clean_vm_id:
        return "vm_not_found", None

    with _connect(db_path) as conn:
        node_row = conn.execute("SELECT * FROM nodes WHERE id = ? LIMIT 1;", (clean_node_id,)).fetchone()
        if node_row is None:
            return "not_found", None

        row = conn.execute(
            """
            SELECT nv.*, vi.name AS image_name
            FROM node_vms nv
            INNER JOIN vm_images vi ON vi.id = nv.image_id
            WHERE nv.node_id = ? AND nv.id = ?
            LIMIT 1;
            """,
            (clean_node_id, clean_vm_id),
        ).fetchone()
        if row is None:
            return "vm_not_found", None

        payload = dict(row)
        latest_operation = _fetch_latest_vm_operation(conn, payload["id"])
        payload["last_operation_json"] = json.dumps(latest_operation) if latest_operation else None
        return "ok", _to_public_vm(payload)


def list_vm_operations(
    db_path: Path,
    node_id: str,
    vm_id: str,
    limit: int = 50,
) -> tuple[str, list[dict[str, Any]]]:
    clean_node_id = (node_id or "").strip()
    clean_vm_id = (vm_id or "").strip()
    if not clean_node_id:
        return "not_found", []
    if not clean_vm_id:
        return "vm_not_found", []

    try:
        clean_limit = int(limit)
    except (TypeError, ValueError):
        clean_limit = 50
    clean_limit = max(1, min(clean_limit, 200))

    with _connect(db_path) as conn:
        node_row = conn.execute("SELECT * FROM nodes WHERE id = ? LIMIT 1;", (clean_node_id,)).fetchone()
        if node_row is None:
            return "not_found", []

        vm_row = conn.execute(
            "SELECT 1 FROM node_vms WHERE id = ? AND node_id = ? LIMIT 1;",
            (clean_vm_id, clean_node_id),
        ).fetchone()
        if vm_row is None:
            return "vm_not_found", []

        rows = conn.execute(
            """
            SELECT *
            FROM vm_operations
            WHERE node_id = ? AND vm_id = ?
            ORDER BY created_at DESC
            LIMIT ?;
            """,
            (clean_node_id, clean_vm_id, clean_limit),
        ).fetchall()

        return "ok", [_to_public_vm_operation(row) for row in rows]


def _parse_vm_create_payload(payload: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
    name = str(payload.get("name", "")).strip()
    if not VM_NAME_REGEX.fullmatch(name):
        return "vm name must match ^[a-z0-9-]{3,32}$", None

    image_id = str(payload.get("image_id", "")).strip()
    if not image_id:
        return "image_id is required", None

    bridge = str(payload.get("bridge", "br0")).strip() or "br0"

    vcpu = _as_int(payload.get("vcpu"))
    memory_mb = _as_int(payload.get("memory_mb"))
    disk_gb = _as_int(payload.get("disk_gb"))

    if vcpu is None or not (1 <= vcpu <= 32):
        return "vcpu must be between 1 and 32", None
    if memory_mb is None or not (512 <= memory_mb <= 262144):
        return "memory_mb must be between 512 and 262144", None
    if disk_gb is None or not (10 <= disk_gb <= 4096):
        return "disk_gb must be between 10 and 4096", None

    guest = payload.get("guest")
    if not isinstance(guest, dict):
        return "guest is required", None

    guest_username = str(guest.get("username", "")).strip()
    guest_password = str(guest.get("password", "")).strip()
    if not guest_username:
        return "guest.username is required", None
    if not guest_password:
        return "guest.password is required", None

    normalized = {
        "name": name,
        "image_id": image_id,
        "bridge": bridge,
        "vcpu": vcpu,
        "memory_mb": memory_mb,
        "disk_gb": disk_gb,
        "guest_username": guest_username,
        "guest_password": guest_password,
    }
    return "", normalized


def create_vm_request(
    db_path: Path,
    node_id: str,
    payload: dict[str, Any],
) -> tuple[str, dict[str, Any] | None]:
    clean_node_id = (node_id or "").strip()
    if not clean_node_id:
        return "not_found", None

    validation_error, normalized = _parse_vm_create_payload(payload)
    if validation_error:
        return "invalid_payload", {"error": validation_error}
    assert normalized is not None

    with _connect(db_path) as conn:
        node_row = conn.execute("SELECT * FROM nodes WHERE id = ? LIMIT 1;", (clean_node_id,)).fetchone()
        if node_row is None:
            return "not_found", None
        if node_row["state"] != NODE_STATE_PAIRED:
            return "node_not_paired", None
        if not _node_vm_capability_ready(node_row):
            return "capability_not_ready", None

        image_row = conn.execute(
            "SELECT * FROM vm_images WHERE id = ? LIMIT 1;",
            (normalized["image_id"],),
        ).fetchone()
        if image_row is None:
            return "image_not_found", None

        existing = conn.execute(
            "SELECT 1 FROM node_vms WHERE node_id = ? AND name = ? LIMIT 1;",
            (clean_node_id, normalized["name"]),
        ).fetchone()
        if existing is not None:
            return "conflict", {"error": "vm name already exists on this node"}

        vm_id = str(uuid.uuid4())
        operation_id = str(uuid.uuid4())
        now = utc_now()
        domain_name = f"lattice-{vm_id.replace('-', '')[:10]}"

        conn.execute(
            """
            INSERT INTO node_vms (
                id, node_id, name, state, provider, domain_name, domain_uuid,
                image_id, vcpu, memory_mb, disk_gb, bridge, ip_address,
                last_error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, NULL, NULL, ?, ?);
            """,
            (
                vm_id,
                clean_node_id,
                normalized["name"],
                "creating",
                "libvirt",
                domain_name,
                normalized["image_id"],
                normalized["vcpu"],
                normalized["memory_mb"],
                normalized["disk_gb"],
                normalized["bridge"],
                now,
                now,
            ),
        )

        redacted_request = {
            "name": normalized["name"],
            "image_id": normalized["image_id"],
            "bridge": normalized["bridge"],
            "vcpu": normalized["vcpu"],
            "memory_mb": normalized["memory_mb"],
            "disk_gb": normalized["disk_gb"],
            "guest": {
                "username": normalized["guest_username"],
                "password": "***redacted***",
            },
        }
        conn.execute(
            """
            INSERT INTO vm_operations (
                id, node_id, vm_id, operation_type, status,
                request_json, result_json, error,
                created_at, started_at, ended_at
            ) VALUES (?, ?, ?, 'create', 'queued', ?, NULL, NULL, ?, NULL, NULL);
            """,
            (
                operation_id,
                clean_node_id,
                vm_id,
                json.dumps(redacted_request),
                now,
            ),
        )

        _insert_node_log(
            conn,
            node_id=clean_node_id,
            level="info",
            message=f"VM create queued name={normalized['name']} vm_id={vm_id}",
            meta={"operation_id": operation_id, "vm_id": vm_id},
        )

        vm_row = conn.execute(
            """
            SELECT nv.*, vi.name AS image_name
            FROM node_vms nv
            INNER JOIN vm_images vi ON vi.id = nv.image_id
            WHERE nv.id = ? LIMIT 1;
            """,
            (vm_id,),
        ).fetchone()
        op_row = conn.execute("SELECT * FROM vm_operations WHERE id = ? LIMIT 1;", (operation_id,)).fetchone()

        command = {
            "type": "command",
            "command_type": "vm_create",
            "command_id": operation_id,
            "operation_id": operation_id,
            "vm_id": vm_id,
            "created_at": now,
            "spec": {
                "vm_id": vm_id,
                "name": normalized["name"],
                "domain_name": domain_name,
                "vcpu": normalized["vcpu"],
                "memory_mb": normalized["memory_mb"],
                "disk_gb": normalized["disk_gb"],
                "bridge": normalized["bridge"],
                "image": {
                    "id": image_row["id"],
                    "name": image_row["name"],
                    "source_url": image_row["source_url"],
                    "sha256": image_row["sha256"],
                    "default_username": image_row["default_username"],
                },
                "guest": {
                    "username": normalized["guest_username"],
                    "password": normalized["guest_password"],
                },
            },
        }

        return "ok", {
            "vm": _to_public_vm(vm_row),
            "operation": _to_public_vm_operation(op_row),
            "command": command,
        }


def queue_vm_action(
    db_path: Path,
    node_id: str,
    vm_id: str,
    action: str,
) -> tuple[str, dict[str, Any] | None]:
    clean_node_id = (node_id or "").strip()
    clean_vm_id = (vm_id or "").strip()
    action_name = (action or "").strip().lower()
    if action_name not in {"start", "stop", "reboot", "delete"}:
        return "invalid_action", None

    with _connect(db_path) as conn:
        node_row = conn.execute("SELECT * FROM nodes WHERE id = ? LIMIT 1;", (clean_node_id,)).fetchone()
        if node_row is None:
            return "not_found", None
        if node_row["state"] != NODE_STATE_PAIRED:
            return "node_not_paired", None
        if not _node_vm_capability_ready(node_row):
            return "capability_not_ready", None

        vm_row = conn.execute(
            "SELECT * FROM node_vms WHERE id = ? AND node_id = ? LIMIT 1;",
            (clean_vm_id, clean_node_id),
        ).fetchone()
        if vm_row is None:
            return "vm_not_found", None

        current_state = str(vm_row["state"])
        if action_name == "start" and current_state == "running":
            return "invalid_state", {"error": "vm is already running"}
        if action_name == "stop" and current_state == "stopped":
            return "invalid_state", {"error": "vm is already stopped"}
        if action_name == "reboot" and current_state not in {"running", "unknown"}:
            return "invalid_state", {"error": "vm must be running to reboot"}
        if current_state in {"creating", "deleting"}:
            return "invalid_state", {"error": f"vm is currently {current_state}"}

        operation_id = str(uuid.uuid4())
        now = utc_now()
        if action_name == "reboot":
            next_state = "rebooting"
        elif action_name == "delete":
            next_state = "deleting"
        else:
            next_state = "unknown"

        conn.execute(
            """
            UPDATE node_vms
            SET state = ?, last_error = NULL, updated_at = ?
            WHERE id = ?;
            """,
            (next_state, now, clean_vm_id),
        )

        request_payload = {
            "action": action_name,
            "domain_name": vm_row["domain_name"],
            "vm_id": clean_vm_id,
        }
        conn.execute(
            """
            INSERT INTO vm_operations (
                id, node_id, vm_id, operation_type, status,
                request_json, result_json, error,
                created_at, started_at, ended_at
            ) VALUES (?, ?, ?, ?, 'queued', ?, NULL, NULL, ?, NULL, NULL);
            """,
            (
                operation_id,
                clean_node_id,
                clean_vm_id,
                action_name,
                json.dumps(request_payload),
                now,
            ),
        )

        _insert_node_log(
            conn,
            node_id=clean_node_id,
            level="info",
            message=f"VM {action_name} queued vm_id={clean_vm_id}",
            meta={"operation_id": operation_id, "vm_id": clean_vm_id},
        )

        refreshed_vm = conn.execute(
            """
            SELECT nv.*, vi.name AS image_name
            FROM node_vms nv
            INNER JOIN vm_images vi ON vi.id = nv.image_id
            WHERE nv.id = ? LIMIT 1;
            """,
            (clean_vm_id,),
        ).fetchone()
        op_row = conn.execute("SELECT * FROM vm_operations WHERE id = ? LIMIT 1;", (operation_id,)).fetchone()

        command = {
            "type": "command",
            "command_type": f"vm_{action_name}",
            "command_id": operation_id,
            "operation_id": operation_id,
            "vm_id": clean_vm_id,
            "created_at": now,
            "domain_name": vm_row["domain_name"],
            "vm_spec": {
                "vm_id": clean_vm_id,
                "domain_name": vm_row["domain_name"],
            },
        }

        return "ok", {
            "vm": _to_public_vm(refreshed_vm),
            "operation": _to_public_vm_operation(op_row),
            "command": command,
        }


def _derive_vm_state_from_power(power_state: Any, fallback: str = "unknown") -> str:
    if not isinstance(power_state, str):
        return fallback
    normalized = power_state.strip().lower()
    if "running" in normalized:
        return "running"
    if "shut" in normalized or "stopped" in normalized or "off" in normalized:
        return "stopped"
    return fallback


def fail_unfinished_vm_operations(db_path: Path, reason: str) -> int:
    now = utc_now()
    updated_count = 0
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM vm_operations
            WHERE status IN ('queued', 'running');
            """
        ).fetchall()

        for row in rows:
            conn.execute(
                """
                UPDATE vm_operations
                SET status = 'failed', error = ?, ended_at = ?, started_at = COALESCE(started_at, ?)
                WHERE id = ?;
                """,
                (reason, now, now, row["id"]),
            )
            if row["vm_id"]:
                conn.execute(
                    """
                    UPDATE node_vms
                    SET state = 'error', last_error = ?, updated_at = ?
                    WHERE id = ?;
                    """,
                    (reason, now, row["vm_id"]),
                )
            _insert_node_log(
                conn,
                node_id=row["node_id"],
                level="error",
                message=f"VM operation {row['operation_type']} failed after restart",
                meta={"operation_id": row["id"], "reason": reason, "vm_id": row["vm_id"]},
                created_at=now,
            )
            updated_count += 1
    return updated_count


def fail_stale_vm_operations(db_path: Path, stale_after_seconds: int = 600) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=max(60, stale_after_seconds))
    now = utc_now()
    updated_count = 0
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM vm_operations
            WHERE status = 'queued';
            """
        ).fetchall()

        for row in rows:
            created_raw = row["created_at"]
            try:
                created_at = datetime.fromisoformat(created_raw)
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
            except Exception:
                continue

            if created_at >= cutoff:
                continue

            reason = "Timed out waiting for agent connection"
            conn.execute(
                """
                UPDATE vm_operations
                SET status = 'failed', error = ?, ended_at = ?, started_at = COALESCE(started_at, ?)
                WHERE id = ?;
                """,
                (reason, now, now, row["id"]),
            )
            if row["vm_id"]:
                conn.execute(
                    """
                    UPDATE node_vms
                    SET state = 'error', last_error = ?, updated_at = ?
                    WHERE id = ?;
                    """,
                    (reason, now, row["vm_id"]),
                )
            _insert_node_log(
                conn,
                node_id=row["node_id"],
                level="error",
                message=f"VM operation {row['operation_type']} timed out",
                meta={"operation_id": row["id"], "vm_id": row["vm_id"]},
                created_at=now,
            )
            updated_count += 1
    return updated_count


def apply_vm_command_result(
    db_path: Path,
    node_id: str,
    operation_id: str,
    command_type: str,
    status: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any] | None]:
    clean_node_id = (node_id or "").strip()
    clean_operation_id = (operation_id or "").strip()
    clean_status = (status or "").strip().lower()
    clean_message = (message or "").strip() or "No details provided"
    details_payload = details if isinstance(details, dict) else None
    now = utc_now()

    with _connect(db_path) as conn:
        op_row = conn.execute(
            "SELECT * FROM vm_operations WHERE id = ? AND node_id = ? LIMIT 1;",
            (clean_operation_id, clean_node_id),
        ).fetchone()
        if op_row is None:
            return "not_found", None

        if clean_status == "running":
            conn.execute(
                """
                UPDATE vm_operations
                SET status = 'running', started_at = COALESCE(started_at, ?)
                WHERE id = ?;
                """,
                (now, clean_operation_id),
            )
            _insert_node_log(
                conn,
                node_id=clean_node_id,
                level="info",
                message=f"VM operation running: {op_row['operation_type']} ({clean_message})",
                meta={"operation_id": clean_operation_id, "vm_id": op_row["vm_id"], "details": details_payload},
                created_at=now,
            )
            updated_op = conn.execute("SELECT * FROM vm_operations WHERE id = ? LIMIT 1;", (clean_operation_id,)).fetchone()
            return "ok", _to_public_vm_operation(updated_op)

        final_status = "succeeded" if clean_status == "succeeded" else "failed"
        op_type = str(op_row["operation_type"])
        vm_id = op_row["vm_id"]

        conn.execute(
            """
            UPDATE vm_operations
            SET
                status = ?,
                started_at = COALESCE(started_at, ?),
                ended_at = ?,
                error = ?,
                result_json = ?
            WHERE id = ?;
            """,
            (
                final_status,
                now,
                now,
                clean_message if final_status == "failed" else None,
                json.dumps(details_payload) if details_payload else None,
                clean_operation_id,
            ),
        )

        vm_payload: dict[str, Any] | None = None
        if vm_id:
            vm_row = conn.execute("SELECT * FROM node_vms WHERE id = ? LIMIT 1;", (vm_id,)).fetchone()
            if vm_row is not None:
                if final_status == "failed":
                    conn.execute(
                        """
                        UPDATE node_vms
                        SET state = 'error', last_error = ?, updated_at = ?
                        WHERE id = ?;
                        """,
                        (clean_message, now, vm_id),
                    )
                else:
                    if op_type == "delete":
                        conn.execute("DELETE FROM node_vms WHERE id = ?;", (vm_id,))
                    else:
                        if op_type in {"create", "start", "reboot"}:
                            next_state = _derive_vm_state_from_power(
                                details_payload.get("power_state") if details_payload else None,
                                fallback="running" if op_type != "create" else "unknown",
                            )
                        elif op_type == "stop":
                            next_state = "stopped"
                        else:
                            next_state = "unknown"

                        domain_uuid = (
                            details_payload.get("domain_uuid")
                            if details_payload and isinstance(details_payload.get("domain_uuid"), str)
                            else vm_row["domain_uuid"]
                        )
                        ip_address = (
                            details_payload.get("ip_address")
                            if details_payload and isinstance(details_payload.get("ip_address"), str)
                            else vm_row["ip_address"]
                        )

                        conn.execute(
                            """
                            UPDATE node_vms
                            SET
                                state = ?,
                                domain_uuid = ?,
                                ip_address = ?,
                                last_error = NULL,
                                updated_at = ?
                            WHERE id = ?;
                            """,
                            (next_state, domain_uuid, ip_address, now, vm_id),
                        )

                refreshed_vm = conn.execute(
                    """
                    SELECT nv.*, vi.name AS image_name
                    FROM node_vms nv
                    INNER JOIN vm_images vi ON vi.id = nv.image_id
                    WHERE nv.id = ?
                    LIMIT 1;
                    """,
                    (vm_id,),
                ).fetchone()
                if refreshed_vm is not None:
                    payload = dict(refreshed_vm)
                    latest_operation = _fetch_latest_vm_operation(conn, vm_id)
                    payload["last_operation_json"] = json.dumps(latest_operation) if latest_operation else None
                    vm_payload = _to_public_vm(payload)

        level = "info" if final_status == "succeeded" else "error"
        _insert_node_log(
            conn,
            node_id=clean_node_id,
            level=level,
            message=f"VM operation {op_type} {final_status}: {clean_message}",
            meta={
                "operation_id": clean_operation_id,
                "vm_id": vm_id,
                "command_type": command_type,
                "details": details_payload,
            },
            created_at=now,
        )

        updated_op = conn.execute("SELECT * FROM vm_operations WHERE id = ? LIMIT 1;", (clean_operation_id,)).fetchone()
        return "ok", {"operation": _to_public_vm_operation(updated_op), "vm": vm_payload}
