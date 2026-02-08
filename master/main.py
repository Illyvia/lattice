from pathlib import Path
import json
import os
import queue
import socket
import sys
import threading
import uuid
from datetime import datetime, timezone
from time import sleep
from typing import Any
from urllib.parse import parse_qs

from flask import Flask, jsonify, request, send_from_directory
from flask_sock import Sock
from werkzeug.serving import get_interface_ip

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from agent.system import log_system_info
from log_setup import setup_logger
from master.db import (
    apply_terminal_command_result,
    apply_vm_command_result,
    append_node_log,
    create_node,
    create_vm_request,
    delete_node,
    fail_stale_vm_operations,
    fail_unfinished_vm_operations,
    get_node_by_id,
    get_node_vm,
    init_db,
    is_valid_node_token,
    list_node_vms,
    list_vm_images,
    list_vm_operations,
    list_node_logs,
    list_nodes,
    list_terminal_commands,
    pair_node,
    queue_terminal_command,
    queue_vm_action,
    rename_node,
    record_heartbeat,
)

DB_PATH = ROOT_DIR / "master" / "master.db"
UI_DIST = ROOT_DIR / "master" / "ui" / "dist"

if os.name == "nt":
    log_path = ROOT_DIR / "logs" / "master.log"
else:
    log_path = Path("/var/log/lattice/master.log")

log = setup_logger("master", str(log_path))
log.rawlog("""                                                                                                                              
                                        @@*                 %@@      @@-                                                      
                          @@@@@@@@@@    @@*                 @@@      @@=   %@@=                                               
                          @@@@@@@@@@@   @@*     .::        :@@*::  :=@@:::           :::         :::                          
                      %%*-@@%#@@@@@@@   @@*  +@@@@@@@@*@@ %@@@@@@* @@@@@@@ =@@   =@@@@@@@@@  =@@@@@@@@@=                      
                     @@@@@     *@@@@%   @@* %@@%     %@@@   @@@      @@=   =@@. @@@*     =  @@@*     *@@@                     
                     @@@@@     @@@@@    @@* @@+       #@@   @@@      @@=   =@@ .@@=        :@@%:::::::#@@:                    
                    :@@@@@@**%@=#@@=    @@* @@=       *@@   @@@      @@=   =@@.:@@:        :@@@@@@@@@@@@@:                    
                    .@@@@@@@@@@#        @@* @@@       @@@   @@@      @@=   =@@  @@@         @@@                               
                     @@@@@@@@@@*        @@*  @@@@@@@@@%@@   -@@@@@   @@@@@ =@@.  @@@@@@@@@@  @@@@@@@@@@                       
                                        @@*    @@@@@= =@@     :@@@     #@@ =@@     *@@@@@*     *@@@@@*                        
                                                                                                                              
                                                                                                                              """)
log.info("Lattice master started")
log_system_info(log)

init_db(DB_PATH)
failed_after_restart = fail_unfinished_vm_operations(
    DB_PATH,
    reason="Master restarted before operation dispatch",
)
if failed_after_restart > 0:
    log.info(f"Marked {failed_after_restart} stale VM operations as failed after restart")

app = Flask(__name__)
sock = Sock(app)

AGENT_COMMANDS_LOCK = threading.Lock()
PENDING_AGENT_COMMANDS: dict[str, list[dict[str, Any]]] = {}
ACTIVE_AGENT_CONNECTIONS: dict[str, str] = {}
AGENT_WS_OUTBOUND_MESSAGES: dict[str, list[dict[str, Any]]] = {}
TERMINAL_SESSIONS_LOCK = threading.Lock()
TERMINAL_SESSIONS: dict[str, dict[str, Any]] = {}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _enqueue_agent_command(node_id: str, command: dict[str, Any]) -> None:
    with AGENT_COMMANDS_LOCK:
        queue = PENDING_AGENT_COMMANDS.setdefault(node_id, [])
        queue.append(command)


def _dequeue_agent_command(node_id: str) -> dict[str, Any] | None:
    with AGENT_COMMANDS_LOCK:
        queue = PENDING_AGENT_COMMANDS.get(node_id)
        if not queue:
            return None
        command = queue.pop(0)
        if not queue:
            PENDING_AGENT_COMMANDS.pop(node_id, None)
        return command


def _activate_agent_connection(node_id: str, connection_id: str) -> str | None:
    with AGENT_COMMANDS_LOCK:
        previous = ACTIVE_AGENT_CONNECTIONS.get(node_id)
        ACTIVE_AGENT_CONNECTIONS[node_id] = connection_id
        return previous


def _is_current_agent_connection(node_id: str, connection_id: str) -> bool:
    with AGENT_COMMANDS_LOCK:
        return ACTIVE_AGENT_CONNECTIONS.get(node_id) == connection_id


def _deactivate_agent_connection(node_id: str, connection_id: str) -> bool:
    with AGENT_COMMANDS_LOCK:
        current = ACTIVE_AGENT_CONNECTIONS.get(node_id)
        if current != connection_id:
            return False
        ACTIVE_AGENT_CONNECTIONS.pop(node_id, None)
        return True


def _is_agent_connected(node_id: str) -> bool:
    with AGENT_COMMANDS_LOCK:
        return node_id in ACTIVE_AGENT_CONNECTIONS


def _enqueue_agent_ws_message(node_id: str, payload: dict[str, Any]) -> None:
    with AGENT_COMMANDS_LOCK:
        messages = AGENT_WS_OUTBOUND_MESSAGES.setdefault(node_id, [])
        messages.append(payload)
        if len(messages) > 2000:
            del messages[:-1000]


def _drain_agent_ws_messages(node_id: str, max_items: int = 100) -> list[dict[str, Any]]:
    with AGENT_COMMANDS_LOCK:
        messages = AGENT_WS_OUTBOUND_MESSAGES.get(node_id)
        if not messages:
            return []
        take = max(1, min(max_items, len(messages)))
        chunk = messages[:take]
        del messages[:take]
        if not messages:
            AGENT_WS_OUTBOUND_MESSAGES.pop(node_id, None)
        return chunk


def _clear_agent_ws_messages(node_id: str) -> None:
    with AGENT_COMMANDS_LOCK:
        AGENT_WS_OUTBOUND_MESSAGES.pop(node_id, None)


def _register_terminal_session(
    node_id: str,
    vm_id: str | None = None,
    terminal_kind: str = "node_shell",
) -> tuple[str, queue.Queue[dict[str, Any]]]:
    session_id = str(uuid.uuid4())
    incoming_queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=500)
    normalized_vm_id = vm_id.strip() if isinstance(vm_id, str) and vm_id.strip() else None
    with TERMINAL_SESSIONS_LOCK:
        TERMINAL_SESSIONS[session_id] = {
            "node_id": node_id,
            "vm_id": normalized_vm_id,
            "kind": terminal_kind,
            "queue": incoming_queue,
        }
    return session_id, incoming_queue


def _unregister_terminal_session(session_id: str) -> None:
    with TERMINAL_SESSIONS_LOCK:
        TERMINAL_SESSIONS.pop(session_id, None)


def _enqueue_terminal_session_event(session_id: str, payload: dict[str, Any]) -> bool:
    with TERMINAL_SESSIONS_LOCK:
        session = TERMINAL_SESSIONS.get(session_id)
    if not session:
        return False
    incoming_queue = session.get("queue")
    if not isinstance(incoming_queue, queue.Queue):
        return False
    try:
        incoming_queue.put_nowait(payload)
        return True
    except queue.Full:
        try:
            _ = incoming_queue.get_nowait()
        except queue.Empty:
            pass
        try:
            incoming_queue.put_nowait(payload)
            return True
        except queue.Full:
            return False


def _close_terminal_sessions_for_node(node_id: str, reason: str) -> None:
    with TERMINAL_SESSIONS_LOCK:
        session_ids = [session_id for session_id, data in TERMINAL_SESSIONS.items() if data.get("node_id") == node_id]
    for session_id in session_ids:
        _enqueue_terminal_session_event(
            session_id,
            {
                "type": "terminal_error",
                "session_id": session_id,
                "error": reason,
            },
        )


def _apply_cors_headers(response):
    origin = request.headers.get("Origin")
    # Allow local dev UIs on any host/port without needing a restart for host changes.
    response.headers["Access-Control-Allow-Origin"] = origin or "*"
    response.headers["Vary"] = "Origin"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, DELETE, PATCH, OPTIONS"
    return response


@app.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        return _apply_cors_headers(app.make_default_options_response())
    return None


@app.after_request
def add_cors_headers(response):
    return _apply_cors_headers(response)


def _json_error(status_code: int, message: str) -> tuple[dict[str, Any], int]:
    return {"error": message}, status_code


def _coerce_vm_limit(raw_value: str | None, default: int = 50) -> int:
    try:
        if raw_value is None:
            raise ValueError
        value = int(raw_value)
    except (TypeError, ValueError):
        value = default
    return max(1, min(value, 200))


def _resolve_logs_request(node_id: str) -> tuple[dict[str, Any], int]:
    limit_raw = request.args.get("limit", "200")
    since_id_raw = request.args.get("since_id")

    try:
        limit = int(limit_raw)
    except ValueError:
        return _json_error(400, "limit must be an integer")

    since_id: int | None = None
    if since_id_raw is not None:
        try:
            since_id = int(since_id_raw)
        except ValueError:
            return _json_error(400, "since_id must be an integer")

    status, items = list_node_logs(DB_PATH, node_id=node_id, limit=limit, since_id=since_id)
    if status == "not_found":
        # Treat missing nodes as an empty log stream to avoid noisy UI polling failures.
        return {"items": [], "next_since_id": since_id}, 200

    next_since_id = items[-1]["id"] if items else since_id
    return {"items": items, "next_since_id": next_since_id}, 200


def _coerce_logs_limit(raw_limit: str | None, default: int = 200) -> int:
    try:
        if raw_limit is None:
            raise ValueError
        value = int(raw_limit)
    except (TypeError, ValueError):
        value = default
    return max(1, min(value, 500))


def _has_recent_heartbeat(last_heartbeat_at: Any, max_age_seconds: int = 45) -> bool:
    if not isinstance(last_heartbeat_at, str) or not last_heartbeat_at.strip():
        return False
    raw = last_heartbeat_at.strip()
    # Support both "+00:00" and trailing "Z" UTC formats.
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    age_seconds = (datetime.now(timezone.utc) - dt).total_seconds()
    return -5 <= age_seconds <= max_age_seconds


def _extract_bearer_token() -> str | None:
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None
    token = auth_header.removeprefix("Bearer ").strip()
    return token or None


def _validate_agent_node_auth(node_id: str) -> tuple[str | None, tuple[dict[str, Any], int] | None]:
    token = _extract_bearer_token()
    if not token:
        return None, _json_error(401, "missing bearer token")
    if not is_valid_node_token(DB_PATH, node_id, token):
        return None, _json_error(403, "unauthorized")

    node = get_node_by_id(DB_PATH, node_id)
    if not node:
        return None, _json_error(404, "node not found")
    expected_hostname = node.get("agent_hostname")
    supplied_hostname = request.headers.get("X-Agent-Hostname", "").strip()
    if (
        isinstance(expected_hostname, str)
        and expected_hostname.strip()
        and supplied_hostname
        and expected_hostname.strip().lower() != supplied_hostname.lower()
    ):
        return None, _json_error(403, "hostname mismatch")
    return token, None


def _process_agent_command_result(
    *,
    node_id: str,
    payload: dict[str, Any],
) -> tuple[str, dict[str, Any] | None]:
    command_id = payload.get("command_id")
    operation_id = payload.get("operation_id")
    command_type = payload.get("command_type")
    status = payload.get("status")
    message = payload.get("message")
    details = payload.get("details")

    if not isinstance(command_id, str) or not command_id.strip():
        return "invalid_command_id", None
    if not isinstance(command_type, str) or not command_type.strip():
        command_type = "unknown"
    if not isinstance(status, str) or not status.strip():
        status = "unknown"
    if not isinstance(message, str) or not message.strip():
        message = "No details provided"
    if not isinstance(details, dict):
        details = None

    vm_operation_id = operation_id if isinstance(operation_id, str) and operation_id.strip() else command_id
    if command_type.startswith("vm_"):
        apply_status, result = apply_vm_command_result(
            DB_PATH,
            node_id=node_id,
            operation_id=vm_operation_id,
            command_type=command_type,
            status=status,
            message=message,
            details=details,
        )
        if apply_status == "not_found":
            return "operation_not_found", None
        log.info(
            f"Agent vm command result node_id={node_id} operation_id={vm_operation_id} "
            f"type={command_type} status={status} message={message}"
        )
        return "ok", result
    if command_type == "terminal_exec":
        apply_status, result = apply_terminal_command_result(
            DB_PATH,
            node_id=node_id,
            operation_id=vm_operation_id,
            status=status,
            message=message,
            details=details,
        )
        if apply_status == "not_found":
            return "operation_not_found", None
        log.info(
            f"Agent terminal command result node_id={node_id} operation_id={vm_operation_id} "
            f"status={status} message={message}"
        )
        return "ok", result

    level = "info"
    if status in {"failed", "error"}:
        level = "error"
    elif status in {"busy"}:
        level = "warning"

    detail_suffix = ""
    if level == "error" and isinstance(details, dict):
        detail_candidate: str | None = None
        for key in ("stderr", "error", "stdout"):
            value = details.get(key)
            if isinstance(value, str) and value.strip():
                detail_candidate = value.strip().splitlines()[0]
                break
        changed_files = details.get("changed_files")
        if not detail_candidate and isinstance(changed_files, list) and changed_files:
            first_file = changed_files[0]
            if isinstance(first_file, str) and first_file.strip():
                detail_candidate = f"local change: {first_file.strip()}"
        if detail_candidate:
            detail_suffix = f" ({detail_candidate[:180]})"

    append_node_log(
        DB_PATH,
        node_id=node_id,
        level=level,
        message=f"Agent command {command_type} -> {status}: {message}{detail_suffix}",
        meta={
            "command_id": command_id,
            "command_type": command_type,
            "status": status,
            "details": details,
        },
    )
    log.info(
        f"Agent command result node_id={node_id} command_id={command_id} "
        f"type={command_type} status={status} message={message}"
    )
    return "ok", {
        "command_id": command_id,
        "command_type": command_type,
        "status": status,
    }


@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.get("/api/nodes")
def get_nodes():
    fail_stale_vm_operations(DB_PATH)
    return jsonify(list_nodes(DB_PATH)), 200


@app.get("/api/vm-images")
def get_vm_images():
    return jsonify(list_vm_images(DB_PATH)), 200


@app.get("/api/nodes/<node_id>/vms")
def get_node_vms(node_id: str):
    fail_stale_vm_operations(DB_PATH)
    status, vms = list_node_vms(DB_PATH, node_id=node_id)
    if status == "not_found":
        return _json_error(404, "node not found")
    return jsonify(vms), 200


@app.get("/api/nodes/<node_id>/vms/<vm_id>")
def get_node_vm_route(node_id: str, vm_id: str):
    status, vm = get_node_vm(DB_PATH, node_id=node_id, vm_id=vm_id)
    if status == "not_found":
        return _json_error(404, "node not found")
    if status == "vm_not_found":
        return _json_error(404, "vm not found")
    return jsonify(vm), 200


@app.get("/api/nodes/<node_id>/vms/<vm_id>/operations")
def get_node_vm_operations(node_id: str, vm_id: str):
    limit = _coerce_vm_limit(request.args.get("limit"))
    status, operations = list_vm_operations(
        DB_PATH,
        node_id=node_id,
        vm_id=vm_id,
        limit=limit,
    )
    if status == "not_found":
        return _json_error(404, "node not found")
    if status == "vm_not_found":
        return _json_error(404, "vm not found")
    return jsonify(operations), 200


@app.post("/api/nodes/<node_id>/vms")
def post_node_vm(node_id: str):
    payload = request.get_json(silent=True) or {}
    status, result = create_vm_request(DB_PATH, node_id=node_id, payload=payload)
    if status == "not_found":
        return _json_error(404, "node not found")
    if status == "node_not_paired":
        return _json_error(409, "node must be paired before creating vms")
    if status == "capability_not_ready":
        return _json_error(409, "node vm capability is not ready")
    if status == "image_not_found":
        return _json_error(404, "vm image not found")
    if status == "invalid_payload":
        return _json_error(400, str(result.get("error")) if isinstance(result, dict) else "invalid payload")
    if status == "conflict":
        return _json_error(409, str(result.get("error")) if isinstance(result, dict) else "vm conflict")

    command = result["command"]
    _enqueue_agent_command(node_id, command)
    connected = _is_agent_connected(node_id)
    operation_id = command.get("operation_id")
    vm_id = command.get("vm_id")
    log.info(
        f"Queued vm_create node_id={node_id} operation_id={operation_id} vm_id={vm_id} connected={connected}"
    )
    return jsonify(
        {
            "ok": True,
            "queued": True,
            "agent_connected": connected,
            "vm": result["vm"],
            "operation": result["operation"],
        }
    ), 202


def _queue_vm_action_route(node_id: str, vm_id: str, action: str):
    status, result = queue_vm_action(DB_PATH, node_id=node_id, vm_id=vm_id, action=action)
    if status == "not_found":
        return _json_error(404, "node not found")
    if status == "vm_not_found":
        return _json_error(404, "vm not found")
    if status == "node_not_paired":
        return _json_error(409, "node must be paired before vm actions")
    if status == "capability_not_ready":
        return _json_error(409, "node vm capability is not ready")
    if status == "invalid_state":
        return _json_error(409, str(result.get("error")) if isinstance(result, dict) else "invalid vm state")
    if status == "invalid_action":
        return _json_error(400, "invalid action")

    command = result["command"]
    _enqueue_agent_command(node_id, command)
    connected = _is_agent_connected(node_id)
    operation_id = command.get("operation_id")
    log.info(
        f"Queued vm_{action} node_id={node_id} vm_id={vm_id} operation_id={operation_id} connected={connected}"
    )
    return jsonify(
        {
            "ok": True,
            "queued": True,
            "agent_connected": connected,
            "vm": result["vm"],
            "operation": result["operation"],
        }
    ), 202


@app.post("/api/nodes/<node_id>/vms/<vm_id>/actions/start")
def post_node_vm_start(node_id: str, vm_id: str):
    return _queue_vm_action_route(node_id=node_id, vm_id=vm_id, action="start")


@app.post("/api/nodes/<node_id>/vms/<vm_id>/actions/stop")
def post_node_vm_stop(node_id: str, vm_id: str):
    return _queue_vm_action_route(node_id=node_id, vm_id=vm_id, action="stop")


@app.post("/api/nodes/<node_id>/vms/<vm_id>/actions/reboot")
def post_node_vm_reboot(node_id: str, vm_id: str):
    return _queue_vm_action_route(node_id=node_id, vm_id=vm_id, action="reboot")


@app.post("/api/nodes/<node_id>/vms/<vm_id>/actions/delete")
def post_node_vm_delete(node_id: str, vm_id: str):
    return _queue_vm_action_route(node_id=node_id, vm_id=vm_id, action="delete")


@app.get("/api/nodes/<node_id>/logs")
def get_node_logs(node_id: str):
    body, status = _resolve_logs_request(node_id)
    return jsonify(body), status


@app.post("/api/nodes")
def post_node():
    payload = request.get_json(silent=True) or {}
    created = create_node(DB_PATH, payload.get("name"))
    log.info(f"Node created id={created['id']} name={created['name']} state={created['state']}")
    return jsonify(created), 201


@app.patch("/api/nodes/<node_id>")
def rename_node_route(node_id: str):
    payload = request.get_json(silent=True) or {}
    name = payload.get("name")
    if not isinstance(name, str):
        return _json_error(400, "name is required")

    status, renamed_node = rename_node(DB_PATH, node_id, name)
    if status == "invalid_name":
        return _json_error(400, "name is required")
    if status == "not_found":
        return _json_error(404, "node not found")

    log.info(f"Node renamed id={renamed_node['id']} name={renamed_node['name']}")
    return jsonify(renamed_node), 200


@app.post("/api/nodes/<node_id>/rename")
def rename_node_route_post(node_id: str):
    payload = request.get_json(silent=True) or {}
    name = payload.get("name")
    if not isinstance(name, str):
        return _json_error(400, "name is required")

    status, renamed_node = rename_node(DB_PATH, node_id, name)
    if status == "invalid_name":
        return _json_error(400, "name is required")
    if status == "not_found":
        return _json_error(404, "node not found")

    log.info(f"Node renamed id={renamed_node['id']} name={renamed_node['name']}")
    return jsonify(renamed_node), 200


@app.post("/api/nodes/delete")
def delete_node_route_body():
    payload = request.get_json(silent=True) or {}
    node_id = payload.get("node_id")
    if not isinstance(node_id, str) or not node_id.strip():
        return _json_error(400, "node_id is required")

    deleted_node = delete_node(DB_PATH, node_id)
    if not deleted_node:
        log.info(f"Node delete requested but not found id={node_id}")
        return _json_error(404, "node not found")
    log.info(f"Node deleted id={deleted_node['id']} name={deleted_node['name']}")
    return jsonify({"ok": True}), 200


@app.delete("/api/nodes/<node_id>")
def delete_node_route(node_id: str):
    deleted_node = delete_node(DB_PATH, node_id)
    if not deleted_node:
        log.info(f"Node delete requested but not found id={node_id}")
        return _json_error(404, "node not found")
    log.info(f"Node deleted id={deleted_node['id']} name={deleted_node['name']}")
    return jsonify({"ok": True}), 200


@app.post("/api/nodes/<node_id>/delete")
def delete_node_route_post(node_id: str):
    deleted_node = delete_node(DB_PATH, node_id)
    if not deleted_node:
        log.info(f"Node delete requested but not found id={node_id}")
        return _json_error(404, "node not found")
    log.info(f"Node deleted id={deleted_node['id']} name={deleted_node['name']}")
    return jsonify({"ok": True}), 200


@app.post("/api/pair")
def post_pair():
    payload = request.get_json(silent=True) or {}
    pair_code = payload.get("pair_code")
    agent_info = payload.get("agent")

    if not isinstance(pair_code, str):
        return _json_error(400, "pair_code is required")
    if agent_info is not None and not isinstance(agent_info, dict):
        return _json_error(400, "agent must be an object")

    status, result = pair_node(DB_PATH, pair_code, agent_info)
    if status == "invalid_code":
        return _json_error(400, "invalid pair code format")
    if status == "not_found":
        return _json_error(404, "pair code not found")
    if status == "already_paired":
        return _json_error(409, "pair code already paired")

    log.info(f"Node paired node_id={result['node_id']}")
    return jsonify(result), 200


@app.post("/api/heartbeat")
def post_heartbeat():
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return _json_error(401, "missing bearer token")

    pair_token = auth_header.removeprefix("Bearer ").strip()
    payload = request.get_json(silent=True) or {}
    node_id = payload.get("node_id")
    if not isinstance(node_id, str):
        return _json_error(400, "node_id is required")

    status, _node = record_heartbeat(DB_PATH, pair_token, node_id, payload)
    if status in {"missing_token", "invalid_token"}:
        return _json_error(401, "invalid token")
    if status in {"node_mismatch", "hostname_mismatch"}:
        return _json_error(403, "token does not match node")

    return jsonify({"ok": True}), 200


@app.post("/api/nodes/<node_id>/actions/update-agent")
def post_update_agent(node_id: str):
    payload = request.get_json(silent=True) or {}
    force = payload.get("force", False)
    branch = payload.get("branch")

    if not isinstance(force, bool):
        return _json_error(400, "force must be a boolean")
    if branch is not None and not isinstance(branch, str):
        return _json_error(400, "branch must be a string")

    node = next((item for item in list_nodes(DB_PATH) if item["id"] == node_id), None)
    if not node:
        return _json_error(404, "node not found")
    if node.get("state") != "paired":
        return _json_error(409, "node must be paired before updating")

    command_id = str(uuid.uuid4())
    command: dict[str, Any] = {
        "type": "command",
        "command_type": "update_agent",
        "command_id": command_id,
        "created_at": _utc_now(),
        "force": force,
    }
    if isinstance(branch, str) and branch.strip():
        command["branch"] = branch.strip()

    _enqueue_agent_command(node_id, command)
    append_node_log(
        DB_PATH,
        node_id=node_id,
        level="info",
        message="Agent update requested from UI",
        meta={"command_id": command_id, "force": force, "branch": command.get("branch")},
    )
    ws_connected = _is_agent_connected(node_id)
    recently_heartbeat = _has_recent_heartbeat(node.get("last_heartbeat_at"))
    connected = ws_connected or recently_heartbeat
    log.info(
        f"Queued update command node_id={node_id} command_id={command_id} "
        f"connected={connected} ws_connected={ws_connected} recent_heartbeat={recently_heartbeat}"
    )
    return jsonify(
        {
            "ok": True,
            "command_id": command_id,
            "queued": True,
            "agent_connected": connected,
            "agent_ws_connected": ws_connected,
            "recent_heartbeat": recently_heartbeat,
        }
    ), 202


@app.get("/api/nodes/<node_id>/terminal/commands")
def get_node_terminal_commands(node_id: str):
    limit = _coerce_vm_limit(request.args.get("limit"), default=100)
    status, commands = list_terminal_commands(DB_PATH, node_id=node_id, limit=limit)
    if status == "not_found":
        return _json_error(404, "node not found")
    return jsonify(commands), 200


@app.post("/api/nodes/<node_id>/terminal/exec")
def post_node_terminal_exec(node_id: str):
    payload = request.get_json(silent=True) or {}
    command_text = payload.get("command")
    if not isinstance(command_text, str):
        return _json_error(400, "command is required")

    status, result = queue_terminal_command(DB_PATH, node_id=node_id, command_text=command_text)
    if status == "not_found":
        return _json_error(404, "node not found")
    if status == "node_not_paired":
        return _json_error(409, "node must be paired before running terminal commands")
    if status == "invalid_payload":
        return _json_error(400, str(result.get("error")) if isinstance(result, dict) else "invalid payload")

    command = result["command"]
    _enqueue_agent_command(node_id, command)
    connected = _is_agent_connected(node_id)
    command_id = command.get("command_id")
    log.info(f"Queued terminal command node_id={node_id} command_id={command_id} connected={connected}")
    return jsonify({"ok": True, "queued": True, "agent_connected": connected, "operation": result["operation"]}), 202


@app.post("/api/nodes/<node_id>/commands/next")
def post_agent_next_command(node_id: str):
    clean_node_id = (node_id or "").strip()
    if not clean_node_id:
        return _json_error(404, "node not found")

    _, auth_error = _validate_agent_node_auth(clean_node_id)
    if auth_error:
        return auth_error

    command = _dequeue_agent_command(clean_node_id)
    if not command:
        return "", 204

    append_node_log(
        DB_PATH,
        node_id=clean_node_id,
        level="info",
        message=f"Dispatched agent command {command.get('command_type', 'unknown')} via polling",
        meta={"command_id": command.get("command_id"), "command": command},
    )
    return jsonify({"command": command}), 200


@app.post("/api/nodes/<node_id>/commands/result")
def post_agent_command_result(node_id: str):
    clean_node_id = (node_id or "").strip()
    if not clean_node_id:
        return _json_error(404, "node not found")

    _, auth_error = _validate_agent_node_auth(clean_node_id)
    if auth_error:
        return auth_error

    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        return _json_error(400, "invalid payload")

    status, result = _process_agent_command_result(node_id=clean_node_id, payload=payload)
    if status == "invalid_command_id":
        return _json_error(400, "command_id is required")
    if status == "operation_not_found":
        return _json_error(404, "operation_not_found")
    if status != "ok":
        return _json_error(500, "failed to process command result")
    return jsonify({"ok": True, "result": result}), 200


def _stream_node_logs_ws(ws, node_id: str, limit: int):
    clean_node_id = (node_id or "").strip()
    if not clean_node_id:
        ws.send(json.dumps({"type": "error", "error": "node_not_found"}))
        return

    since_id: int | None = None
    try:
        status, items = list_node_logs(DB_PATH, node_id=clean_node_id, limit=limit, since_id=None)
        if status == "not_found":
            ws.send(json.dumps({"type": "error", "error": "node_not_found"}))
            return

        since_id = items[-1]["id"] if items else None
        ws.send(json.dumps({"type": "snapshot", "items": items, "next_since_id": since_id}))

        while True:
            sleep(1)
            status, delta_items = list_node_logs(
                DB_PATH,
                node_id=clean_node_id,
                limit=limit,
                since_id=since_id,
            )
            if status == "not_found":
                ws.send(json.dumps({"type": "error", "error": "node_not_found"}))
                break
            if not delta_items:
                continue

            since_id = delta_items[-1]["id"]
            ws.send(
                json.dumps(
                    {
                        "type": "append",
                        "items": delta_items,
                        "next_since_id": since_id,
                    }
                )
            )
    except Exception as exc:
        log.info(f"Node log websocket closed node_id={clean_node_id} details={exc}")


@sock.route("/ws/node-logs")
def ws_node_logs(ws):
    query_string = ""
    environ = getattr(ws, "environ", None)
    if isinstance(environ, dict):
        query_string = str(environ.get("QUERY_STRING", ""))
    query = parse_qs(query_string)
    node_id = (query.get("node_id") or [""])[0]
    limit = _coerce_logs_limit((query.get("limit") or [None])[0])
    _stream_node_logs_ws(ws, node_id=node_id, limit=limit)


@sock.route("/ws/nodes/<node_id>/logs")
def ws_node_logs_compat(ws, node_id: str):
    query_string = ""
    environ = getattr(ws, "environ", None)
    if isinstance(environ, dict):
        query_string = str(environ.get("QUERY_STRING", ""))
    query = parse_qs(query_string)
    limit = _coerce_logs_limit((query.get("limit") or [None])[0])
    _stream_node_logs_ws(ws, node_id=node_id, limit=limit)


def _coerce_terminal_size(value: Any, default_value: int, min_value: int, max_value: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        number = default_value
    return max(min_value, min(max_value, number))


@sock.route("/ws/nodes/<node_id>/terminal")
def ws_node_terminal(ws, node_id: str):
    clean_node_id = (node_id or "").strip()
    if not clean_node_id:
        ws.send(json.dumps({"type": "terminal_error", "error": "node_not_found"}))
        return

    node = get_node_by_id(DB_PATH, clean_node_id)
    if not node:
        ws.send(json.dumps({"type": "terminal_error", "error": "node_not_found"}))
        return
    if node.get("state") != "paired":
        ws.send(json.dumps({"type": "terminal_error", "error": "node_not_paired"}))
        return

    query_string = ""
    environ = getattr(ws, "environ", None)
    if isinstance(environ, dict):
        query_string = str(environ.get("QUERY_STRING", ""))
    query = parse_qs(query_string)

    cols = _coerce_terminal_size((query.get("cols") or [80])[0], default_value=80, min_value=20, max_value=300)
    rows = _coerce_terminal_size((query.get("rows") or [24])[0], default_value=24, min_value=5, max_value=120)

    session_id, inbound_queue = _register_terminal_session(clean_node_id)
    _enqueue_agent_ws_message(
        clean_node_id,
        {
            "type": "terminal_open",
            "session_id": session_id,
            "cols": cols,
            "rows": rows,
        },
    )
    if not _is_agent_connected(clean_node_id):
        try:
            inbound_queue.put_nowait(
                {
                    "type": "terminal_data",
                    "session_id": session_id,
                    "data": "\r\n[waiting for agent websocket connection...]\r\n",
                }
            )
        except queue.Full:
            pass

    append_node_log(
        DB_PATH,
        node_id=clean_node_id,
        level="info",
        message="Terminal session opened",
        meta={"session_id": session_id},
    )

    ws.send(json.dumps({"type": "terminal_ready", "session_id": session_id}))

    try:
        while True:
            while True:
                try:
                    outbound = inbound_queue.get_nowait()
                except queue.Empty:
                    break
                ws.send(json.dumps(outbound))

            try:
                raw = ws.receive(timeout=0.2)
            except TimeoutError:
                continue
            if raw is None:
                # simple-websocket returns None on timeout when no frame is available.
                # Treat this as idle and keep the session open.
                continue

            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                ws.send(json.dumps({"type": "terminal_error", "error": "invalid_json"}))
                continue

            if not isinstance(payload, dict):
                ws.send(json.dumps({"type": "terminal_error", "error": "invalid_payload"}))
                continue

            message_type = payload.get("type")
            if message_type == "input":
                data = payload.get("data")
                if not isinstance(data, str):
                    continue
                _enqueue_agent_ws_message(
                    clean_node_id,
                    {
                        "type": "terminal_input",
                        "session_id": session_id,
                        "data": data,
                    },
                )
                continue

            if message_type == "resize":
                new_cols = _coerce_terminal_size(payload.get("cols"), default_value=cols, min_value=20, max_value=300)
                new_rows = _coerce_terminal_size(payload.get("rows"), default_value=rows, min_value=5, max_value=120)
                cols = new_cols
                rows = new_rows
                _enqueue_agent_ws_message(
                    clean_node_id,
                    {
                        "type": "terminal_resize",
                        "session_id": session_id,
                        "cols": new_cols,
                        "rows": new_rows,
                    },
                )
                continue

            if message_type == "ping":
                ws.send(json.dumps({"type": "pong"}))
                continue

            if message_type == "close":
                break
    except Exception as exc:
        log.info(f"Terminal websocket closed node_id={clean_node_id} session_id={session_id} details={exc}")
    finally:
        _unregister_terminal_session(session_id)
        _enqueue_agent_ws_message(
            clean_node_id,
            {
                "type": "terminal_close",
                "session_id": session_id,
            },
        )
        append_node_log(
            DB_PATH,
            node_id=clean_node_id,
            level="info",
            message="Terminal session closed",
            meta={"session_id": session_id},
        )


@sock.route("/ws/nodes/<node_id>/vms/<vm_id>/terminal")
def ws_vm_terminal(ws, node_id: str, vm_id: str):
    clean_node_id = (node_id or "").strip()
    clean_vm_id = (vm_id or "").strip()
    if not clean_node_id:
        ws.send(json.dumps({"type": "terminal_error", "error": "node_not_found"}))
        return
    if not clean_vm_id:
        ws.send(json.dumps({"type": "terminal_error", "error": "vm_not_found"}))
        return

    node = get_node_by_id(DB_PATH, clean_node_id)
    if not node:
        ws.send(json.dumps({"type": "terminal_error", "error": "node_not_found"}))
        return
    if node.get("state") != "paired":
        ws.send(json.dumps({"type": "terminal_error", "error": "node_not_paired"}))
        return

    vm_status, vm = get_node_vm(DB_PATH, node_id=clean_node_id, vm_id=clean_vm_id)
    if vm_status == "not_found":
        ws.send(json.dumps({"type": "terminal_error", "error": "node_not_found"}))
        return
    if vm_status == "vm_not_found" or not isinstance(vm, dict):
        ws.send(json.dumps({"type": "terminal_error", "error": "vm_not_found"}))
        return

    domain_name = str(vm.get("domain_name") or "").strip()
    if not domain_name:
        ws.send(json.dumps({"type": "terminal_error", "error": "vm_domain_missing"}))
        return

    query_string = ""
    environ = getattr(ws, "environ", None)
    if isinstance(environ, dict):
        query_string = str(environ.get("QUERY_STRING", ""))
    query = parse_qs(query_string)

    cols = _coerce_terminal_size((query.get("cols") or [80])[0], default_value=80, min_value=20, max_value=300)
    rows = _coerce_terminal_size((query.get("rows") or [24])[0], default_value=24, min_value=5, max_value=120)

    session_id, inbound_queue = _register_terminal_session(
        clean_node_id,
        vm_id=clean_vm_id,
        terminal_kind="vm_console",
    )
    _enqueue_agent_ws_message(
        clean_node_id,
        {
            "type": "vm_terminal_open",
            "session_id": session_id,
            "vm_id": clean_vm_id,
            "domain_name": domain_name,
            "cols": cols,
            "rows": rows,
        },
    )
    if not _is_agent_connected(clean_node_id):
        try:
            inbound_queue.put_nowait(
                {
                    "type": "terminal_data",
                    "session_id": session_id,
                    "data": "\r\n[waiting for agent websocket connection...]\r\n",
                }
            )
        except queue.Full:
            pass

    append_node_log(
        DB_PATH,
        node_id=clean_node_id,
        level="info",
        message="VM terminal session opened",
        meta={"session_id": session_id, "vm_id": clean_vm_id, "domain_name": domain_name},
    )

    ws.send(json.dumps({"type": "terminal_ready", "session_id": session_id}))

    try:
        while True:
            while True:
                try:
                    outbound = inbound_queue.get_nowait()
                except queue.Empty:
                    break
                ws.send(json.dumps(outbound))

            try:
                raw = ws.receive(timeout=0.2)
            except TimeoutError:
                continue
            if raw is None:
                # simple-websocket returns None on timeout when no frame is available.
                # Treat this as idle and keep the session open.
                continue

            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                ws.send(json.dumps({"type": "terminal_error", "error": "invalid_json"}))
                continue

            if not isinstance(payload, dict):
                ws.send(json.dumps({"type": "terminal_error", "error": "invalid_payload"}))
                continue

            message_type = payload.get("type")
            if message_type == "input":
                data = payload.get("data")
                if not isinstance(data, str):
                    continue
                _enqueue_agent_ws_message(
                    clean_node_id,
                    {
                        "type": "vm_terminal_input",
                        "session_id": session_id,
                        "vm_id": clean_vm_id,
                        "data": data,
                    },
                )
                continue

            if message_type == "resize":
                new_cols = _coerce_terminal_size(payload.get("cols"), default_value=cols, min_value=20, max_value=300)
                new_rows = _coerce_terminal_size(payload.get("rows"), default_value=rows, min_value=5, max_value=120)
                cols = new_cols
                rows = new_rows
                _enqueue_agent_ws_message(
                    clean_node_id,
                    {
                        "type": "vm_terminal_resize",
                        "session_id": session_id,
                        "vm_id": clean_vm_id,
                        "cols": new_cols,
                        "rows": new_rows,
                    },
                )
                continue

            if message_type == "ping":
                ws.send(json.dumps({"type": "pong"}))
                continue

            if message_type == "close":
                break
    except Exception as exc:
        log.info(
            f"VM terminal websocket closed node_id={clean_node_id} vm_id={clean_vm_id} "
            f"session_id={session_id} details={exc}"
        )
    finally:
        _unregister_terminal_session(session_id)
        _enqueue_agent_ws_message(
            clean_node_id,
            {
                "type": "vm_terminal_close",
                "session_id": session_id,
                "vm_id": clean_vm_id,
            },
        )
        append_node_log(
            DB_PATH,
            node_id=clean_node_id,
            level="info",
            message="VM terminal session closed",
            meta={"session_id": session_id, "vm_id": clean_vm_id, "domain_name": domain_name},
        )


@sock.route("/ws/agent")
def ws_agent(ws):
    node_id: str | None = None
    pair_token: str | None = None
    authenticated = False
    connection_id = str(uuid.uuid4())

    def send_message(payload: dict[str, Any]) -> None:
        ws.send(json.dumps(payload))

    try:
        while True:
            if authenticated and node_id and not _is_current_agent_connection(node_id, connection_id):
                send_message({"type": "error", "error": "superseded_connection"})
                break
            if authenticated and node_id:
                outbound_messages = _drain_agent_ws_messages(node_id, max_items=200)
                for outbound in outbound_messages:
                    send_message(outbound)
            try:
                raw = ws.receive(timeout=0.1)
            except TimeoutError:
                continue
            if raw is None:
                # simple-websocket returns None on timeout when idle.
                continue

            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                send_message({"type": "error", "error": "invalid_json"})
                continue

            if not isinstance(payload, dict):
                send_message({"type": "error", "error": "invalid_payload"})
                continue

            message_type = payload.get("type")
            if not authenticated:
                if message_type == "subscribe_logs":
                    requested_node_id = payload.get("node_id")
                    if not isinstance(requested_node_id, str) or not requested_node_id.strip():
                        send_message({"type": "error", "error": "node_id is required"})
                        break
                    limit_raw = payload.get("limit")
                    limit_input = str(limit_raw) if limit_raw is not None else None
                    limit = _coerce_logs_limit(limit_input)
                    _stream_node_logs_ws(ws, node_id=requested_node_id, limit=limit)
                    break

                if message_type != "auth":
                    send_message({"type": "error", "error": "auth_required"})
                    break

                candidate_node_id = payload.get("node_id")
                candidate_pair_token = payload.get("pair_token")
                if not isinstance(candidate_node_id, str) or not isinstance(candidate_pair_token, str):
                    send_message({"type": "error", "error": "invalid_auth_payload"})
                    break

                if not is_valid_node_token(DB_PATH, candidate_node_id, candidate_pair_token):
                    send_message({"type": "error", "error": "unauthorized"})
                    break

                node_id = candidate_node_id.strip()
                pair_token = candidate_pair_token.strip()
                authenticated = True
                previous_connection_id = _activate_agent_connection(node_id, connection_id)
                if previous_connection_id and previous_connection_id != connection_id:
                    append_node_log(
                        DB_PATH,
                        node_id=node_id,
                        level="warning",
                        message="Agent websocket connection replaced an existing session",
                        meta={
                            "previous_connection_id": previous_connection_id,
                            "new_connection_id": connection_id,
                        },
                    )
                append_node_log(
                    DB_PATH,
                    node_id=node_id,
                    level="info",
                    message="Agent websocket connected",
                    meta={"connection_id": connection_id},
                )
                log.info(f"Agent websocket connected node_id={node_id} connection_id={connection_id}")
                send_message({"type": "auth_ok"})
                continue

            if message_type == "log":
                message = payload.get("message")
                if not isinstance(message, str) or not message.strip():
                    send_message({"type": "error", "error": "message is required"})
                    continue

                level = payload.get("level")
                if not isinstance(level, str) or not level.strip():
                    level = "info"
                level = level.strip().lower()
                if level not in {"debug", "info", "warning", "error"}:
                    level = "info"

                timestamp = payload.get("timestamp")
                if not isinstance(timestamp, str):
                    timestamp = None

                meta = payload.get("meta")
                if not isinstance(meta, dict):
                    meta = None

                status = append_node_log(
                    DB_PATH,
                    node_id=node_id or "",
                    level=level,
                    message=message,
                    meta=meta,
                    timestamp=timestamp,
                )
                if status != "ok":
                    send_message({"type": "error", "error": "node_not_found"})
                    break
                continue

            if message_type == "heartbeat":
                message_payload = payload.get("payload")
                if not isinstance(message_payload, dict):
                    message_payload = {}
                message_payload["node_id"] = node_id

                status, _ = record_heartbeat(
                    DB_PATH,
                    pair_token=pair_token or "",
                    node_id=node_id or "",
                    payload=message_payload,
                )
                if status in {"missing_token", "invalid_token", "node_mismatch", "hostname_mismatch"}:
                    send_message({"type": "error", "error": "unauthorized"})
                    break
                continue

            if message_type == "command_result":
                if not isinstance(payload, dict):
                    send_message({"type": "error", "error": "invalid_payload"})
                    continue
                status, _ = _process_agent_command_result(node_id=node_id or "", payload=payload)
                if status == "invalid_command_id":
                    send_message({"type": "error", "error": "command_id is required"})
                elif status == "operation_not_found":
                    send_message({"type": "error", "error": "operation_not_found"})
                continue

            if message_type in {"terminal_data", "terminal_exit", "terminal_error"}:
                session_id = payload.get("session_id")
                if not isinstance(session_id, str) or not session_id.strip():
                    send_message({"type": "error", "error": "session_id is required"})
                    continue
                ok = _enqueue_terminal_session_event(session_id.strip(), payload)
                if not ok:
                    send_message({"type": "error", "error": "terminal_session_not_found"})
                continue

            if message_type == "ping":
                send_message({"type": "pong"})
                continue

            send_message({"type": "error", "error": "unsupported_type"})
    except Exception as exc:
        log.info(f"Agent websocket closed with error node_id={node_id} details={exc}")
    finally:
        if authenticated and node_id:
            was_active = _deactivate_agent_connection(node_id, connection_id)
            if was_active:
                _clear_agent_ws_messages(node_id)
                _close_terminal_sessions_for_node(node_id, "Agent websocket disconnected")
                append_node_log(
                    DB_PATH,
                    node_id=node_id,
                    level="warning",
                    message="Agent websocket disconnected",
                    meta={"connection_id": connection_id},
                )
                log.info(f"Agent websocket disconnected node_id={node_id} connection_id={connection_id}")


def _serve_index():
    return send_from_directory(UI_DIST, "index.html")


@app.get("/")
def root():
    if UI_DIST.exists():
        return _serve_index()
    return jsonify({"message": "UI build not found. Build master/ui to serve frontend."}), 200


@app.get("/<path:path>")
def spa(path: str):
    if path.startswith("api/nodes/") and path.endswith("/logs"):
        parts = path.split("/")
        if len(parts) == 4 and parts[0] == "api" and parts[1] == "nodes" and parts[3] == "logs":
            body, status = _resolve_logs_request(parts[2])
            return jsonify(body), status

    if path.startswith("api/") or path == "health":
        return _json_error(404, "not found")

    if not UI_DIST.exists():
        return _json_error(404, "not found")

    target_file = UI_DIST / path
    if target_file.exists() and target_file.is_file():
        return send_from_directory(UI_DIST, path)
    return _serve_index()


if __name__ == "__main__":
    host = "0.0.0.0"
    port = 8000
    try:
        local_ip = get_interface_ip(socket.AF_INET)
    except OSError:
        local_ip = "127.0.0.1"
    log.info(f"Starting master endpoint on {local_ip}:{port}")
    app.run(host=host, port=port)
