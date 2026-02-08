from pathlib import Path
import json
import os
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
    append_node_log,
    create_node,
    delete_node,
    init_db,
    is_valid_node_token,
    list_node_logs,
    list_nodes,
    pair_node,
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

app = Flask(__name__)
sock = Sock(app)

AGENT_COMMANDS_LOCK = threading.Lock()
PENDING_AGENT_COMMANDS: dict[str, list[dict[str, Any]]] = {}
CONNECTED_AGENT_NODES: set[str] = set()


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


def _set_agent_connected(node_id: str, connected: bool) -> None:
    with AGENT_COMMANDS_LOCK:
        if connected:
            CONNECTED_AGENT_NODES.add(node_id)
        else:
            CONNECTED_AGENT_NODES.discard(node_id)


def _is_agent_connected(node_id: str) -> bool:
    with AGENT_COMMANDS_LOCK:
        return node_id in CONNECTED_AGENT_NODES


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


@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.get("/api/nodes")
def get_nodes():
    return jsonify(list_nodes(DB_PATH)), 200


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
    if status == "node_mismatch":
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
    connected = _is_agent_connected(node_id)
    log.info(
        f"Queued update command node_id={node_id} command_id={command_id} connected={connected}"
    )
    return jsonify({"ok": True, "command_id": command_id, "queued": True, "agent_connected": connected}), 202


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


@sock.route("/ws/agent")
def ws_agent(ws):
    node_id: str | None = None
    pair_token: str | None = None
    authenticated = False

    def send_message(payload: dict[str, Any]) -> None:
        ws.send(json.dumps(payload))

    def send_pending_command() -> None:
        if not authenticated or not node_id:
            return
        command = _dequeue_agent_command(node_id)
        if not command:
            return
        send_message(command)
        append_node_log(
            DB_PATH,
            node_id=node_id,
            level="info",
            message=f"Sent agent command {command.get('command_type', 'unknown')}",
            meta={"command_id": command.get("command_id"), "command": command},
        )

    try:
        while True:
            send_pending_command()
            raw = ws.receive()
            if raw is None:
                break

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
                _set_agent_connected(node_id, True)
                append_node_log(
                    DB_PATH,
                    node_id=node_id,
                    level="info",
                    message="Agent websocket connected",
                )
                log.info(f"Agent websocket connected node_id={node_id}")
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
                if status in {"missing_token", "invalid_token", "node_mismatch"}:
                    send_message({"type": "error", "error": "unauthorized"})
                    break
                continue

            if message_type == "command_result":
                command_id = payload.get("command_id")
                command_type = payload.get("command_type")
                status = payload.get("status")
                message = payload.get("message")
                details = payload.get("details")

                if not isinstance(command_id, str) or not command_id.strip():
                    send_message({"type": "error", "error": "command_id is required"})
                    continue
                if not isinstance(command_type, str) or not command_type.strip():
                    command_type = "unknown"
                if not isinstance(status, str) or not status.strip():
                    status = "unknown"
                if not isinstance(message, str) or not message.strip():
                    message = "No details provided"
                if not isinstance(details, dict):
                    details = None

                level = "info"
                if status in {"failed", "error"}:
                    level = "error"
                elif status in {"busy"}:
                    level = "warning"

                append_node_log(
                    DB_PATH,
                    node_id=node_id or "",
                    level=level,
                    message=f"Agent command {command_type} -> {status}: {message}",
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
                continue

            if message_type == "ping":
                send_message({"type": "pong"})
                continue

            send_message({"type": "error", "error": "unsupported_type"})
    except Exception as exc:
        log.info(f"Agent websocket closed with error node_id={node_id} details={exc}")
    finally:
        if authenticated and node_id:
            _set_agent_connected(node_id, False)
            append_node_log(
                DB_PATH,
                node_id=node_id,
                level="warning",
                message="Agent websocket disconnected",
            )
            log.info(f"Agent websocket disconnected node_id={node_id}")


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
