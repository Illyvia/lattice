from pathlib import Path
import json
from datetime import datetime, timezone
import logging
import os
import socket
import subprocess
import sys
import threading
import time
from typing import Any, Callable
from urllib import error, request
from urllib.parse import quote

ROOT_DIR = Path(__file__).resolve().parent.parent
AGENT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from agent.config import AgentConfig, load_config
from agent.heartbeat import HeartbeatSender
from agent.vm_libvirt import auto_install_vm_prerequisites, execute_vm_command, get_vm_capability
from agent.ws_stream import AgentWebSocketStreamer
from log_setup import setup_logger
from agent.system import get_runtime_metrics, get_system_info, log_system_info

CONFIG_PATH = AGENT_DIR / "config.json"
STATE_PATH = AGENT_DIR / "state.json"


if os.name == "nt":
    log_path = ROOT_DIR / "logs" / "log.txt"
else:
    log_path = Path("/var/log/lattice/log.txt")

log = setup_logger("agent", str(log_path))


class WebSocketLogHandler(logging.Handler):
    def __init__(self, streamer: AgentWebSocketStreamer) -> None:
        super().__init__(level=logging.INFO)
        self.streamer = streamer

    def emit(self, record: logging.LogRecord) -> None:
        if getattr(record, "raw_only", False):
            return
        message = record.getMessage()
        if not message:
            return
        created_at = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()
        self.streamer.send_log(
            level=record.levelname.lower(),
            message=message,
            meta={"logger": record.name},
            timestamp=created_at,
        )


def load_state(path: Path) -> dict[str, str] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None

    node_id = payload.get("node_id")
    pair_token = payload.get("pair_token")
    paired_at = payload.get("paired_at")
    if not all(isinstance(v, str) and v.strip() for v in [node_id, pair_token, paired_at]):
        return None
    return {
        "node_id": node_id.strip(),
        "pair_token": pair_token.strip(),
        "paired_at": paired_at.strip(),
    }


def save_state(path: Path, state: dict[str, str]) -> None:
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def clear_state(path: Path) -> None:
    if path.exists():
        path.unlink()


def _request_pair_once(config: AgentConfig) -> tuple[bool, int | None, dict[str, Any] | str]:
    info = get_system_info()
    payload = {
        "pair_code": config.pair_code,
        "agent": {
            "hostname": info["hardware"]["node"],
            "os": f"{info['os']['name']} {info['os']['release']}",
            "arch": info["arch"]["machine"],
            "hardware": info["hardware"]["processor"],
        },
    }
    req = request.Request(
        url=f"{config.master_url}/api/pair",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            data = json.loads(body) if body else {}
            return True, resp.getcode(), data
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            data = json.loads(body) if body else {}
        except json.JSONDecodeError:
            data = body
        return False, exc.code, data
    except Exception as exc:
        return False, None, str(exc)


def pair_until_success(config: AgentConfig) -> dict[str, str]:
    while True:
        ok, status_code, data = _request_pair_once(config)
        if ok and isinstance(data, dict):
            node_id = data.get("node_id")
            pair_token = data.get("pair_token")
            if isinstance(node_id, str) and isinstance(pair_token, str):
                state = {
                    "node_id": node_id,
                    "pair_token": pair_token,
                    "paired_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "master_url": config.master_url,
                }
                save_state(STATE_PATH, state)
                log.info(f"Paired with master node_id={node_id}")
                return state
            log.info("Pair response missing required fields")
        else:
            log.info(f"Pair attempt failed status={status_code} details={data}")

        time.sleep(config.pair_retry_seconds)


def build_heartbeat_extra() -> dict[str, Any]:
    info = get_system_info()
    payload = {
        "os": info["os"],
        "arch": info["arch"],
        "hardware": info["hardware"],
        "usage": get_runtime_metrics(),
        "vm": get_vm_capability(max_age_seconds=60),
    }
    git_commit = get_git_commit_hash()
    if git_commit:
        payload["git_commit"] = git_commit
    return payload


def _run_git_command(args: list[str]) -> tuple[int, str, str]:
    try:
        completed = subprocess.run(
            args,
            cwd=str(ROOT_DIR),
            capture_output=True,
            text=True,
            check=False,
        )
        return completed.returncode, completed.stdout.strip(), completed.stderr.strip()
    except Exception as exc:
        return 1, "", str(exc)


def get_git_commit_hash() -> str | None:
    rc, stdout, _ = _run_git_command(["git", "rev-parse", "HEAD"])
    if rc != 0:
        return None
    commit_hash = stdout.strip()
    return commit_hash if commit_hash else None


def _parse_ahead_behind(raw_counts: str) -> tuple[int, int] | None:
    parts = raw_counts.split()
    if len(parts) != 2:
        return None
    try:
        return int(parts[0]), int(parts[1])
    except ValueError:
        return None


def execute_agent_update(force: bool = False, branch: str | None = None) -> tuple[str, str, dict[str, Any]]:
    branch_name = branch.strip() if isinstance(branch, str) and branch.strip() else None

    rc, inside_repo, err = _run_git_command(["git", "rev-parse", "--is-inside-work-tree"])
    if rc != 0 or inside_repo.lower() != "true":
        return "failed", "Agent is not running from a git repository", {"stderr": err}

    # Fast-forward pulls frequently fail on dev nodes with local edits.
    # Return a clear reason before attempting network operations.
    rc, dirty_status, err = _run_git_command(["git", "status", "--porcelain"])
    if rc != 0:
        return "failed", "Unable to check working tree state", {"stderr": err}
    if dirty_status.strip() and not force:
        ignored_local_paths = {"agent/config.json", "agent\\config.json"}
        changed_lines = [line.rstrip() for line in dirty_status.splitlines() if line.strip()]
        changed_files: list[str] = []
        for line in changed_lines:
            # Porcelain format is "XY <path>".
            path = line[3:] if len(line) > 3 else line.lstrip()
            normalized = path.replace("\\", "/")
            if normalized in ignored_local_paths:
                continue
            changed_files.append(path)
        if changed_files:
            return (
                "failed",
                "Working tree has local changes; commit, stash, or discard changes before update",
                {"changed_files": changed_files[:25]},
            )

    fetch_args = ["git", "fetch", "--all", "--prune"]
    if branch_name:
        fetch_args = ["git", "fetch", "origin", branch_name, "--prune"]
    rc, _, err = _run_git_command(fetch_args)
    if rc != 0:
        return "failed", "git fetch failed", {"stderr": err, "branch": branch_name}

    rc, before_sha, err = _run_git_command(["git", "rev-parse", "HEAD"])
    if rc != 0:
        return "failed", "Unable to resolve current commit", {"stderr": err}

    upstream_ref: str | None = None
    if branch_name:
        upstream_ref = f"origin/{branch_name}"
    else:
        rc, upstream, err = _run_git_command(
            ["git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"]
        )
        if rc == 0 and upstream:
            upstream_ref = upstream
        elif not force:
            return "failed", "No upstream branch configured; set one or pass branch", {"stderr": err}

    ahead = 0
    behind = 0
    if upstream_ref:
        rc, counts, err = _run_git_command(["git", "rev-list", "--left-right", "--count", f"HEAD...{upstream_ref}"])
        parsed_counts = _parse_ahead_behind(counts)
        if rc != 0 or parsed_counts is None:
            return "failed", "Unable to compare local branch to upstream", {"stderr": err, "upstream": upstream_ref}
        ahead, behind = parsed_counts
        if behind == 0 and not force:
            return "up_to_date", "Agent code is already up to date", {
                "before": before_sha,
                "after": before_sha,
                "upstream": upstream_ref,
                "ahead": ahead,
                "behind": behind,
            }

    pull_args = ["git", "pull", "--ff-only"]
    if branch_name:
        pull_args = ["git", "pull", "--ff-only", "origin", branch_name]
    rc, stdout, err = _run_git_command(pull_args)
    if rc != 0:
        return "failed", "git pull failed", {"stderr": err, "stdout": stdout, "branch": branch_name}

    rc, after_sha, err = _run_git_command(["git", "rev-parse", "HEAD"])
    if rc != 0:
        return "failed", "Unable to resolve updated commit", {"stderr": err}

    if after_sha != before_sha:
        return "updated", "Agent code updated successfully", {
            "before": before_sha,
            "after": after_sha,
            "upstream": upstream_ref,
            "ahead": ahead,
            "behind": behind,
        }
    return "up_to_date", "No new commit applied", {
        "before": before_sha,
        "after": after_sha,
        "upstream": upstream_ref,
        "ahead": ahead,
        "behind": behind,
    }


def execute_terminal_shell(command_text: str) -> tuple[str, str, dict[str, Any]]:
    clean_command = (command_text or "").strip()
    if not clean_command:
        return "failed", "command is required", {}

    try:
        completed = subprocess.run(
            clean_command,
            cwd=str(ROOT_DIR),
            shell=True,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return "failed", "Command timed out", {"timeout_seconds": 120, "stdout": exc.stdout or "", "stderr": exc.stderr or ""}
    except Exception as exc:
        return "failed", f"Failed to execute command: {exc}", {}

    stdout_text = (completed.stdout or "")[:20000]
    stderr_text = (completed.stderr or "")[:20000]
    exit_code = int(completed.returncode)
    details = {"stdout": stdout_text, "stderr": stderr_text, "exit_code": exit_code}
    if exit_code == 0:
        return "succeeded", "Command completed", details
    return "failed", f"Command exited with code {exit_code}", details


def _build_agent_command_url(master_url: str, node_id: str, suffix: str) -> str:
    return f"{master_url.rstrip('/')}/api/nodes/{quote(node_id)}/commands/{suffix}"


class CommandPoller:
    def __init__(
        self,
        master_url: str,
        node_id: str,
        pair_token: str,
        execute_command: Callable[[dict[str, Any], Callable[..., None]], None],
        logger=None,
        on_auth_failure: Callable[[int | None, str], None] | None = None,
        poll_interval_seconds: int = 2,
        timeout_seconds: int = 10,
    ) -> None:
        self.master_url = master_url.rstrip("/")
        self.node_id = node_id
        self.pair_token = pair_token
        self.execute_command = execute_command
        self.logger = logger
        self.on_auth_failure = on_auth_failure
        self.poll_interval_seconds = max(1, poll_interval_seconds)
        self.timeout_seconds = timeout_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="command-poller", daemon=True)
        self._thread.start()
        if self.logger:
            self.logger.info("Agent command poller started")

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2)
        if self.logger:
            self.logger.info("Agent command poller stopped")

    def _post_json(self, url: str, payload: dict[str, Any]) -> tuple[bool, int | None, Any]:
        req = request.Request(
            url=url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.pair_token}",
                "X-Agent-Hostname": socket.gethostname(),
            },
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                status_code = resp.getcode()
                body = resp.read().decode("utf-8", errors="replace")
                if body.strip():
                    try:
                        parsed = json.loads(body)
                    except json.JSONDecodeError:
                        parsed = body
                else:
                    parsed = None
                return True, status_code, parsed
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(body) if body.strip() else {}
            except json.JSONDecodeError:
                parsed = body
            return False, exc.code, parsed
        except Exception as exc:
            return False, None, str(exc)

    def _send_result(
        self,
        command_id: str,
        command_type: str,
        status: str,
        message: str,
        details: dict[str, Any] | None = None,
        operation_id: str | None = None,
        vm_id: str | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "command_id": str(command_id),
            "command_type": str(command_type),
            "status": str(status),
            "message": str(message),
        }
        if isinstance(details, dict) and details:
            payload["details"] = details
        if isinstance(operation_id, str) and operation_id.strip():
            payload["operation_id"] = operation_id.strip()
        if isinstance(vm_id, str) and vm_id.strip():
            payload["vm_id"] = vm_id.strip()

        url = _build_agent_command_url(self.master_url, self.node_id, "result")
        ok, status_code, response = self._post_json(url, payload)
        if not ok and status_code in {401, 403} and self.on_auth_failure:
            self.on_auth_failure(status_code, str(response))
        if self.logger and (not ok):
            self.logger.info(
                f"Command result post failed status={status_code} details={response}"
            )

    def _run(self) -> None:
        next_url = _build_agent_command_url(self.master_url, self.node_id, "next")
        while not self._stop_event.is_set():
            ok, status_code, data = self._post_json(next_url, {})
            if ok and status_code == 204:
                self._stop_event.wait(self.poll_interval_seconds)
                continue

            if ok and status_code == 200:
                command = data.get("command") if isinstance(data, dict) else None
                if isinstance(command, dict):
                    self.execute_command(command, self._send_result)
                    continue
                self._stop_event.wait(self.poll_interval_seconds)
                continue

            if status_code in {401, 403} and self.on_auth_failure:
                if self.logger:
                    self.logger.info(
                        f"Command poll auth failure status={status_code} details={data}"
                    )
                self.on_auth_failure(status_code, str(data))
                self._stop_event.wait(self.poll_interval_seconds)
                continue

            if self.logger and status_code is not None:
                self.logger.info(f"Command poll failed status={status_code} details={data}")
            elif self.logger:
                self.logger.info(f"Command poll failed details={data}")
            self._stop_event.wait(self.poll_interval_seconds)


def main() -> None:
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
    log.info("Lattice agent started")
    log_system_info(log)

    try:
        config = load_config(CONFIG_PATH)
    except Exception as exc:
        log.info(f"Failed loading config: {exc}")
        raise SystemExit(1) from exc

    def bootstrap_vm_prerequisites() -> None:
        result = auto_install_vm_prerequisites(force=False)
        details = result.get("details")
        hint = None
        if isinstance(details, dict):
            stderr = str(details.get("stderr", "")).strip()
            stdout = str(details.get("stdout", "")).strip()
            for source in (stderr, stdout):
                if not source:
                    continue
                for raw_line in source.splitlines():
                    line = raw_line.strip()
                    if line:
                        hint = line
                        break
                if hint:
                    break

        if result.get("attempted"):
            ready = bool(result.get("ready"))
            message = (
                f"VM prerequisite auto-install attempted ready={ready} "
                f"manager={result.get('package_manager')} message={result.get('message')}"
            )
            if hint:
                message = f"{message} detail={hint}"
            log.info(message)
        else:
            message = f"VM prerequisite auto-install skipped message={result.get('message')}"
            if hint:
                message = f"{message} detail={hint}"
            log.info(message)

    threading.Thread(target=bootstrap_vm_prerequisites, daemon=True).start()

    state = load_state(STATE_PATH)
    if state and state.get("node_id") and state.get("pair_token"):
        log.info(f"Using existing pairing state node_id={state['node_id']}")
    else:
        state = pair_until_success(config)

    ws_streamer: AgentWebSocketStreamer | None = None
    ws_log_handler: WebSocketLogHandler | None = None
    command_poller: CommandPoller | None = None
    update_in_progress = threading.Event()
    vm_command_in_progress = threading.Event()
    terminal_command_in_progress = threading.Event()

    def execute_command(
        command: dict[str, Any],
        result_sender: Callable[..., None],
    ) -> None:
        if not isinstance(command, dict):
            return

        command_type = command.get("command_type")
        operation_id_raw = command.get("operation_id")
        operation_id = (
            operation_id_raw.strip()
            if isinstance(operation_id_raw, str) and operation_id_raw.strip()
            else None
        )
        vm_id_raw = command.get("vm_id")
        vm_id = vm_id_raw.strip() if isinstance(vm_id_raw, str) and vm_id_raw.strip() else None
        command_id = command.get("command_id")
        if not isinstance(command_id, str) or not command_id.strip():
            command_id = "unknown"

        def send_result(
            *,
            status: str,
            message: str,
            details: dict[str, Any] | None = None,
        ) -> None:
            try:
                result_sender(
                    command_id=command_id,
                    operation_id=operation_id,
                    vm_id=vm_id,
                    command_type=str(command_type),
                    status=status,
                    message=message,
                    details=details,
                )
            except Exception as exc:
                log.info(f"Failed to send command result command_id={command_id} details={exc}")

        if command_type == "update_agent":
            if update_in_progress.is_set():
                message = "Update already in progress"
                log.info(message)
                send_result(status="busy", message=message)
                return

            update_in_progress.set()
            try:
                force = bool(command.get("force", False))
                branch = command.get("branch") if isinstance(command.get("branch"), str) else None
                log.info(
                    f"Received update command command_id={command_id} force={force} branch={branch or 'upstream'}"
                )
                status, message, details = execute_agent_update(force=force, branch=branch)
                if status == "failed":
                    log.info(f"Agent update failed command_id={command_id} details={details}")
                else:
                    log.info(f"Agent update {status} command_id={command_id} details={details}")

                send_result(status=status, message=message, details=details)
            finally:
                update_in_progress.clear()
            return

        if isinstance(command_type, str) and command_type.startswith("vm_"):
            if vm_command_in_progress.is_set():
                message = "Another VM command is already in progress"
                log.info(f"{message} command_id={command_id}")
                send_result(status="busy", message=message)
                return

            vm_command_in_progress.set()
            try:
                log.info(f"Received VM command command_id={command_id} type={command_type} vm_id={vm_id}")
                send_result(status="running", message=f"{command_type} started")

                status, message, details = execute_vm_command(command)
                level = "error" if status != "succeeded" else "info"
                log.log(logging.ERROR if level == "error" else logging.INFO, f"VM command {command_type} -> {status}: {message}")
                send_result(status=status, message=message, details=details)
            finally:
                vm_command_in_progress.clear()
            return

        if command_type == "terminal_exec":
            if terminal_command_in_progress.is_set():
                message = "Another terminal command is already in progress"
                log.info(f"{message} command_id={command_id}")
                send_result(status="busy", message=message)
                return

            terminal_command_in_progress.set()
            try:
                command_text = command.get("command")
                if not isinstance(command_text, str):
                    send_result(status="failed", message="Missing terminal command text")
                    return

                log.info(f"Received terminal command command_id={command_id}")
                send_result(status="running", message="terminal_exec started")
                status, message, details = execute_terminal_shell(command_text)
                log.log(logging.ERROR if status != "succeeded" else logging.INFO, f"Terminal command -> {status}: {message}")
                send_result(status=status, message=message, details=details)
            finally:
                terminal_command_in_progress.clear()
            return

        log.info(f"Ignoring unsupported command type={command_type}")
        send_result(status="failed", message=f"Unsupported command type: {command_type}")

    def handle_ws_command(command: dict[str, Any]) -> None:
        nonlocal ws_streamer

        def ws_result_sender(
            *,
            command_id: str,
            operation_id: str | None,
            vm_id: str | None,
            command_type: str,
            status: str,
            message: str,
            details: dict[str, Any] | None = None,
        ) -> None:
            if ws_streamer:
                ws_streamer.send_command_result(
                    command_id=command_id,
                    operation_id=operation_id,
                    vm_id=vm_id,
                    command_type=command_type,
                    status=status,
                    message=message,
                    details=details,
                )

        execute_command(command, ws_result_sender)

    def start_ws_streamer(current_state: dict[str, str]) -> None:
        nonlocal ws_streamer, ws_log_handler
        if ws_log_handler:
            log.removeHandler(ws_log_handler)
            ws_log_handler = None
        if ws_streamer:
            ws_streamer.stop()
            ws_streamer = None

        ws_streamer = AgentWebSocketStreamer(
            master_url=config.master_url,
            node_id=current_state["node_id"],
            pair_token=current_state["pair_token"],
            command_handler=handle_ws_command,
        )
        ws_streamer.start()
        ws_log_handler = WebSocketLogHandler(ws_streamer)
        log.addHandler(ws_log_handler)
        log.info("Agent websocket log stream started")

    def start_command_poller(current_state: dict[str, str]) -> None:
        nonlocal command_poller
        if command_poller:
            command_poller.stop()
            command_poller = None

        command_poller = CommandPoller(
            master_url=config.master_url,
            node_id=current_state["node_id"],
            pair_token=current_state["pair_token"],
            execute_command=execute_command,
            logger=log,
            on_auth_failure=on_auth_failure,
            poll_interval_seconds=2,
            timeout_seconds=10,
        )
        command_poller.start()

    auth_failure_event = threading.Event()

    def on_auth_failure(status_code: int | None, details: str) -> None:
        log.info(f"Heartbeat auth failure requires re-pair status={status_code} details={details}")
        auth_failure_event.set()

    sender = HeartbeatSender(
        master_url=config.master_url,
        node_id=state["node_id"],
        pair_token=state["pair_token"],
        interval_seconds=config.heartbeat_interval_seconds,
        timeout_seconds=config.heartbeat_timeout_seconds,
        logger=log,
        extra_provider=build_heartbeat_extra,
        on_auth_failure=on_auth_failure,
    )
    start_ws_streamer(state)
    start_command_poller(state)
    sender.start()

    try:
        while True:
            if auth_failure_event.is_set():
                auth_failure_event.clear()
                sender.stop()
                if command_poller:
                    command_poller.stop()
                    command_poller = None
                clear_state(STATE_PATH)
                log.info("Cleared local state; retrying pair flow")
                state = pair_until_success(config)
                start_ws_streamer(state)
                start_command_poller(state)
                sender = HeartbeatSender(
                    master_url=config.master_url,
                    node_id=state["node_id"],
                    pair_token=state["pair_token"],
                    interval_seconds=config.heartbeat_interval_seconds,
                    timeout_seconds=config.heartbeat_timeout_seconds,
                    logger=log,
                    extra_provider=build_heartbeat_extra,
                    on_auth_failure=on_auth_failure,
                )
                sender.start()
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Agent shutdown requested")
    finally:
        if ws_log_handler:
            log.removeHandler(ws_log_handler)
            ws_log_handler = None
        if ws_streamer:
            ws_streamer.stop()
            ws_streamer = None
        if command_poller:
            command_poller.stop()
            command_poller = None
        sender.stop()


if __name__ == "__main__":
    main()
