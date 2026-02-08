from pathlib import Path
import json
from datetime import datetime, timezone
import logging
import os
import sys
import threading
import time
from typing import Any
from urllib import error, request

ROOT_DIR = Path(__file__).resolve().parent.parent
AGENT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from agent.config import AgentConfig, load_config
from agent.heartbeat import HeartbeatSender
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
    return {
        "os": info["os"],
        "arch": info["arch"],
        "hardware": info["hardware"],
        "usage": get_runtime_metrics(),
    }


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

    state = load_state(STATE_PATH)
    if state and state.get("node_id") and state.get("pair_token"):
        log.info(f"Using existing pairing state node_id={state['node_id']}")
    else:
        state = pair_until_success(config)

    ws_streamer: AgentWebSocketStreamer | None = None
    ws_log_handler: WebSocketLogHandler | None = None

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
        )
        ws_streamer.start()
        ws_log_handler = WebSocketLogHandler(ws_streamer)
        log.addHandler(ws_log_handler)
        log.info("Agent websocket log stream started")

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
    sender.start()

    try:
        while True:
            if auth_failure_event.is_set():
                auth_failure_event.clear()
                sender.stop()
                clear_state(STATE_PATH)
                log.info("Cleared local state; retrying pair flow")
                state = pair_until_success(config)
                start_ws_streamer(state)
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
        sender.stop()


if __name__ == "__main__":
    main()
