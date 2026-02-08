import json
import queue
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse

import websocket


def build_agent_ws_url(master_url: str) -> str:
    parsed = urlparse(master_url.rstrip("/"))
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return urlunparse((scheme, parsed.netloc, "/ws/agent", "", "", ""))


class AgentWebSocketStreamer:
    def __init__(
        self,
        master_url: str,
        node_id: str,
        pair_token: str,
        reconnect_seconds: int = 3,
        queue_size: int = 1000,
    ) -> None:
        self.ws_url = build_agent_ws_url(master_url)
        self.node_id = node_id
        self.pair_token = pair_token
        self.reconnect_seconds = max(1, reconnect_seconds)
        self._queue: queue.Queue[dict] = queue.Queue(maxsize=max(10, queue_size))
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="agent-ws-stream", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2)

    def send_log(
        self,
        level: str,
        message: str,
        meta: dict | None = None,
        timestamp: str | None = None,
    ) -> None:
        if not isinstance(message, str) or not message.strip():
            return
        event = {
            "type": "log",
            "level": str(level or "info").strip().lower(),
            "message": message,
            "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
        }
        if isinstance(meta, dict) and meta:
            event["meta"] = meta
        self._enqueue(event)

    def send_heartbeat(self, payload: dict) -> None:
        if not isinstance(payload, dict):
            return
        self._enqueue({"type": "heartbeat", "payload": payload})

    def _enqueue(self, event: dict) -> None:
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            try:
                _ = self._queue.get_nowait()
            except queue.Empty:
                pass
            try:
                self._queue.put_nowait(event)
            except queue.Full:
                return

    def _run(self) -> None:
        while not self._stop_event.is_set():
            ws = None
            try:
                ws = websocket.create_connection(self.ws_url, timeout=10)
                ws.send(
                    json.dumps(
                        {
                            "type": "auth",
                            "node_id": self.node_id,
                            "pair_token": self.pair_token,
                        }
                    )
                )

                auth_response_raw = ws.recv()
                auth_response = json.loads(auth_response_raw) if auth_response_raw else {}
                if not isinstance(auth_response, dict) or auth_response.get("type") != "auth_ok":
                    raise RuntimeError(f"ws auth failed: {auth_response}")

                while not self._stop_event.is_set():
                    try:
                        event = self._queue.get(timeout=1)
                    except queue.Empty:
                        continue
                    ws.send(json.dumps(event))
            except Exception:
                time.sleep(self.reconnect_seconds)
            finally:
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:
                        pass
