import json
import queue
import socket
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import urlparse, urlunparse

try:
    import websocket
except Exception:  # pragma: no cover - runtime environment dependent
    websocket = None  # type: ignore[assignment]

try:
    import websockets
    from websockets.sync.client import connect as websockets_connect
except Exception:  # pragma: no cover - runtime environment dependent
    websockets = None  # type: ignore[assignment]
    websockets_connect = None  # type: ignore[assignment]


def build_agent_ws_url(master_url: str) -> str:
    parsed = urlparse(master_url.rstrip("/"))
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return urlunparse((scheme, parsed.netloc, "/ws/agent", "", "", ""))


def _safe_module_path(module: Any) -> str:
    if module is None:
        return "<not-installed>"
    path = getattr(module, "__file__", None)
    if isinstance(path, str) and path.strip():
        return path
    return "<unknown>"


def _resolve_timeout_exception(timeout_exc: Any) -> tuple[type[BaseException], ...]:
    candidates: list[type[BaseException]] = [socket.timeout, TimeoutError]
    if isinstance(timeout_exc, type) and issubclass(timeout_exc, BaseException):
        candidates.append(timeout_exc)
    unique: list[type[BaseException]] = []
    for candidate in candidates:
        if candidate not in unique:
            unique.append(candidate)
    return tuple(unique)


class _WebSocketConnection:
    def send(self, payload: str) -> None:
        raise NotImplementedError

    def recv(self, timeout_seconds: float | None = None) -> str | None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


class _WebsocketsSyncConnection(_WebSocketConnection):
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def send(self, payload: str) -> None:
        self._connection.send(payload)

    def recv(self, timeout_seconds: float | None = None) -> str | None:
        try:
            data = self._connection.recv(timeout=timeout_seconds, decode=True)
        except TimeoutError:
            return None

        if data is None:
            return None
        if isinstance(data, bytes):
            return data.decode("utf-8", errors="replace")
        return str(data)

    def close(self) -> None:
        self._connection.close()


class _WebsocketClientConnection(_WebSocketConnection):
    def __init__(self, connection: Any, timeout_exceptions: tuple[type[BaseException], ...]) -> None:
        self._connection = connection
        self._timeout_exceptions = timeout_exceptions

    def send(self, payload: str) -> None:
        self._connection.send(payload)

    def recv(self, timeout_seconds: float | None = None) -> str | None:
        if timeout_seconds is not None:
            try:
                self._connection.settimeout(timeout_seconds)
            except Exception:
                pass

        try:
            data = self._connection.recv()
        except Exception as exc:
            if isinstance(exc, self._timeout_exceptions):
                return None
            raise

        if data is None:
            return None
        if isinstance(data, bytes):
            return data.decode("utf-8", errors="replace")
        return str(data)

    def close(self) -> None:
        self._connection.close()


def _resolve_websocket_client_factory() -> tuple[Callable[[str, float], _WebSocketConnection] | None, str]:
    if websocket is None:
        return None, "<not-installed>"

    timeout_exc = getattr(websocket, "WebSocketTimeoutException", None)
    timeout_exceptions = _resolve_timeout_exception(timeout_exc)
    module_path = _safe_module_path(websocket)

    create_connection = getattr(websocket, "create_connection", None)
    if callable(create_connection):

        def _factory(url: str, timeout_seconds: float) -> _WebSocketConnection:
            # Some client stacks incorrectly negotiate permessage-deflate and then fail
            # to decode compressed frames. Explicitly sending an empty extensions header
            # keeps the stream uncompressed for compatibility.
            kwargs: dict[str, Any] = {
                "timeout": timeout_seconds,
                "header": ["Sec-WebSocket-Extensions:"],
            }
            try:
                kwargs["enable_multithread"] = True
                connection = create_connection(url, **kwargs)
            except TypeError:
                kwargs.pop("enable_multithread", None)
                connection = create_connection(url, **kwargs)
            return _WebsocketClientConnection(connection, timeout_exceptions)

        return _factory, module_path

    core = getattr(websocket, "_core", None)
    create_connection_core = getattr(core, "create_connection", None) if core is not None else None
    timeout_exc_core = getattr(core, "WebSocketTimeoutException", timeout_exc) if core is not None else timeout_exc
    timeout_exceptions_core = _resolve_timeout_exception(timeout_exc_core)
    if callable(create_connection_core):

        def _factory(url: str, timeout_seconds: float) -> _WebSocketConnection:
            connection = create_connection_core(url, timeout=timeout_seconds)
            return _WebsocketClientConnection(connection, timeout_exceptions_core)

        return _factory, module_path

    return None, module_path


def _resolve_ws_factory() -> tuple[Callable[[str, float], _WebSocketConnection] | None, str, str]:
    if callable(websockets_connect):

        def _factory(url: str, timeout_seconds: float) -> _WebSocketConnection:
            connection = websockets_connect(
                url,
                open_timeout=timeout_seconds,
                ping_interval=None,
                ping_timeout=None,
                compression=None,
                max_size=None,
            )
            return _WebsocketsSyncConnection(connection)

        return _factory, "websockets.sync", _safe_module_path(websockets)
    # The websocket-client fallback is intentionally disabled because some
    # node environments negotiate/receive RSV-compressed frames and then fail
    # with "rsv is not implemented, yet", causing reconnect loops.
    return None, "none", _safe_module_path(websockets)


class AgentWebSocketStreamer:
    def __init__(
        self,
        master_url: str,
        node_id: str,
        pair_token: str,
        command_handler: Callable[[dict], None] | None = None,
        terminal_handler: Callable[[dict], None] | None = None,
        status_logger: Callable[[str], None] | None = None,
        reconnect_seconds: int = 3,
        queue_size: int = 1000,
    ) -> None:
        self.ws_url = build_agent_ws_url(master_url)
        self.node_id = node_id
        self.pair_token = pair_token
        self.command_handler = command_handler
        self.terminal_handler = terminal_handler
        self.status_logger = status_logger
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

    def send_command_result(
        self,
        command_id: str,
        command_type: str,
        status: str,
        message: str,
        details: dict | None = None,
        operation_id: str | None = None,
        vm_id: str | None = None,
    ) -> None:
        event: dict = {
            "type": "command_result",
            "command_id": str(command_id),
            "command_type": str(command_type),
            "status": str(status),
            "message": str(message),
        }
        if isinstance(details, dict) and details:
            event["details"] = details
        if isinstance(operation_id, str) and operation_id.strip():
            event["operation_id"] = operation_id.strip()
        if isinstance(vm_id, str) and vm_id.strip():
            event["vm_id"] = vm_id.strip()
        self._enqueue(event)

    def send_terminal_data(self, session_id: str, data: str) -> None:
        if not isinstance(session_id, str) or not session_id.strip():
            return
        if not isinstance(data, str) or data == "":
            return
        self._enqueue({"type": "terminal_data", "session_id": session_id.strip(), "data": data})

    def send_terminal_exit(self, session_id: str, exit_code: int | None = None) -> None:
        if not isinstance(session_id, str) or not session_id.strip():
            return
        payload: dict[str, object] = {"type": "terminal_exit", "session_id": session_id.strip()}
        if isinstance(exit_code, int):
            payload["exit_code"] = exit_code
        self._enqueue(payload)

    def send_terminal_error(self, session_id: str, error_message: str) -> None:
        if not isinstance(session_id, str) or not session_id.strip():
            return
        self._enqueue(
            {
                "type": "terminal_error",
                "session_id": session_id.strip(),
                "error": str(error_message or "terminal error"),
            }
        )

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

    def _log_status(self, message: str) -> None:
        if not self.status_logger:
            return
        try:
            self.status_logger(str(message))
        except Exception:
            return

    def _run(self) -> None:
        ws_factory, ws_backend, ws_module_path = _resolve_ws_factory()
        if ws_factory is None:
            self._log_status(
                "Agent websocket unavailable: `websockets` is required for stable transport. "
                "If `websocket` is installed, remove it first. "
                "Run: python3 -m pip uninstall -y websocket && "
                "python3 -m pip install --upgrade websockets websocket-client"
            )
            while not self._stop_event.is_set():
                time.sleep(self.reconnect_seconds)
            return

        while not self._stop_event.is_set():
            ws = None
            try:
                self._log_status(
                    f"Agent websocket connecting to {self.ws_url} using {ws_backend} at {ws_module_path}"
                )
                ws = ws_factory(self.ws_url, 10)
                ws.send(
                    json.dumps(
                        {
                            "type": "auth",
                            "node_id": self.node_id,
                            "pair_token": self.pair_token,
                        }
                    )
                )

                auth_response_raw = ws.recv(timeout_seconds=10)
                if auth_response_raw is None:
                    raise RuntimeError("ws auth timed out")
                auth_response = json.loads(auth_response_raw) if auth_response_raw else {}
                if not isinstance(auth_response, dict) or auth_response.get("type") != "auth_ok":
                    raise RuntimeError(f"ws auth failed: {auth_response}")
                self._log_status("Agent websocket authenticated")

                last_ping_at = time.monotonic()
                while not self._stop_event.is_set():
                    sent_outbound = False
                    # Drain a bounded batch each loop to avoid 1-message/second throttling
                    # under high-frequency terminal output.
                    for _ in range(200):
                        try:
                            event = self._queue.get_nowait()
                        except queue.Empty:
                            break
                        ws.send(json.dumps(event))
                        sent_outbound = True

                    try:
                        # Keep control-plane responsiveness without stalling outbound flush.
                        inbound_timeout = 0.05 if sent_outbound else 1
                        inbound_raw = ws.recv(timeout_seconds=inbound_timeout)
                        if inbound_raw:
                            inbound = json.loads(inbound_raw)
                            if (
                                isinstance(inbound, dict)
                                and inbound.get("type") == "command"
                                and self.command_handler
                            ):
                                threading.Thread(
                                    target=self.command_handler,
                                    args=(inbound,),
                                    name="agent-command-handler",
                                    daemon=True,
                                ).start()
                            if (
                                isinstance(inbound, dict)
                                and inbound.get("type")
                                in {
                                    "terminal_open",
                                    "terminal_input",
                                    "terminal_resize",
                                    "terminal_close",
                                    "vm_terminal_open",
                                    "vm_terminal_input",
                                    "vm_terminal_resize",
                                    "vm_terminal_close",
                                }
                                and self.terminal_handler
                            ):
                                # Terminal control/data must preserve strict ordering (open -> input -> resize -> close).
                                self.terminal_handler(inbound)
                    except Exception:
                        raise

                    now = time.monotonic()
                    if now - last_ping_at >= 15:
                        ws.send(json.dumps({"type": "ping"}))
                        last_ping_at = now
            except Exception as exc:
                if not self._stop_event.is_set():
                    self._log_status(
                        f"Agent websocket disconnected, retrying in {self.reconnect_seconds}s: {exc}"
                    )
                time.sleep(self.reconnect_seconds)
            finally:
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:
                        pass
