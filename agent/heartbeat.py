import json
import socket
import threading
from datetime import datetime, timezone
from typing import Any, Callable
from urllib import error, request


def build_heartbeat_payload(
    node_id: str,
    status: str = "alive",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "node_id": node_id,
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "hostname": socket.gethostname(),
    }
    if extra:
        payload["extra"] = extra
    return payload


def send_heartbeat(
    master_url: str,
    pair_token: str,
    payload: dict[str, Any],
    timeout_seconds: int = 5,
) -> tuple[bool, int | None, str]:
    data = json.dumps(payload).encode("utf-8")
    url = f"{master_url.rstrip('/')}/api/heartbeat"
    req = request.Request(
        url=url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {pair_token}",
        },
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout_seconds) as resp:
            status_code = resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
            return 200 <= status_code < 300, status_code, body
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return False, exc.code, body
    except Exception as exc:
        return False, None, str(exc)


class HeartbeatSender:
    def __init__(
        self,
        master_url: str,
        node_id: str,
        pair_token: str,
        interval_seconds: int = 10,
        timeout_seconds: int = 5,
        logger=None,
        extra_provider: Callable[[], dict[str, Any] | None] | None = None,
        on_auth_failure: Callable[[int | None, str], None] | None = None,
    ) -> None:
        self.master_url = master_url.rstrip("/")
        self.node_id = node_id
        self.pair_token = pair_token
        self.interval_seconds = interval_seconds
        self.timeout_seconds = timeout_seconds
        self.logger = logger
        self.extra_provider = extra_provider
        self.on_auth_failure = on_auth_failure
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="heartbeat-sender", daemon=True)
        self._thread.start()
        if self.logger:
            self.logger.info(f"Heartbeat sender started for {self.master_url}")

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2)
        if self.logger:
            self.logger.info("Heartbeat sender stopped")

    def _run(self) -> None:
        while not self._stop_event.is_set():
            extra = self.extra_provider() if self.extra_provider else None
            payload = build_heartbeat_payload(node_id=self.node_id, extra=extra)
            ok, status_code, details = send_heartbeat(
                master_url=self.master_url,
                pair_token=self.pair_token,
                payload=payload,
                timeout_seconds=self.timeout_seconds,
            )

            if self.logger:
                if ok:
                    self.logger.info(f"Heartbeat sent status={status_code}")
                elif status_code in {401, 403}:
                    self.logger.info(f"Heartbeat auth failed status={status_code} details={details}")
                else:
                    self.logger.info(f"Heartbeat failed status={status_code} details={details}")

            if not ok and status_code in {401, 403} and self.on_auth_failure:
                self.on_auth_failure(status_code, details)
                self._stop_event.wait(self.interval_seconds)
                continue

            self._stop_event.wait(self.interval_seconds)
