import os
import platform
import shutil
import subprocess
import threading
import time
from typing import Any, Callable


_CAPABILITY_LOCK = threading.Lock()
_CAPABILITY_CACHE: dict[str, Any] = {"checked_at": 0.0, "value": None}
_AUTO_INSTALL_LOCK = threading.Lock()
_AUTO_INSTALL_STATE: dict[str, Any] = {"last_attempt": 0.0}


def _run(cmd: list[str], timeout_seconds: int = 120) -> tuple[int, str, str]:
    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
        return completed.returncode, completed.stdout.strip(), completed.stderr.strip()
    except Exception as exc:
        return 1, "", str(exc)


def _run_sudo(cmd: list[str], timeout_seconds: int = 120) -> tuple[int, str, str]:
    # If the agent runs as root, execute directly and avoid depending on sudo.
    if hasattr(os, "geteuid"):
        try:
            if os.geteuid() == 0:
                return _run(cmd, timeout_seconds=timeout_seconds)
        except Exception:
            pass
    return _run(["sudo", "-n", *cmd], timeout_seconds=timeout_seconds)


def _first_error_line(stdout: str, stderr: str) -> str:
    for source in (stderr, stdout):
        if not source:
            continue
        for raw_line in source.splitlines():
            line = raw_line.strip()
            if line:
                return line
    return "unknown error"


def _looks_like_apt_lock_error(stdout: str, stderr: str) -> bool:
    combined = f"{stdout}\n{stderr}".lower()
    lock_markers = [
        "could not get lock",
        "unable to acquire the dpkg frontend lock",
        "is another process using it",
        "/var/lib/dpkg/lock",
        "/var/lib/apt/lists/lock",
    ]
    return any(marker in combined for marker in lock_markers)


def _run_sudo_with_retry(
    cmd: list[str],
    timeout_seconds: int,
    retries: int,
    retry_delay_seconds: int,
    retry_on: Callable[[str, str], bool] | None = None,
) -> tuple[int, str, str]:
    attempts = max(1, retries)
    last_rc, last_out, last_err = 1, "", "command not executed"
    for attempt in range(1, attempts + 1):
        rc, out, err = _run_sudo(cmd, timeout_seconds=timeout_seconds)
        last_rc, last_out, last_err = rc, out, err
        if rc == 0:
            return rc, out, err
        if attempt >= attempts:
            break
        if callable(retry_on) and not retry_on(out, err):
            break
        time.sleep(max(1, retry_delay_seconds))
    return last_rc, last_out, last_err


def _detect_linux_package_manager() -> str | None:
    if shutil.which("apt-get"):
        return "apt"
    if shutil.which("dnf"):
        return "dnf"
    if shutil.which("yum"):
        return "yum"
    if shutil.which("pacman"):
        return "pacman"
    if shutil.which("zypper"):
        return "zypper"
    return None


def _detect_capability() -> dict[str, Any]:
    if platform.system().lower() != "linux":
        return {
            "provider": "docker",
            "ready": False,
            "message": "docker container support is Linux-only in v1",
            "missing_tools": [],
        }

    required_tools = ["docker"]
    missing_tools = [tool for tool in required_tools if shutil.which(tool) is None]
    if missing_tools:
        return {
            "provider": "docker",
            "ready": False,
            "message": "Missing required container tools",
            "missing_tools": missing_tools,
        }

    rc, stdout, stderr = _run_sudo(["docker", "info", "--format", "{{.ServerVersion}}"], timeout_seconds=30)
    if rc != 0:
        lower_err = f"{stderr}\n{stdout}".lower()
        message = "Unable to access docker daemon"
        if (
            "password is required" in lower_err
            or "no tty present" in lower_err
            or "not in the sudoers file" in lower_err
        ):
            message = "sudo -n denied; configure NOPASSWD sudo or install prerequisites manually"
        return {
            "provider": "docker",
            "ready": False,
            "message": message,
            "missing_tools": [],
            "details": stderr or stdout,
        }

    version = stdout.strip() or None
    payload: dict[str, Any] = {
        "provider": "docker",
        "ready": True,
        "message": "docker ready",
        "missing_tools": [],
    }
    if version:
        payload["version"] = version
    return payload


def _refresh_capability_cache(value: dict[str, Any]) -> None:
    with _CAPABILITY_LOCK:
        _CAPABILITY_CACHE["checked_at"] = time.monotonic()
        _CAPABILITY_CACHE["value"] = dict(value)


def _install_linux_prerequisites(package_manager: str) -> tuple[bool, str, dict[str, Any]]:
    if package_manager == "apt":
        update_cmd = ["apt-get", "-o", "Acquire::Retries=3", "update"]
        install_cmd = ["apt-get", "install", "-y", "--no-install-recommends", "docker.io"]
        rc, out, err = _run_sudo_with_retry(
            update_cmd,
            timeout_seconds=1200,
            retries=4,
            retry_delay_seconds=8,
            retry_on=_looks_like_apt_lock_error,
        )
        if rc != 0:
            reason = _first_error_line(out, err)
            return False, f"apt-get update failed: {reason}", {"stdout": out, "stderr": err}
        rc, out, err = _run_sudo_with_retry(
            install_cmd,
            timeout_seconds=1800,
            retries=4,
            retry_delay_seconds=8,
            retry_on=_looks_like_apt_lock_error,
        )
        if rc != 0:
            reason = _first_error_line(out, err)
            return False, f"apt-get install failed: {reason}", {"stdout": out, "stderr": err}
        _run_sudo(["systemctl", "enable", "--now", "docker"], timeout_seconds=120)
        return True, "Installed container prerequisites with apt", {}

    if package_manager == "dnf":
        cmd = ["dnf", "install", "-y", "docker"]
        rc, out, err = _run_sudo(cmd, timeout_seconds=1800)
        if rc != 0:
            reason = _first_error_line(out, err)
            return False, f"dnf install failed: {reason}", {"stdout": out, "stderr": err}
        _run_sudo(["systemctl", "enable", "--now", "docker"], timeout_seconds=120)
        return True, "Installed container prerequisites with dnf", {}

    if package_manager == "yum":
        cmd = ["yum", "install", "-y", "docker"]
        rc, out, err = _run_sudo(cmd, timeout_seconds=1800)
        if rc != 0:
            reason = _first_error_line(out, err)
            return False, f"yum install failed: {reason}", {"stdout": out, "stderr": err}
        _run_sudo(["systemctl", "enable", "--now", "docker"], timeout_seconds=120)
        return True, "Installed container prerequisites with yum", {}

    if package_manager == "pacman":
        cmd = ["pacman", "-Sy", "--noconfirm", "docker"]
        rc, out, err = _run_sudo(cmd, timeout_seconds=1800)
        if rc != 0:
            reason = _first_error_line(out, err)
            return False, f"pacman install failed: {reason}", {"stdout": out, "stderr": err}
        _run_sudo(["systemctl", "enable", "--now", "docker"], timeout_seconds=120)
        return True, "Installed container prerequisites with pacman", {}

    if package_manager == "zypper":
        cmd = ["zypper", "--non-interactive", "install", "docker"]
        rc, out, err = _run_sudo(cmd, timeout_seconds=1800)
        if rc != 0:
            reason = _first_error_line(out, err)
            return False, f"zypper install failed: {reason}", {"stdout": out, "stderr": err}
        _run_sudo(["systemctl", "enable", "--now", "docker"], timeout_seconds=120)
        return True, "Installed container prerequisites with zypper", {}

    return False, "Unsupported package manager", {"package_manager": package_manager}


def auto_install_container_prerequisites(force: bool = False) -> dict[str, Any]:
    if platform.system().lower() != "linux":
        return {"attempted": False, "ready": False, "message": "Auto-install only runs on Linux"}

    with _AUTO_INSTALL_LOCK:
        now = time.monotonic()
        last_attempt = float(_AUTO_INSTALL_STATE.get("last_attempt", 0.0))
        cooldown_seconds = 600
        if not force and (now - last_attempt) < cooldown_seconds:
            current = get_container_capability(max_age_seconds=0)
            return {
                "attempted": False,
                "ready": bool(current.get("ready")),
                "message": "Auto-install attempt is in cooldown",
                "capability": current,
            }

        _AUTO_INSTALL_STATE["last_attempt"] = now
        capability = _detect_capability()
        if capability.get("ready"):
            _refresh_capability_cache(capability)
            return {
                "attempted": False,
                "ready": True,
                "message": "Prerequisites already installed",
                "capability": capability,
            }

        missing_tools = capability.get("missing_tools")
        if not isinstance(missing_tools, list) or len(missing_tools) == 0:
            _refresh_capability_cache(capability)
            return {
                "attempted": False,
                "ready": False,
                "message": str(capability.get("message", "Container capability is not ready")),
                "capability": capability,
            }

        package_manager = _detect_linux_package_manager()
        if not package_manager:
            _refresh_capability_cache(capability)
            return {
                "attempted": False,
                "ready": False,
                "message": "No supported package manager found for auto-install",
                "capability": capability,
            }

        ok, install_message, details = _install_linux_prerequisites(package_manager)
        refreshed = _detect_capability()
        _refresh_capability_cache(refreshed)
        return {
            "attempted": True,
            "ok": ok,
            "ready": bool(refreshed.get("ready")),
            "message": install_message,
            "details": details,
            "package_manager": package_manager,
            "capability": refreshed,
        }


def get_container_capability(max_age_seconds: int = 60) -> dict[str, Any]:
    with _CAPABILITY_LOCK:
        now = time.monotonic()
        checked_at = float(_CAPABILITY_CACHE["checked_at"])
        cached = _CAPABILITY_CACHE["value"]
        if cached is not None and now - checked_at <= max(1, max_age_seconds):
            return dict(cached)

        fresh = _detect_capability()
        _CAPABILITY_CACHE["checked_at"] = now
        _CAPABILITY_CACHE["value"] = fresh
        return dict(fresh)


def _container_state(runtime_name: str) -> str:
    rc, stdout, _ = _run_sudo(
        ["docker", "inspect", "-f", "{{.State.Status}}", runtime_name],
        timeout_seconds=30,
    )
    if rc != 0:
        return "unknown"
    return stdout.strip().lower() or "unknown"


def _container_runtime_id(runtime_name: str) -> str | None:
    rc, stdout, _ = _run_sudo(
        ["docker", "inspect", "-f", "{{.Id}}", runtime_name],
        timeout_seconds=30,
    )
    if rc != 0:
        return None
    value = stdout.strip()
    return value or None


def _derive_state(runtime_state: str, fallback: str = "unknown") -> str:
    normalized = (runtime_state or "").strip().lower()
    if not normalized:
        return fallback
    if "running" in normalized:
        return "running"
    if "restarting" in normalized:
        return "restarting"
    if any(token in normalized for token in {"exited", "created", "dead", "stopped"}):
        return "stopped"
    if any(token in normalized for token in {"removing", "deleting"}):
        return "deleting"
    return fallback


def _create_container(spec: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    container_id = str(spec.get("container_id", "")).strip()
    name = str(spec.get("name", "")).strip()
    runtime_name = str(spec.get("runtime_name", "")).strip()
    image = str(spec.get("image", "")).strip()
    command_text_raw = spec.get("command_text")
    start_immediately = bool(spec.get("start_immediately", True))
    command_text = command_text_raw.strip() if isinstance(command_text_raw, str) else ""

    if not container_id or not name or not runtime_name or not image:
        return "failed", "Invalid container_create payload", {}

    create_cmd = ["docker", "create", "--name", runtime_name, image]
    if command_text:
        create_cmd.extend(["/bin/sh", "-lc", command_text])
    rc, stdout, stderr = _run_sudo(create_cmd, timeout_seconds=240)
    if rc != 0:
        combined = f"{stderr}\n{stdout}".strip()
        return "failed", f"Docker create failed: {_first_error_line(stdout, stderr)}", {"stderr": combined}

    runtime_id = stdout.strip().splitlines()[0].strip() if stdout.strip() else None
    if start_immediately:
        rc, out_start, err_start = _run_sudo(["docker", "start", runtime_name], timeout_seconds=120)
        if rc != 0:
            return (
                "failed",
                f"Docker start failed: {_first_error_line(out_start, err_start)}",
                {"stderr": f"{err_start}\n{out_start}".strip()},
            )

    runtime_state = _container_state(runtime_name)
    return "succeeded", "Container created", {
        "container_id": container_id,
        "runtime_name": runtime_name,
        "runtime_id": runtime_id or _container_runtime_id(runtime_name),
        "image": image,
        "state": _derive_state(runtime_state, fallback="running" if start_immediately else "stopped"),
        "runtime_state": runtime_state,
    }


def execute_container_command(command: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    try:
        capability = get_container_capability()
        if not capability.get("ready"):
            auto_install_result = auto_install_container_prerequisites(force=False)
            capability = get_container_capability(max_age_seconds=0)
            if not capability.get("ready"):
                return (
                    "failed",
                    str(capability.get("message", "Container capability is not ready")),
                    {"capability": capability, "auto_install": auto_install_result},
                )

        command_type = str(command.get("command_type", "")).strip()
        container_id = str(command.get("container_id", "")).strip()
        runtime_name = str(command.get("runtime_name", "")).strip()

        if command_type == "container_create":
            spec = command.get("spec")
            if not isinstance(spec, dict):
                return "failed", "Missing create spec", {}
            return _create_container(spec)

        if not runtime_name:
            container_spec = command.get("container_spec")
            if isinstance(container_spec, dict):
                runtime_name = str(container_spec.get("runtime_name", "")).strip()
        if not runtime_name:
            return "failed", "runtime_name is required", {}

        if command_type == "container_start":
            rc, stdout, stderr = _run_sudo(["docker", "start", runtime_name], timeout_seconds=90)
            combined = f"{stdout}\n{stderr}".lower()
            if rc != 0 and "already started" not in combined and "is already running" not in combined:
                return "failed", f"Unable to start container: {_first_error_line(stdout, stderr)}", {}
            runtime_state = _container_state(runtime_name)
            return "succeeded", "Container started", {
                "container_id": container_id,
                "runtime_name": runtime_name,
                "runtime_id": _container_runtime_id(runtime_name),
                "state": _derive_state(runtime_state, fallback="running"),
                "runtime_state": runtime_state,
            }

        if command_type == "container_stop":
            rc, stdout, stderr = _run_sudo(["docker", "stop", "--time", "15", runtime_name], timeout_seconds=120)
            combined = f"{stdout}\n{stderr}".lower()
            if rc != 0 and "is not running" not in combined:
                return "failed", f"Unable to stop container: {_first_error_line(stdout, stderr)}", {}
            runtime_state = _container_state(runtime_name)
            return "succeeded", "Container stopped", {
                "container_id": container_id,
                "runtime_name": runtime_name,
                "runtime_id": _container_runtime_id(runtime_name),
                "state": _derive_state(runtime_state, fallback="stopped"),
                "runtime_state": runtime_state,
            }

        if command_type == "container_restart":
            rc, stdout, stderr = _run_sudo(["docker", "restart", runtime_name], timeout_seconds=120)
            if rc != 0:
                return "failed", f"Unable to restart container: {_first_error_line(stdout, stderr)}", {}
            runtime_state = _container_state(runtime_name)
            return "succeeded", "Container restarted", {
                "container_id": container_id,
                "runtime_name": runtime_name,
                "runtime_id": _container_runtime_id(runtime_name),
                "state": _derive_state(runtime_state, fallback="running"),
                "runtime_state": runtime_state,
            }

        if command_type == "container_delete":
            rc, stdout, stderr = _run_sudo(["docker", "rm", "-f", runtime_name], timeout_seconds=120)
            combined = f"{stdout}\n{stderr}".lower()
            if rc != 0 and "no such container" not in combined:
                return "failed", f"Unable to delete container: {_first_error_line(stdout, stderr)}", {}
            return "succeeded", "Container deleted", {
                "container_id": container_id,
                "runtime_name": runtime_name,
                "state": "deleted",
            }

        if command_type == "container_sync":
            rc, stdout, stderr = _run_sudo(
                ["docker", "ps", "-a", "--no-trunc", "--format", "{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.State}}\t{{.Status}}"],
                timeout_seconds=60,
            )
            if rc != 0:
                return "failed", f"Unable to sync container state: {_first_error_line(stdout, stderr)}", {}
            containers: list[dict[str, Any]] = []
            for line in stdout.splitlines():
                raw = line.strip()
                if not raw:
                    continue
                parts = raw.split("\t")
                if len(parts) < 5:
                    continue
                runtime_id_raw, name_raw, image_raw, state_raw, status_raw = parts[:5]
                containers.append(
                    {
                        "runtime_id": runtime_id_raw.strip(),
                        "runtime_name": name_raw.strip(),
                        "image": image_raw.strip(),
                        "runtime_state": state_raw.strip().lower(),
                        "status_text": status_raw.strip(),
                        "state": _derive_state(state_raw, fallback="unknown"),
                    }
                )
            return "succeeded", "Container sync complete", {"containers": containers}

        return "failed", f"Unsupported container command type: {command_type}", {}
    except Exception as exc:
        return "failed", f"Container command exception: {exc}", {}
