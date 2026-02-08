import hashlib
import os
import platform
import re
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable
from urllib import request


VM_ROOT = Path("/var/lib/lattice/vms")
IMAGE_ROOT = Path("/var/lib/lattice/vm-images")
_IPV4_REGEX = re.compile(r"(?<!\d)(?:\d{1,3}\.){3}\d{1,3}(?!\d)")
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


def _looks_like_missing_domain_error(stdout: str, stderr: str) -> bool:
    combined = f"{stdout}\n{stderr}".lower()
    markers = [
        "domain not found",
        "failed to get domain",
        "no domain with matching name",
        "domain does not exist",
    ]
    return any(marker in combined for marker in markers)


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


def _compute_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest().lower()


def _bridge_exists(name: str) -> bool:
    interface = (name or "").strip()
    if not interface:
        return False
    rc, _, _ = _run(["ip", "link", "show", interface], timeout_seconds=20)
    return rc == 0


def _ensure_libvirt_default_network() -> bool:
    rc, stdout, _ = _run_sudo(["virsh", "net-info", "default"], timeout_seconds=30)
    if rc != 0:
        # Try defining the default network from common libvirt XML locations.
        candidate_paths = [
            Path("/usr/share/libvirt/networks/default.xml"),
            Path("/etc/libvirt/qemu/networks/default.xml"),
        ]
        defined = False
        for candidate in candidate_paths:
            try:
                if not candidate.exists():
                    continue
            except Exception:
                continue
            define_rc, _, _ = _run_sudo(["virsh", "net-define", str(candidate)], timeout_seconds=60)
            if define_rc == 0:
                defined = True
                break
        if not defined:
            return False
        rc, stdout, _ = _run_sudo(["virsh", "net-info", "default"], timeout_seconds=30)
        if rc != 0:
            return False
    lower = stdout.lower()
    if "active: yes" in lower:
        return True
    _run_sudo(["virsh", "net-start", "default"], timeout_seconds=60)
    _run_sudo(["virsh", "net-autostart", "default"], timeout_seconds=30)
    rc, stdout, _ = _run_sudo(["virsh", "net-info", "default"], timeout_seconds=30)
    return rc == 0 and "active: yes" in stdout.lower()


def _resolve_network_argument(requested_bridge: str) -> tuple[str | None, str | None]:
    bridge = (requested_bridge or "").strip() or "br0"
    if _bridge_exists(bridge):
        return f"bridge={bridge},model=virtio", None
    if _ensure_libvirt_default_network():
        return "network=default,model=virtio", f"Bridge '{bridge}' not found; using libvirt default network"
    return "user,model=virtio", f"Bridge '{bridge}' not found and libvirt network 'default' is unavailable; using user-mode network"


def _resolve_osinfo_value(image: dict[str, Any]) -> str:
    family = str(image.get("os_family", "")).strip().lower()
    if family == "linux":
        return "linux2022"
    if family == "windows":
        return "win10"
    return "detect=on,require=off"


def _normalize_arch(value: str) -> str:
    raw = (value or "").strip().lower()
    if raw in {"x86_64", "amd64", "x64"}:
        return "amd64"
    if raw in {"aarch64", "arm64"}:
        return "arm64"
    return raw


def _resolve_image_architecture(image: dict[str, Any]) -> str:
    declared = _normalize_arch(str(image.get("architecture", "")).strip())
    if declared:
        return declared
    source_url = str(image.get("source_url", "")).strip().lower()
    name = str(image.get("name", "")).strip().lower()
    combined = f"{source_url} {name}"
    if any(token in combined for token in {"amd64", "x86_64"}):
        return "amd64"
    if any(token in combined for token in {"arm64", "aarch64"}):
        return "arm64"
    return ""


def _domain_state(domain_name: str) -> str:
    rc, stdout, _ = _run_sudo(["virsh", "domstate", domain_name], timeout_seconds=30)
    if rc != 0:
        return "unknown"
    return stdout.strip().lower()


def _domain_uuid(domain_name: str) -> str | None:
    rc, stdout, _ = _run_sudo(["virsh", "domuuid", domain_name], timeout_seconds=30)
    if rc != 0:
        return None
    value = stdout.strip()
    return value or None


def _domain_ip(domain_name: str) -> str | None:
    rc, stdout, _ = _run_sudo(["virsh", "domifaddr", domain_name, "--source", "agent"], timeout_seconds=30)
    if rc != 0:
        return None
    for line in stdout.splitlines():
        match = _IPV4_REGEX.search(line)
        if match:
            return match.group(0)
    return None


def _detect_capability() -> dict[str, Any]:
    if platform.system().lower() != "linux":
        return {
            "provider": "libvirt",
            "ready": False,
            "message": "libvirt VM support is Linux-only in v1",
            "missing_tools": [],
            "managed_paths": [str(IMAGE_ROOT), str(VM_ROOT)],
        }

    required_tools = ["sudo", "ip", "virsh", "virt-install", "qemu-img", "cloud-localds", "install", "mkdir", "rm"]
    missing_tools = [tool for tool in required_tools if shutil.which(tool) is None]
    if missing_tools:
        return {
            "provider": "libvirt",
            "ready": False,
            "message": "Missing required virtualization tools",
            "missing_tools": missing_tools,
            "managed_paths": [str(IMAGE_ROOT), str(VM_ROOT)],
        }

    rc, _stdout, stderr = _run_sudo(["virsh", "list", "--all"], timeout_seconds=30)
    if rc != 0:
        lower_err = (stderr or "").lower()
        message = "Unable to access libvirt with sudo -n"
        if "password is required" in lower_err or "no tty present" in lower_err or "not in the sudoers file" in lower_err:
            message = "sudo -n denied; configure NOPASSWD sudo or install prerequisites manually"
        return {
            "provider": "libvirt",
            "ready": False,
            "message": message,
            "missing_tools": [],
            "managed_paths": [str(IMAGE_ROOT), str(VM_ROOT)],
            "details": stderr,
        }

    return {
        "provider": "libvirt",
        "ready": True,
        "message": "libvirt ready",
        "missing_tools": [],
        "managed_paths": [str(IMAGE_ROOT), str(VM_ROOT)],
    }


def _refresh_capability_cache(value: dict[str, Any]) -> None:
    with _CAPABILITY_LOCK:
        _CAPABILITY_CACHE["checked_at"] = time.monotonic()
        _CAPABILITY_CACHE["value"] = dict(value)


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


def _install_linux_prerequisites(package_manager: str) -> tuple[bool, str, dict[str, Any]]:
    if package_manager == "apt":
        update_cmd = ["apt-get", "-o", "Acquire::Retries=3", "update"]
        install_cmd = [
            "apt-get",
            "install",
            "-y",
            "--no-install-recommends",
            "qemu-kvm",
            "libvirt-daemon-system",
            "libvirt-clients",
            "virtinst",
            "cloud-image-utils",
            "qemu-utils",
        ]
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
        # Service enable/start can fail harmlessly on some distros/images.
        _run_sudo(["systemctl", "enable", "--now", "libvirtd"], timeout_seconds=120)
        return True, "Installed VM prerequisites with apt", {}

    if package_manager == "dnf":
        cmd = [
            "dnf",
            "install",
            "-y",
            "qemu-kvm",
            "libvirt",
            "virt-install",
            "cloud-utils",
            "qemu-img",
        ]
        rc, out, err = _run_sudo(cmd, timeout_seconds=1800)
        if rc != 0:
            reason = _first_error_line(out, err)
            return False, f"dnf install failed: {reason}", {"stdout": out, "stderr": err}
        _run_sudo(["systemctl", "enable", "--now", "libvirtd"], timeout_seconds=120)
        return True, "Installed VM prerequisites with dnf", {}

    if package_manager == "yum":
        cmd = [
            "yum",
            "install",
            "-y",
            "qemu-kvm",
            "libvirt",
            "virt-install",
            "cloud-utils",
            "qemu-img",
        ]
        rc, out, err = _run_sudo(cmd, timeout_seconds=1800)
        if rc != 0:
            reason = _first_error_line(out, err)
            return False, f"yum install failed: {reason}", {"stdout": out, "stderr": err}
        _run_sudo(["systemctl", "enable", "--now", "libvirtd"], timeout_seconds=120)
        return True, "Installed VM prerequisites with yum", {}

    if package_manager == "pacman":
        cmd = [
            "pacman",
            "-Sy",
            "--noconfirm",
            "qemu-full",
            "libvirt",
            "virt-install",
            "cloud-image-utils",
        ]
        rc, out, err = _run_sudo(cmd, timeout_seconds=1800)
        if rc != 0:
            reason = _first_error_line(out, err)
            return False, f"pacman install failed: {reason}", {"stdout": out, "stderr": err}
        _run_sudo(["systemctl", "enable", "--now", "libvirtd"], timeout_seconds=120)
        return True, "Installed VM prerequisites with pacman", {}

    if package_manager == "zypper":
        cmd = [
            "zypper",
            "--non-interactive",
            "install",
            "qemu-kvm",
            "libvirt",
            "virt-install",
            "cloud-utils",
        ]
        rc, out, err = _run_sudo(cmd, timeout_seconds=1800)
        if rc != 0:
            reason = _first_error_line(out, err)
            return False, f"zypper install failed: {reason}", {"stdout": out, "stderr": err}
        _run_sudo(["systemctl", "enable", "--now", "libvirtd"], timeout_seconds=120)
        return True, "Installed VM prerequisites with zypper", {}

    return False, "Unsupported package manager", {"package_manager": package_manager}


def auto_install_vm_prerequisites(force: bool = False) -> dict[str, Any]:
    if platform.system().lower() != "linux":
        return {"attempted": False, "ready": False, "message": "Auto-install only runs on Linux"}

    with _AUTO_INSTALL_LOCK:
        now = time.monotonic()
        last_attempt = float(_AUTO_INSTALL_STATE.get("last_attempt", 0.0))
        cooldown_seconds = 600
        if not force and (now - last_attempt) < cooldown_seconds:
            current = get_vm_capability(max_age_seconds=0)
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
            return {"attempted": False, "ready": True, "message": "Prerequisites already installed", "capability": capability}

        missing_tools = capability.get("missing_tools")
        if not isinstance(missing_tools, list) or len(missing_tools) == 0:
            _refresh_capability_cache(capability)
            return {
                "attempted": False,
                "ready": False,
                "message": str(capability.get("message", "VM capability is not ready")),
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


def get_vm_capability(max_age_seconds: int = 60) -> dict[str, Any]:
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


def _download_cloud_image(image: dict[str, Any]) -> tuple[str, Path | None]:
    image_id = str(image.get("id", "")).strip()
    image_url = str(image.get("source_url", "")).strip()
    expected_sha = image.get("sha256")

    if not image_id:
        return "image.id is required", None
    if not image_url:
        return "image.source_url is required", None

    image_path = IMAGE_ROOT / f"{image_id}.qcow2"
    tmp_path = Path(tempfile.gettempdir()) / f"lattice-image-{image_id}.tmp"

    rc, _, err = _run_sudo(["mkdir", "-p", str(IMAGE_ROOT)], timeout_seconds=30)
    if rc != 0:
        return f"unable to prepare image directory: {err}", None

    if not image_path.exists():
        request.urlretrieve(image_url, tmp_path)
        if isinstance(expected_sha, str) and expected_sha.strip():
            digest = _compute_sha256(tmp_path)
            if digest != expected_sha.strip().lower():
                tmp_path.unlink(missing_ok=True)
                return "image checksum mismatch", None
        rc, _, err = _run_sudo(["install", "-m", "0644", str(tmp_path), str(image_path)], timeout_seconds=120)
        tmp_path.unlink(missing_ok=True)
        if rc != 0:
            return f"unable to install image: {err}", None

    return "", image_path


def _create_cloud_init_seed(vm_dir: Path, domain_name: str, username: str, password: str) -> tuple[str, Path | None]:
    user_data_path = Path(tempfile.gettempdir()) / f"{domain_name}-user-data.yaml"
    meta_data_path = Path(tempfile.gettempdir()) / f"{domain_name}-meta-data.yaml"
    seed_path = vm_dir / "seed.iso"
    user_data = (
        "#cloud-config\n"
        f"hostname: {domain_name}\n"
        "manage_etc_hosts: true\n"
        "users:\n"
        f"  - name: {username}\n"
        "    shell: /bin/bash\n"
        "    groups: sudo\n"
        "    sudo: ALL=(ALL) NOPASSWD:ALL\n"
        "    lock_passwd: false\n"
        f"    plain_text_passwd: '{password}'\n"
        "ssh_pwauth: true\n"
        "chpasswd:\n"
        "  expire: false\n"
        # Ensure a serial login prompt is available for virsh console access.
        "runcmd:\n"
        "  - [ sh, -c, \"systemctl enable --now serial-getty@ttyS0.service || true\" ]\n"
    )
    meta_data = f"instance-id: {domain_name}\nlocal-hostname: {domain_name}\n"
    user_data_path.write_text(user_data, encoding="utf-8")
    meta_data_path.write_text(meta_data, encoding="utf-8")
    try:
        rc, _, err = _run_sudo(["cloud-localds", str(seed_path), str(user_data_path), str(meta_data_path)], timeout_seconds=120)
        if rc != 0:
            return f"cloud-init seed creation failed: {err}", None
        return "", seed_path
    finally:
        user_data_path.unlink(missing_ok=True)
        meta_data_path.unlink(missing_ok=True)


def _create_vm(spec: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    vm_id = str(spec.get("vm_id", "")).strip()
    domain_name = str(spec.get("domain_name", "")).strip()
    image = spec.get("image")
    guest = spec.get("guest")
    if not vm_id or not domain_name or not isinstance(image, dict) or not isinstance(guest, dict):
        return "failed", "Invalid vm_create payload", {}

    vcpu = int(spec.get("vcpu", 1))
    memory_mb = int(spec.get("memory_mb", 1024))
    disk_gb = int(spec.get("disk_gb", 20))
    bridge = str(spec.get("bridge", "br0")).strip() or "br0"
    guest_username = str(guest.get("username", "")).strip()
    guest_password = str(guest.get("password", "")).strip()
    if not guest_username or not guest_password:
        return "failed", "Guest credentials are required", {}

    host_arch = _normalize_arch(platform.machine())
    image_arch = _resolve_image_architecture(image)
    if host_arch and image_arch and host_arch != image_arch:
        return (
            "failed",
            f"Image architecture '{image_arch}' is incompatible with node architecture '{host_arch}'. "
            f"Choose a '{host_arch}' cloud image.",
            {"host_architecture": host_arch, "image_architecture": image_arch},
        )

    vm_dir = VM_ROOT / vm_id
    disk_path = vm_dir / "disk.qcow2"
    rc, _, err = _run_sudo(["mkdir", "-p", str(vm_dir)], timeout_seconds=30)
    if rc != 0:
        return "failed", f"Unable to create VM directory: {err}", {}

    image_error, base_image_path = _download_cloud_image(image)
    if image_error or base_image_path is None:
        return "failed", image_error or "Failed to resolve VM image", {}

    rc, _, err = _run_sudo(
        ["qemu-img", "create", "-f", "qcow2", "-F", "qcow2", "-b", str(base_image_path), str(disk_path), f"{disk_gb}G"],
        timeout_seconds=240,
    )
    if rc != 0:
        return "failed", f"Disk provisioning failed: {err}", {}

    seed_error, seed_path = _create_cloud_init_seed(vm_dir, domain_name, guest_username, guest_password)
    if seed_error or seed_path is None:
        return "failed", seed_error or "Cloud-init seed error", {}

    network_arg, network_notice = _resolve_network_argument(bridge)
    if not network_arg:
        return "failed", network_notice or "Unable to resolve VM network target", {}

    osinfo_value = _resolve_osinfo_value(image)
    virt_cmd = [
        "virt-install",
        "--name",
        domain_name,
        "--memory",
        str(memory_mb),
        "--vcpus",
        str(vcpu),
        "--import",
        "--disk",
        f"path={disk_path},format=qcow2,bus=virtio",
        "--disk",
        f"path={seed_path},device=cdrom",
        "--network",
        network_arg,
        "--serial",
        "pty",
        "--console",
        "pty,target.type=serial",
        # Newer virt-install builds require explicit OS info to avoid unsafe defaults.
        "--osinfo",
        osinfo_value,
        "--graphics",
        "none",
        "--noautoconsole",
    ]
    rc, _stdout, err = _run_sudo(virt_cmd, timeout_seconds=300)
    if rc != 0:
        return "failed", f"virt-install failed: {err}", {}

    result_message = "VM created"
    if network_notice:
        result_message = f"{result_message} ({network_notice})"

    return "succeeded", result_message, {
        "vm_id": vm_id,
        "domain_name": domain_name,
        "domain_uuid": _domain_uuid(domain_name),
        "power_state": _domain_state(domain_name),
        "ip_address": _domain_ip(domain_name),
    }


def execute_vm_command(command: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    try:
        capability = get_vm_capability()
        if not capability.get("ready"):
            auto_install_result = auto_install_vm_prerequisites(force=False)
            capability = get_vm_capability(max_age_seconds=0)
            if not capability.get("ready"):
                return (
                    "failed",
                    str(capability.get("message", "VM capability is not ready")),
                    {"capability": capability, "auto_install": auto_install_result},
                )

        command_type = str(command.get("command_type", "")).strip()
        vm_id = str(command.get("vm_id", "")).strip()
        domain_name = str(command.get("domain_name", "")).strip()
        if command_type == "vm_create":
            spec = command.get("spec")
            if not isinstance(spec, dict):
                return "failed", "Missing create spec", {}
            return _create_vm(spec)

        if not domain_name:
            vm_spec = command.get("vm_spec")
            if isinstance(vm_spec, dict):
                domain_name = str(vm_spec.get("domain_name", "")).strip()
        if not domain_name:
            return "failed", "domain_name is required", {}

        if command_type == "vm_start":
            rc, _, err = _run_sudo(["virsh", "start", domain_name], timeout_seconds=60)
            if rc != 0 and "already active" not in err.lower():
                return "failed", f"Unable to start VM: {err}", {}
            return "succeeded", "VM started", {"vm_id": vm_id, "domain_name": domain_name, "power_state": _domain_state(domain_name), "domain_uuid": _domain_uuid(domain_name), "ip_address": _domain_ip(domain_name)}

        if command_type == "vm_stop":
            _run_sudo(["virsh", "shutdown", domain_name], timeout_seconds=30)
            for _ in range(12):
                state = _domain_state(domain_name)
                if "shut" in state or "off" in state or "stopped" in state:
                    return "succeeded", "VM stopped", {"vm_id": vm_id, "domain_name": domain_name, "power_state": "stopped", "domain_uuid": _domain_uuid(domain_name)}
                time.sleep(2)
            _run_sudo(["virsh", "destroy", domain_name], timeout_seconds=30)
            state = _domain_state(domain_name)
            if "running" in state:
                return "failed", "VM did not stop", {"vm_id": vm_id, "domain_name": domain_name, "power_state": state}
            return "succeeded", "VM stopped", {"vm_id": vm_id, "domain_name": domain_name, "power_state": state, "domain_uuid": _domain_uuid(domain_name)}

        if command_type == "vm_reboot":
            rc, _, err = _run_sudo(["virsh", "reboot", domain_name], timeout_seconds=60)
            if rc != 0:
                return "failed", f"Unable to reboot VM: {err}", {}
            return "succeeded", "VM rebooted", {"vm_id": vm_id, "domain_name": domain_name, "power_state": _domain_state(domain_name), "domain_uuid": _domain_uuid(domain_name), "ip_address": _domain_ip(domain_name)}

        if command_type == "vm_delete":
            _run_sudo(["virsh", "destroy", domain_name], timeout_seconds=30)
            rc, stdout, err = _run_sudo(["virsh", "undefine", domain_name, "--nvram", "--remove-all-storage"], timeout_seconds=120)
            if rc != 0 and not _looks_like_missing_domain_error(stdout, err):
                return "failed", f"Unable to delete VM: {err or stdout}", {}
            if vm_id:
                _run_sudo(["rm", "-rf", str(VM_ROOT / vm_id)], timeout_seconds=30)
            return "succeeded", "VM deleted", {"vm_id": vm_id, "domain_name": domain_name, "power_state": "deleted"}

        if command_type == "vm_sync":
            rc, stdout, err = _run_sudo(["virsh", "list", "--all", "--name"], timeout_seconds=30)
            if rc != 0:
                return "failed", f"Unable to sync VM state: {err}", {}
            names = [line.strip() for line in stdout.splitlines() if line.strip()]
            vms = [{"domain_name": name, "power_state": _domain_state(name), "domain_uuid": _domain_uuid(name)} for name in names]
            return "succeeded", "VM sync complete", {"vms": vms}

        return "failed", f"Unsupported vm command type: {command_type}", {}
    except Exception as exc:
        return "failed", f"VM command exception: {exc}", {}
