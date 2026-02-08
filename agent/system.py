import os
import platform
from typing import Any
from pathlib import Path

try:
    import psutil
except ImportError:  # pragma: no cover
    psutil = None


def get_system_info() -> dict[str, Any]:
    uname = platform.uname()
    return {
        "os": {
            "name": platform.system(),
            "release": platform.release(),
            "version": platform.version(),
        },
        "arch": {
            "machine": platform.machine(),
            "python_bits": platform.architecture()[0],
        },
        "hardware": {
            "node": uname.node,
            "processor": uname.processor or "unknown",
            "cpu_count": os.cpu_count(),
        },
    }


def get_runtime_metrics() -> dict[str, Any]:
    if psutil is None:
        return {}

    root_path = Path.cwd().anchor or "/"
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage(root_path)
    return {
        "cpu_percent": round(float(psutil.cpu_percent(interval=None)), 2),
        "memory_percent": round(float(memory.percent), 2),
        "memory_used_bytes": int(memory.used),
        "memory_total_bytes": int(memory.total),
        "storage_percent": round(float(disk.percent), 2),
        "storage_used_bytes": int(disk.used),
        "storage_total_bytes": int(disk.total),
    }


def log_system_info(logger) -> None:
    logger.info(f"Detected OS as {platform.system()} {platform.release()} v{platform.version()}")
