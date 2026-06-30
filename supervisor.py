from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import TextIO
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from miner_gpu_control.config import load_topology
from miner_gpu_control.models import GpuConfig, MinerConfig, SlotConfig, Topology, utc_now


SLOT_RESTART_BASE_DELAY_SECONDS = float(os.getenv("SLOT_RESTART_BASE_DELAY_SECONDS", "2"))
SLOT_RESTART_MAX_DELAY_SECONDS = float(os.getenv("SLOT_RESTART_MAX_DELAY_SECONDS", "30"))
SUPERVISOR_STATUS_PATH = Path(os.getenv("SUPERVISOR_STATUS_PATH", "run/supervisor_status.json"))
EXTERNAL_SLOT_CHECK_SECONDS = float(os.getenv("SUPERVISOR_EXTERNAL_SLOT_CHECK_SECONDS", "5"))
EXTERNAL_SLOT_PROBE_TIMEOUT_SECONDS = float(os.getenv("SUPERVISOR_EXTERNAL_SLOT_PROBE_TIMEOUT_SECONDS", "1.5"))
SUPERVISOR_TOPOLOGY_RELOAD_SECONDS = float(os.getenv("SUPERVISOR_TOPOLOGY_RELOAD_SECONDS", "5"))
TAKEOVER_EXTERNAL_SLOTS = os.getenv("SUPERVISOR_TAKEOVER_EXTERNAL_SLOTS", "1").lower() not in {
    "0",
    "false",
    "no",
}
TAKEOVER_MAX_ACTIVE_REQUESTS = int(os.getenv("SUPERVISOR_TAKEOVER_MAX_ACTIVE_REQUESTS", "0"))


@dataclass
class ManagedProcess:
    name: str
    log_name: str
    cmd: list[str]
    env: dict[str, str]
    restart: bool = False
    kind: str = "service"
    slot_id: str | None = None
    slot_index: int | None = None
    miner_id: str | None = None
    port: int | None = None
    url: str | None = None
    process: subprocess.Popen[bytes] | None = None
    log_file: TextIO | None = None
    restart_count: int = 0
    next_restart_at: float = 0.0
    external_active: bool = False
    external_last_seen: float = 0.0
    external_next_check_at: float = 0.0
    last_started_at: float = 0.0
    last_exit_code: int | None = None
    last_error: str | None = None


def _base_env(topology_path: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["TOPOLOGY_PATH"] = str(topology_path)
    env["PYTHONPATH"] = str(Path.cwd()) + os.pathsep + env.get("PYTHONPATH", "")
    return env


def _start_process(
    *,
    name: str,
    cmd: list[str],
    env: dict[str, str],
    log_dir: Path,
) -> tuple[subprocess.Popen[bytes], TextIO]:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = (log_dir / f"{name}.log").open("ab")
    process = subprocess.Popen(cmd, env=env, stdout=log_file, stderr=subprocess.STDOUT)
    return process, log_file


def _spawn_managed(managed: ManagedProcess, log_dir: Path) -> None:
    process, log_file = _start_process(
        name=managed.log_name,
        cmd=managed.cmd,
        env=managed.env,
        log_dir=log_dir,
    )
    managed.process = process
    managed.log_file = log_file
    managed.external_active = False
    managed.next_restart_at = 0.0
    managed.last_started_at = time.time()
    managed.last_error = None


def _close_log_file(managed: ManagedProcess) -> None:
    if managed.log_file is not None:
        managed.log_file.close()
        managed.log_file = None


def _restart_delay(restart_count: int) -> float:
    delay = SLOT_RESTART_BASE_DELAY_SECONDS * (2 ** max(0, restart_count - 1))
    return min(SLOT_RESTART_MAX_DELAY_SECONDS, delay)


def _iso_from_epoch(value: float) -> str | None:
    if value <= 0:
        return None
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def _topology_fingerprint(path: Path) -> tuple[int, int]:
    try:
        stat = path.stat()
    except OSError:
        return (0, 0)
    return (int(stat.st_mtime_ns), int(stat.st_size))


def _slot_probe_url(managed: ManagedProcess) -> str:
    return f"{str(managed.url or '').rstrip('/')}/slot"


def _slot_payload_matches(managed: ManagedProcess, data: dict[str, object]) -> bool:
    debug = data.get("_slot_debug") if isinstance(data.get("_slot_debug"), dict) else {}
    if data.get("slot_id") == managed.slot_id:
        return True
    if debug.get("slot_id") == managed.slot_id:
        return True

    gpu_uuids = data.get("gpu_uuids")
    hardware = data.get("hardware") if isinstance(data.get("hardware"), dict) else {}
    hardware_gpu_uuids = hardware.get("gpu_uuids")
    if isinstance(gpu_uuids, list) and managed.slot_id in gpu_uuids:
        return True
    if isinstance(hardware_gpu_uuids, list) and managed.slot_id in hardware_gpu_uuids:
        return True
    if isinstance(gpu_uuids, str) and managed.slot_id in {
        item.strip() for item in gpu_uuids.split(",") if item.strip()
    }:
        return True
    if isinstance(hardware_gpu_uuids, str) and managed.slot_id in {
        item.strip() for item in hardware_gpu_uuids.split(",") if item.strip()
    }:
        return True
    return False


def _slot_probe_payload(managed: ManagedProcess) -> tuple[bool, dict[str, object] | None, str | None]:
    if managed.kind != "slot" or not managed.url or not managed.slot_id:
        return False, None, "not a slot process"
    try:
        with urlopen(_slot_probe_url(managed), timeout=EXTERNAL_SLOT_PROBE_TIMEOUT_SECONDS) as response:
            raw = response.read(128 * 1024)
    except HTTPError as exc:
        if exc.code == 429 and managed.external_active:
            return True, None, "slot is busy"
        return False, None, f"HTTP {exc.code} from {_slot_probe_url(managed)}"
    except URLError as exc:
        return False, None, f"{exc.__class__.__name__}: {exc.reason}"
    except TimeoutError:
        return False, None, "slot probe timed out"
    except Exception as exc:
        return False, None, f"{exc.__class__.__name__}: {exc}"

    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        return False, None, f"slot probe returned non-json: {exc.__class__.__name__}"
    if not isinstance(data, dict):
        return False, None, "slot probe returned non-object json"
    if _slot_payload_matches(managed, data):
        return True, data, None
    return False, data, f"slot probe did not match slot_id={managed.slot_id}"


def _slot_health_matches(managed: ManagedProcess) -> tuple[bool, str | None]:
    healthy, _payload, error = _slot_probe_payload(managed)
    return healthy, error


def _listening_socket_inodes(port: int) -> set[str]:
    inodes: set[str] = set()
    for path in (Path("/proc/net/tcp"), Path("/proc/net/tcp6")):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()[1:]
        except OSError:
            continue
        for line in lines:
            parts = line.split()
            if len(parts) < 10 or parts[3] != "0A":
                continue
            try:
                local_port = int(parts[1].rsplit(":", 1)[1], 16)
            except (IndexError, ValueError):
                continue
            if local_port == port:
                inodes.add(parts[9])
    return inodes


def _listening_pids(port: int) -> list[int]:
    inodes = _listening_socket_inodes(port)
    if not inodes:
        return []
    pids: set[int] = set()
    for proc_dir in Path("/proc").glob("[0-9]*"):
        fd_dir = proc_dir / "fd"
        try:
            fd_paths = list(fd_dir.iterdir())
        except OSError:
            continue
        for fd_path in fd_paths:
            try:
                target = os.readlink(fd_path)
            except OSError:
                continue
            if target.startswith("socket:[") and target[8:-1] in inodes:
                try:
                    pids.add(int(proc_dir.name))
                except ValueError:
                    pass
                break
    return sorted(pids)


def _process_cmdline(pid: int) -> str:
    try:
        raw = (Path("/proc") / str(pid) / "cmdline").read_bytes()
    except OSError:
        return ""
    return " ".join(part.decode("utf-8", errors="replace") for part in raw.split(b"\0") if part)


def _process_environ(pid: int) -> dict[str, str]:
    try:
        raw = (Path("/proc") / str(pid) / "environ").read_bytes()
    except OSError:
        return {}
    values: dict[str, str] = {}
    for part in raw.split(b"\0"):
        if not part or b"=" not in part:
            continue
        key, value = part.split(b"=", 1)
        values[key.decode("utf-8", errors="replace")] = value.decode("utf-8", errors="replace")
    return values


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _terminate_pid(pid: int, *, timeout: float = 5.0) -> bool:
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return True
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _pid_alive(pid):
            return True
        time.sleep(0.1)
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return True
    return not _pid_alive(pid)


def _safe_to_takeover_slot_pid(managed: ManagedProcess, pid: int) -> tuple[bool, str | None]:
    if pid == os.getpid():
        return False, "refusing to terminate supervisor process"
    cmdline = _process_cmdline(pid)
    if "miner_gpu_control.slot_server:app" not in cmdline:
        return False, f"pid {pid} is not a miner slot server: {cmdline}"
    env = _process_environ(pid)
    slot_id = env.get("SLOT_ID")
    if slot_id and slot_id != managed.slot_id:
        return False, f"pid {pid} has SLOT_ID={slot_id}, expected {managed.slot_id}"
    miner_id = env.get("MINER_ID")
    if miner_id and managed.miner_id and miner_id != managed.miner_id:
        return False, f"pid {pid} has MINER_ID={miner_id}, expected {managed.miner_id}"
    return True, None


def _takeover_external_slot_if_safe(
    managed: ManagedProcess,
    log_dir: Path,
    *,
    reason: str,
) -> bool:
    if not TAKEOVER_EXTERNAL_SLOTS or managed.kind != "slot" or managed.port is None:
        return False
    healthy, payload, error = _slot_probe_payload(managed)
    if not healthy:
        managed.last_error = error
        return False
    if error == "slot is busy":
        managed.last_error = "external slot busy; takeover delayed"
        return False
    active_requests = 0
    if isinstance(payload, dict):
        try:
            active_requests = int(payload.get("active_requests") or 0)
        except (TypeError, ValueError):
            active_requests = 0
    if active_requests > TAKEOVER_MAX_ACTIVE_REQUESTS:
        managed.last_error = f"external slot busy with {active_requests} active request(s)"
        return False

    pids = _listening_pids(managed.port)
    takeover_pids: list[int] = []
    errors: list[str] = []
    for pid in pids:
        ok, pid_error = _safe_to_takeover_slot_pid(managed, pid)
        if ok:
            takeover_pids.append(pid)
        elif pid_error:
            errors.append(pid_error)
    if not takeover_pids:
        managed.last_error = (
            f"no safe external slot pid found on port {managed.port}"
            + (f": {'; '.join(errors)}" if errors else "")
        )
        return False

    for pid in takeover_pids:
        print(f"{managed.name} taking over external pid={pid} at {managed.url} ({reason})", flush=True)
        if not _terminate_pid(pid):
            managed.last_error = f"failed to terminate external pid {pid}"
            return False

    _spawn_managed(managed, log_dir)
    managed.restart_count = 0
    managed.external_active = False
    managed.external_next_check_at = 0.0
    print(f"{managed.name} now supervised pid={managed.process.pid if managed.process else '?'}", flush=True)
    return True


def _adopt_external_slot_if_healthy(managed: ManagedProcess, *, reason: str) -> bool:
    healthy, error = _slot_health_matches(managed)
    if not healthy:
        managed.last_error = error
        return False
    now = time.time()
    managed.external_active = True
    managed.external_last_seen = now
    managed.external_next_check_at = now + EXTERNAL_SLOT_CHECK_SECONDS
    managed.process = None
    managed.next_restart_at = 0.0
    managed.last_error = None
    print(f"{managed.name} using healthy external slot at {managed.url} ({reason})", flush=True)
    return True


def _process_status(managed: ManagedProcess) -> str:
    process = managed.process
    if process is not None and process.poll() is None:
        return "running"
    if managed.external_active:
        return "external"
    if managed.next_restart_at:
        return "restarting"
    if process is not None:
        return "exited"
    return "waiting" if managed.restart else "stopped"


def _status_summary(processes: list[ManagedProcess]) -> dict[str, int]:
    summary = {
        "total": len(processes),
        "running": 0,
        "external": 0,
        "restarting": 0,
        "down": 0,
        "slots": 0,
        "slot_external": 0,
    }
    for managed in processes:
        status = _process_status(managed)
        if status == "running":
            summary["running"] += 1
        elif status == "external":
            summary["external"] += 1
        elif status == "restarting":
            summary["restarting"] += 1
        else:
            summary["down"] += 1
        if managed.kind == "slot":
            summary["slots"] += 1
            if status == "external":
                summary["slot_external"] += 1
    return summary


def _write_status(
    processes: list[ManagedProcess],
    path: Path = SUPERVISOR_STATUS_PATH,
    *,
    topology_status: dict[str, object] | None = None,
) -> None:
    now = time.time()
    rows = []
    for managed in processes:
        process = managed.process
        pid = process.pid if process is not None else None
        pid_alive = bool(process is not None and process.poll() is None)
        rows.append(
            {
                "name": managed.name,
                "kind": managed.kind,
                "status": _process_status(managed),
                "managed_by_supervisor": pid_alive,
                "pid": pid,
                "pid_alive": pid_alive,
                "restart": managed.restart,
                "restart_count": managed.restart_count,
                "next_restart_at": managed.next_restart_at or None,
                "next_restart_seconds": (
                    max(0.0, managed.next_restart_at - now)
                    if managed.next_restart_at
                    else None
                ),
                "last_started_at": _iso_from_epoch(managed.last_started_at),
                "last_exit_code": managed.last_exit_code,
                "last_error": managed.last_error,
                "external_active": managed.external_active,
                "external_last_seen": _iso_from_epoch(managed.external_last_seen),
                "slot_id": managed.slot_id,
                "slot_index": managed.slot_index,
                "miner_id": managed.miner_id,
                "port": managed.port,
                "url": managed.url,
            }
        )
    summary = _status_summary(processes)
    status = "ok"
    if summary["down"] or summary["restarting"]:
        status = "warn"
    payload = {
        "status": status,
        "updated_at": utc_now(),
        "updated_at_epoch": now,
        "pid": os.getpid(),
        "summary": summary,
        "processes": rows,
    }
    if topology_status is not None:
        payload["topology"] = topology_status
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


def _router_command(topology: Topology) -> list[str]:
    return [
        sys.executable,
        "-m",
        "uvicorn",
        "miner_gpu_control.router:create_app",
        "--factory",
        "--host",
        topology.router.host,
        "--port",
        str(topology.router.port),
        "--log-level",
        "info",
    ]


def _slot_command(slot: SlotConfig) -> list[str]:
    return [
        sys.executable,
        "-m",
        "uvicorn",
        "miner_gpu_control.slot_server:app",
        "--host",
        slot.host,
        "--port",
        str(slot.port),
        "--log-level",
        "info",
    ]


def _miner_for_slot(topology: Topology, slot: SlotConfig) -> MinerConfig | None:
    for miner in topology.miners:
        if miner.miner_id == slot.miner_id:
            return miner
    return None


def _gpu_command(gpu: GpuConfig) -> list[str]:
    return [
        sys.executable,
        "-m",
        "uvicorn",
        "miner_gpu_control.gpu_worker:app",
        "--host",
        gpu.host,
        "--port",
        str(gpu.port),
        "--log-level",
        "info",
    ]


def _lease_watcher_command(slot_count: int) -> list[str]:
    return [
        sys.executable,
        "-m",
        "miner_gpu_control.lease_watcher",
        "--slots",
        str(slot_count),
        "--execute",
    ]


def _receipt_watcher_command() -> list[str]:
    return [
        sys.executable,
        "-m",
        "miner_gpu_control.receipt_watcher",
    ]


def _validator_allowlist_command() -> list[str]:
    return [
        sys.executable,
        "-m",
        "miner_gpu_control.validator_allowlist_watcher",
    ]


def _first_gpu_metadata(topology: Topology) -> dict[str, object]:
    if not topology.gpus:
        return {}
    gpu = sorted(topology.gpus, key=lambda item: (item.priority, item.gpu_id))[0]
    return dict(gpu.metadata or {})


def _topology_runtime_env(topology: Topology, base: dict[str, str] | None = None) -> dict[str, str]:
    source = dict(os.environ)
    if base is not None:
        source.update(base)
    metadata = _first_gpu_metadata(topology)
    model_id = str(metadata.get("model") or "Qwen/Qwen3.5-9B")
    quant = str(metadata.get("quant") or metadata.get("dtype") or "fp16")
    max_context_len = str(
        metadata.get("max_context_len") or metadata.get("max_model_len") or "262144"
    )
    gpu_name = str(
        metadata.get("gpu_name")
        or metadata.get("hardware")
        or metadata.get("name")
        or "NVIDIA GeForce RTX 5090"
    )
    vram_gb = str(
        metadata.get("vram_gb")
        or metadata.get("gpu_memory_gb")
        or metadata.get("memory_gb")
        or "32"
    )
    return {
        "SLOT_MODEL_ID": source.get("SLOT_MODEL_ID", model_id),
        "SLOT_QUANT": source.get("SLOT_QUANT", quant),
        "SLOT_MAX_CONTEXT_LEN": source.get("SLOT_MAX_CONTEXT_LEN", max_context_len),
        "SLOT_GPU_NAME": source.get("SLOT_GPU_NAME", gpu_name),
        "SLOT_VRAM_GB": source.get("SLOT_VRAM_GB", vram_gb),
        "LEASE_MODEL_ID": source.get("LEASE_MODEL_ID", model_id),
        "LEASE_QUANT": source.get("LEASE_QUANT", quant),
    }


def _slot_env(base: dict[str, str], topology: Topology, slot: SlotConfig) -> dict[str, str]:
    env = dict(base)
    miner = _miner_for_slot(topology, slot)
    hotkey = slot.hotkey or (miner.hotkey if miner is not None else None) or slot.miner_id
    coldkey = slot.coldkey or (miner.coldkey if miner is not None else None)
    env.update(
        {
            "ROUTER_URL": topology.router.url,
            "MINER_ID": slot.miner_id,
            "HOTKEY": hotkey,
            "SLOT_ID": slot.slot_id,
            "SLOT_INDEX": str(slot.slot_index),
            "SLOT_GPU_UUIDS": slot.slot_id,
            "VERATHOS_GPU_UUID_OVERRIDE": slot.slot_id,
            **_topology_runtime_env(topology, base),
        }
    )
    if coldkey:
        env["COLDKEY"] = coldkey
    return env


def _gpu_env(base: dict[str, str], gpu: GpuConfig) -> dict[str, str]:
    env = dict(base)
    env.update(
        {
            "GPU_ID": gpu.gpu_id,
            "GPU_NAME": str(gpu.metadata.get("name") or gpu.gpu_id),
            "GPU_MAX_JOBS": str(gpu.max_jobs),
            "GPU_DEVICE_INDEX": str(gpu.metadata.get("device_index", "0")),
        }
    )
    for key in (
        "OPENAI_COMPATIBLE_BASE_URL",
        "OPENAI_API_KEY",
        "OPENAI_MODEL",
        "GPU_MOCK_LATENCY_SECONDS",
    ):
        value = gpu.metadata.get(key.lower()) or gpu.metadata.get(key)
        if value is not None:
            env[key] = str(value)
    return env


def _should_start_local_gpu_worker(gpu: GpuConfig) -> bool:
    if str(gpu.metadata.get("provider", "")).lower() == "vast":
        return False
    return gpu.host in {"127.0.0.1", "localhost", "0.0.0.0"}


def _terminate(processes: list[ManagedProcess]) -> None:
    for managed in processes:
        process = managed.process
        if process is not None and process.poll() is None:
            print(f"stopping {managed.name} pid={process.pid}", flush=True)
            process.terminate()
    deadline = time.time() + 10
    for managed in processes:
        process = managed.process
        if process is None:
            continue
        remaining = max(0.0, deadline - time.time())
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            process.kill()
        _close_log_file(managed)


def _terminate_managed(managed: ManagedProcess, *, reason: str, timeout: float = 10.0) -> None:
    process = managed.process
    if process is None:
        managed.external_active = False
        return
    if process.poll() is None:
        print(f"stopping {managed.name} pid={process.pid} ({reason})", flush=True)
        process.terminate()
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=timeout)
    _close_log_file(managed)
    managed.process = None
    managed.external_active = False


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the local miner/GPU/router stack.")
    parser.add_argument("--topology", default="topology.json", help="Path to topology JSON.")
    parser.add_argument("--logs", default="logs", help="Directory for child process logs.")
    parser.add_argument("--no-router", action="store_true", help="Do not start the router.")
    parser.add_argument("--no-slots", action="store_true", help="Do not start miner slot servers.")
    parser.add_argument("--no-gpus", action="store_true", help="Do not start local GPU workers.")
    parser.add_argument("--no-validator-allowlist", action="store_true", help="Do not refresh the Verathos validator allowlist.")
    parser.add_argument("--no-lease-watcher", action="store_true", help="Do not start the Verathos lease renew watcher.")
    parser.add_argument("--no-receipt-watcher", action="store_true", help="Do not watch slot receipt integrity.")
    args = parser.parse_args(argv)

    topology_path = Path(args.topology).resolve()
    topology = load_topology(topology_path)
    topology_fingerprint = _topology_fingerprint(topology_path)
    topology_status: dict[str, object] = {
        "path": str(topology_path),
        "last_loaded_at": utc_now(),
        "last_error": None,
        "reload_count": 0,
        "fingerprint": list(topology_fingerprint),
    }
    log_dir = Path(args.logs)
    base = _base_env(topology_path)
    base.update(_topology_runtime_env(topology, base))
    processes: list[ManagedProcess] = []

    def add_process(
        *,
        name: str,
        log_name: str,
        cmd: list[str],
        env: dict[str, str],
        restart: bool = False,
        kind: str = "service",
        slot: SlotConfig | None = None,
    ) -> ManagedProcess:
        managed = ManagedProcess(
            name=name,
            log_name=log_name,
            cmd=cmd,
            env=env,
            restart=restart,
            kind=kind,
            slot_id=slot.slot_id if slot is not None else None,
            slot_index=slot.slot_index if slot is not None else None,
            miner_id=slot.miner_id if slot is not None else None,
            port=slot.port if slot is not None else None,
            url=slot.url if slot is not None else None,
        )
        if kind != "slot" or not (
            _takeover_external_slot_if_safe(managed, log_dir, reason="startup preflight")
            or _adopt_external_slot_if_healthy(managed, reason="startup preflight")
        ):
            _spawn_managed(managed, log_dir)
        processes.append(managed)
        _write_status(processes, topology_status=topology_status)
        return managed

    def _slot_env_subset(env: dict[str, str]) -> dict[str, str | None]:
        keys = (
            "ROUTER_URL",
            "MINER_ID",
            "HOTKEY",
            "COLDKEY",
            "SLOT_ID",
            "SLOT_INDEX",
            "SLOT_GPU_UUIDS",
            "VERATHOS_GPU_UUID_OVERRIDE",
            "SLOT_GPU_NAME",
            "SLOT_VRAM_GB",
            "SLOT_MAX_CONTEXT_LEN",
        )
        return {key: env.get(key) for key in keys}

    def _slot_process_needs_update(
        managed: ManagedProcess,
        slot: SlotConfig,
        current_topology: Topology,
    ) -> bool:
        return (
            managed.name != f"slot:{slot.slot_id}"
            or managed.log_name != f"slot-{slot.miner_id}-{slot.slot_index:02d}"
            or managed.cmd != _slot_command(slot)
            or _slot_env_subset(managed.env) != _slot_env_subset(_slot_env(base, current_topology, slot))
            or managed.slot_index != slot.slot_index
            or managed.miner_id != slot.miner_id
            or managed.port != slot.port
            or managed.url != slot.url
        )

    def _slot_active_requests(managed: ManagedProcess) -> int:
        healthy, payload, error = _slot_probe_payload(managed)
        if not healthy:
            managed.last_error = error
            return 0
        if not isinstance(payload, dict):
            return 0
        try:
            return int(payload.get("active_requests") or 0)
        except (TypeError, ValueError):
            return 0

    def _configure_slot_process(
        managed: ManagedProcess,
        slot: SlotConfig,
        current_topology: Topology,
    ) -> None:
        managed.name = f"slot:{slot.slot_id}"
        managed.log_name = f"slot-{slot.miner_id}-{slot.slot_index:02d}"
        managed.cmd = _slot_command(slot)
        managed.env = _slot_env(base, current_topology, slot)
        managed.kind = "slot"
        managed.restart = True
        managed.slot_id = slot.slot_id
        managed.slot_index = slot.slot_index
        managed.miner_id = slot.miner_id
        managed.port = slot.port
        managed.url = slot.url

    def reconcile_topology_if_changed(now: float) -> None:
        nonlocal topology, topology_fingerprint
        if SUPERVISOR_TOPOLOGY_RELOAD_SECONDS <= 0:
            return
        next_check = float(topology_status.get("next_check_at_epoch") or 0.0)
        if now < next_check:
            return
        topology_status["next_check_at_epoch"] = now + SUPERVISOR_TOPOLOGY_RELOAD_SECONDS

        current_fingerprint = _topology_fingerprint(topology_path)
        if current_fingerprint == topology_fingerprint:
            return

        try:
            next_topology = load_topology(topology_path)
        except Exception as exc:
            topology_status["last_error"] = f"{exc.__class__.__name__}: {exc}"
            return

        current_slots = {
            managed.slot_id: managed
            for managed in processes
            if managed.kind == "slot" and managed.slot_id
        }
        desired_slots = {slot.slot_id: slot for slot in next_topology.slots}
        added: list[str] = []
        updated: list[str] = []
        removed: list[str] = []
        pending: list[str] = []

        if not args.no_slots:
            for slot_id, slot in desired_slots.items():
                managed = current_slots.get(slot_id)
                if managed is None:
                    add_process(
                        name=f"slot:{slot.slot_id}",
                        log_name=f"slot-{slot.miner_id}-{slot.slot_index:02d}",
                        cmd=_slot_command(slot),
                        env=_slot_env(base, next_topology, slot),
                        restart=True,
                        kind="slot",
                        slot=slot,
                    )
                    added.append(slot_id)
                    continue
                if not _slot_process_needs_update(managed, slot, next_topology):
                    continue
                active_requests = _slot_active_requests(managed)
                if active_requests > 0:
                    managed.last_error = (
                        f"topology update pending; slot busy with {active_requests} active request(s)"
                    )
                    pending.append(slot_id)
                    continue
                _terminate_managed(managed, reason="topology update")
                _configure_slot_process(managed, slot, next_topology)
                _spawn_managed(managed, log_dir)
                updated.append(slot_id)

            for slot_id in sorted(set(current_slots) - set(desired_slots)):
                managed = current_slots[slot_id]
                active_requests = _slot_active_requests(managed)
                if active_requests > 0:
                    managed.last_error = (
                        f"topology removal pending; slot busy with {active_requests} active request(s)"
                    )
                    pending.append(slot_id)
                    continue
                _terminate_managed(managed, reason="topology removal")
                processes.remove(managed)
                removed.append(slot_id)

        topology = next_topology
        if not pending:
            topology_fingerprint = current_fingerprint
        topology_status["last_loaded_at"] = utc_now()
        topology_status["last_error"] = None if not pending else "some slot changes are pending idle slots"
        topology_status["reload_count"] = int(topology_status.get("reload_count") or 0) + 1
        topology_status["fingerprint"] = list(topology_fingerprint)
        topology_status["pending_fingerprint"] = list(current_fingerprint) if pending else None
        topology_status["last_reload"] = {
            "added_slots": added,
            "updated_slots": updated,
            "removed_slots": removed,
            "pending_slots": pending,
            "loaded_at": topology_status["last_loaded_at"],
        }
        print(
            "supervisor topology reload: "
            + json.dumps(topology_status["last_reload"], sort_keys=True),
            flush=True,
        )

    def handle_signal(signum: int, _frame: object) -> None:
        print(f"received signal {signum}", flush=True)
        _terminate(processes)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    if not args.no_router:
        add_process(
            name="router",
            log_name="router",
            cmd=_router_command(topology),
            env=base,
        )

    if not args.no_validator_allowlist:
        add_process(
            name="validator-allowlist",
            log_name="validator-allowlist",
            cmd=_validator_allowlist_command(),
            env=base,
        )

    if not args.no_gpus:
        for gpu in topology.gpus:
            if not _should_start_local_gpu_worker(gpu):
                print(f"skipping remote gpu worker {gpu.gpu_id} at {gpu.url}", flush=True)
                continue
            add_process(
                name=f"gpu:{gpu.gpu_id}",
                log_name=f"gpu-{gpu.gpu_id}",
                cmd=_gpu_command(gpu),
                env=_gpu_env(base, gpu),
            )

    if not args.no_slots:
        for slot in topology.slots:
            add_process(
                name=f"slot:{slot.slot_id}",
                log_name=f"slot-{slot.miner_id}-{slot.slot_index:02d}",
                cmd=_slot_command(slot),
                env=_slot_env(base, topology, slot),
                restart=True,
                kind="slot",
                slot=slot,
            )

    if not args.no_lease_watcher:
        add_process(
            name="lease-watcher",
            log_name="lease-watcher",
            cmd=_lease_watcher_command(len(topology.slots)),
            env=base,
        )

    if not args.no_receipt_watcher:
        add_process(
            name="receipt-watcher",
            log_name="receipt-watcher",
            cmd=_receipt_watcher_command(),
            env=base,
        )

    print(f"started {len(processes)} processes", flush=True)
    for managed in processes:
        process = managed.process
        pid = process.pid if process is not None else "?"
        print(f"{managed.name} pid={pid}", flush=True)
    _write_status(processes, topology_status=topology_status)

    try:
        while True:
            now = time.time()
            reconcile_topology_if_changed(now)
            for managed in processes:
                if managed.external_active and now >= managed.external_next_check_at:
                    if _takeover_external_slot_if_safe(managed, log_dir, reason="external monitor"):
                        continue
                    healthy, error = _slot_health_matches(managed)
                    if healthy:
                        managed.external_last_seen = now
                        managed.external_next_check_at = now + EXTERNAL_SLOT_CHECK_SECONDS
                        managed.last_error = None
                    else:
                        managed.external_active = False
                        managed.external_next_check_at = 0.0
                        managed.last_error = error
                        print(
                            f"{managed.name} external slot lost: {error}; starting managed process",
                            flush=True,
                        )
                        _spawn_managed(managed, log_dir)
                    continue

                process = managed.process
                if process is None:
                    if managed.restart and managed.next_restart_at and now >= managed.next_restart_at:
                        if managed.kind == "slot" and (
                            _takeover_external_slot_if_safe(
                                managed,
                                log_dir,
                                reason="restart preflight",
                            )
                            or _adopt_external_slot_if_healthy(
                                managed,
                                reason="restart preflight",
                            )
                        ):
                            continue
                        _spawn_managed(managed, log_dir)
                        restarted = managed.process
                        pid = restarted.pid if restarted is not None else "?"
                        print(f"{managed.name} restarted pid={pid}", flush=True)
                    continue

                code = process.poll()
                if code is None:
                    continue

                if managed.restart:
                    managed.last_exit_code = code
                    _close_log_file(managed)
                    managed.process = None
                    if managed.kind == "slot" and (
                        _takeover_external_slot_if_safe(
                            managed,
                            log_dir,
                            reason=f"managed process exited with code {code}",
                        )
                        or _adopt_external_slot_if_healthy(
                            managed,
                            reason=f"managed process exited with code {code}",
                        )
                    ):
                        continue
                    managed.restart_count += 1
                    delay = _restart_delay(managed.restart_count)
                    managed.next_restart_at = now + delay
                    print(
                        f"{managed.name} exited with code {code}; restarting in {delay:.1f}s",
                        flush=True,
                    )
                    continue

                print(f"{managed.name} exited with code {code}; stopping stack", flush=True)
                _terminate(processes)
                _write_status(processes, topology_status=topology_status)
                return code or 1
            _write_status(processes, topology_status=topology_status)
            time.sleep(1)
    finally:
        for managed in processes:
            _close_log_file(managed)
        _write_status(processes, topology_status=topology_status)


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
