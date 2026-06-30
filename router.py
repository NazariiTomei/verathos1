from __future__ import annotations

import asyncio
import contextlib
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.parse
import urllib.request
from collections import deque
from dataclasses import dataclass, field
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from pydantic import ValidationError

from miner_gpu_control.config import load_topology, topology_path_from_env
from miner_gpu_control.dashboard import DASHBOARD_HTML
from miner_gpu_control.models import (
    DispatchAttempt,
    DispatchRequest,
    DispatchResponse,
    GpuConfig,
    GpuExecuteRequest,
    GpuExecuteResponse,
    GpuRegisterRequest,
    SlotConfig,
    Topology,
    utc_now,
)
from miner_gpu_control.receipt_audit import (
    DEFAULT_RECEIPT_DIR,
    DEFAULT_STALE_SECONDS as RECEIPT_DEFAULT_STALE_SECONDS,
    audit_receipts,
)


PROBE_TIMEOUT_SECONDS = float(os.getenv("ROUTER_PROBE_TIMEOUT_SECONDS", "15.0"))
DASHBOARD_LOG_DIR = Path(os.getenv("DASHBOARD_LOG_DIR", "logs"))
VERATHOS_DASHBOARD_URL = os.getenv(
    "VERATHOS_DASHBOARD_URL",
    "https://verathos.ai/api/dashboard",
)
VERATHOS_DASHBOARD_CACHE_SECONDS = float(os.getenv("VERATHOS_DASHBOARD_CACHE_SECONDS", "20"))
BITTENSOR_RPC_URL = os.getenv("VERATHOS_RPC_URL", "https://lite.chain.opentensor.ai")
DASHBOARD_EPOCH_CACHE_SECONDS = float(os.getenv("DASHBOARD_EPOCH_CACHE_SECONDS", "12"))
DASHBOARD_EPOCH_BLOCKS = int(os.getenv("VERATHOS_EPOCH_BLOCKS", "360"))
DASHBOARD_BLOCK_SECONDS = float(os.getenv("VERATHOS_BLOCK_SECONDS", "12"))
LEASE_WATCHER_STATUS_PATH = Path(os.getenv("LEASE_STATUS_PATH", "run/lease_watcher_status.json"))
RECEIPT_INTEGRITY_STATUS_PATH = Path(os.getenv("RECEIPT_STATUS_PATH", "run/receipt_integrity_status.json"))
SUPERVISOR_STATUS_PATH = Path(os.getenv("SUPERVISOR_STATUS_PATH", "run/supervisor_status.json"))
RECEIPT_DIR = Path(os.getenv("RECEIPT_DIR", str(DEFAULT_RECEIPT_DIR)))
RECEIPT_STALE_SECONDS = int(os.getenv("RECEIPT_STALE_SECONDS", str(RECEIPT_DEFAULT_STALE_SECONDS)))
ROUTER_STALE_INFLIGHT_SECONDS = float(os.getenv("ROUTER_STALE_INFLIGHT_SECONDS", "45"))
ROUTER_GPU_FAILURE_COOLDOWN_SECONDS = float(os.getenv("ROUTER_GPU_FAILURE_COOLDOWN_SECONDS", "60"))
SUPERVISOR_STATUS_STALE_SECONDS = float(os.getenv("SUPERVISOR_STATUS_STALE_SECONDS", "15"))
ROUTER_TOPOLOGY_RELOAD_SECONDS = float(os.getenv("ROUTER_TOPOLOGY_RELOAD_SECONDS", "5"))
VAST_DASHBOARD_CACHE_SECONDS = float(os.getenv("VAST_DASHBOARD_CACHE_SECONDS", "30"))
VAST_DASHBOARD_TIMEOUT_SECONDS = float(os.getenv("VAST_DASHBOARD_TIMEOUT_SECONDS", "5"))


@dataclass
class SlotRuntime:
    config: SlotConfig
    healthy: bool = False
    last_seen: str | None = None
    last_error: str | None = None
    request_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    health_payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class GpuRuntime:
    config: GpuConfig
    healthy: bool = False
    active_jobs: int = 0
    max_jobs: int = 1
    router_inflight: int = 0
    router_inflight_since: float | None = None
    gpu_metrics: dict[str, Any] = field(default_factory=dict)
    last_seen: str | None = None
    last_error: str | None = None
    success_count: int = 0
    failure_count: int = 0
    worker_completed_jobs: int = 0
    worker_failed_jobs: int = 0
    unhealthy_until: float = 0.0

    @property
    def free_jobs(self) -> int:
        reserved_jobs = max(self.active_jobs, self.router_inflight)
        return max(0, self.max_jobs - reserved_jobs)


def _topology_fingerprint(path: Path) -> tuple[int, int]:
    try:
        stat = path.stat()
    except OSError:
        return (0, 0)
    return (int(stat.st_mtime_ns), int(stat.st_size))


class RouterState:
    def __init__(self, topology: Topology, *, topology_path: Path) -> None:
        self.topology = topology
        self.topology_path = topology_path
        self.topology_fingerprint = _topology_fingerprint(topology_path)
        self.topology_last_loaded_at = utc_now()
        self.topology_last_error: str | None = None
        self.topology_reload_count = 0
        self.slots: dict[str, SlotRuntime] = {
            slot.slot_id: SlotRuntime(config=slot)
            for slot in topology.slots
        }
        self.gpus: dict[str, GpuRuntime] = {
            gpu.gpu_id: GpuRuntime(config=gpu, max_jobs=gpu.max_jobs)
            for gpu in topology.gpus
        }
        self.lock = asyncio.Lock()
        self.condition = asyncio.Condition(self.lock)
        self.dispatch_queue: deque[str] = deque()
        self.verathos_dashboard: dict[str, Any] | None = None
        self.verathos_checked_monotonic = 0.0
        self.verathos_checked_at: str | None = None
        self.verathos_error: str | None = None
        self.chain_epoch: dict[str, Any] | None = None
        self.chain_epoch_checked_monotonic = 0.0
        self.chain_epoch_checked_at: str | None = None
        self.chain_epoch_error: str | None = None
        self.started_at = utc_now()

    async def reconcile_topology(
        self,
        topology: Topology,
        *,
        fingerprint: tuple[int, int] | None = None,
        source: str = "manual",
    ) -> dict[str, Any]:
        async with self.lock:
            old_slot_ids = set(self.slots)
            old_gpu_ids = set(self.gpus)
            new_slots = {slot.slot_id: slot for slot in topology.slots}
            new_gpus = {gpu.gpu_id: gpu for gpu in topology.gpus}

            added_slots: list[str] = []
            updated_slots: list[str] = []
            removed_slots: list[str] = []
            for slot_id, config in new_slots.items():
                existing = self.slots.get(slot_id)
                if existing is None:
                    self.slots[slot_id] = SlotRuntime(config=config)
                    added_slots.append(slot_id)
                else:
                    if existing.config.model_dump() != config.model_dump():
                        updated_slots.append(slot_id)
                    existing.config = config
            for slot_id in sorted(old_slot_ids - set(new_slots)):
                self.slots.pop(slot_id, None)
                removed_slots.append(slot_id)

            added_gpus: list[str] = []
            updated_gpus: list[str] = []
            removed_gpus: list[str] = []
            for gpu_id, config in new_gpus.items():
                existing = self.gpus.get(gpu_id)
                if existing is None:
                    self.gpus[gpu_id] = GpuRuntime(config=config, max_jobs=config.max_jobs)
                    added_gpus.append(gpu_id)
                else:
                    if existing.config.model_dump() != config.model_dump():
                        updated_gpus.append(gpu_id)
                    existing.config = config
                    existing.max_jobs = max(1, min(existing.max_jobs, config.max_jobs))
            for gpu_id in sorted(old_gpu_ids - set(new_gpus)):
                self.gpus.pop(gpu_id, None)
                removed_gpus.append(gpu_id)

            self.topology = topology
            if fingerprint is not None:
                self.topology_fingerprint = fingerprint
            self.topology_last_loaded_at = utc_now()
            self.topology_last_error = None
            self.topology_reload_count += 1
            self.condition.notify_all()
            return {
                "ok": True,
                "source": source,
                "loaded_at": self.topology_last_loaded_at,
                "reload_count": self.topology_reload_count,
                "slots": {
                    "added": added_slots,
                    "updated": updated_slots,
                    "removed": removed_slots,
                    "total": len(self.slots),
                },
                "gpus": {
                    "added": added_gpus,
                    "updated": updated_gpus,
                    "removed": removed_gpus,
                    "total": len(self.gpus),
                },
            }

    async def mark_topology_reload_error(self, error: str) -> None:
        async with self.lock:
            self.topology_last_error = error

    async def mark_slot_request(self, slot_id: str) -> None:
        async with self.lock:
            slot = self.slots.get(slot_id)
            if slot is not None:
                slot.healthy = True
                slot.last_seen = utc_now()
                slot.last_error = None
                slot.request_count += 1

    async def mark_slot_result(
        self,
        slot_id: str,
        *,
        success: bool,
        error: str | None = None,
    ) -> None:
        async with self.lock:
            slot = self.slots.get(slot_id)
            if slot is None:
                return
            slot.last_seen = utc_now()
            if success:
                slot.success_count += 1
                slot.last_error = None
            else:
                slot.failure_count += 1
                slot.last_error = error

    async def acquire_gpu(
        self,
        *,
        request_id: str,
        exclude: set[str],
        deadline: float,
    ) -> GpuRuntime | None:
        async with self.condition:
            if request_id not in self.dispatch_queue:
                self.dispatch_queue.append(request_id)
                self.condition.notify_all()

            while True:
                if self.dispatch_queue and self.dispatch_queue[0] == request_id:
                    candidates = [
                        gpu for gpu in self.gpus.values()
                        if gpu.config.gpu_id not in exclude and gpu.healthy and gpu.free_jobs > 0
                    ]
                    if candidates:
                        candidates.sort(key=_gpu_sort_key)
                        chosen = candidates[0]
                        if chosen.router_inflight == 0:
                            chosen.router_inflight_since = time.monotonic()
                        chosen.router_inflight += 1
                        self.dispatch_queue.popleft()
                        self.condition.notify_all()
                        return chosen

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._remove_queued_request(request_id)
                    self.condition.notify_all()
                    return None
                try:
                    await asyncio.wait_for(
                        self.condition.wait(),
                        timeout=min(remaining, 0.5),
                    )
                except asyncio.TimeoutError:
                    pass

    async def cancel_dispatch(self, request_id: str) -> None:
        async with self.condition:
            self._remove_queued_request(request_id)
            self.condition.notify_all()

    def _remove_queued_request(self, request_id: str) -> None:
        try:
            self.dispatch_queue.remove(request_id)
        except ValueError:
            pass

    async def release_gpu(
        self,
        gpu_id: str,
        *,
        success: bool,
        error: str | None = None,
        unhealthy: bool = False,
    ) -> None:
        async with self.lock:
            gpu = self.gpus.get(gpu_id)
            if gpu is None:
                return
            gpu.router_inflight = max(0, gpu.router_inflight - 1)
            if gpu.router_inflight == 0:
                gpu.router_inflight_since = None
            if success:
                gpu.success_count += 1
                gpu.last_error = None
                self.condition.notify_all()
                return
            gpu.failure_count += 1
            gpu.last_error = error
            if unhealthy:
                gpu.healthy = False
                gpu.unhealthy_until = time.monotonic() + ROUTER_GPU_FAILURE_COOLDOWN_SECONDS
            self.condition.notify_all()

    async def reservation_was_stale_released(
        self,
        gpu_id: str,
        *,
        acquired_at: float,
    ) -> bool:
        async with self.lock:
            gpu = self.gpus.get(gpu_id)
            if gpu is None:
                return False
            if ROUTER_STALE_INFLIGHT_SECONDS <= 0:
                return False
            if time.monotonic() - acquired_at < ROUTER_STALE_INFLIGHT_SECONDS:
                return False
            return (
                gpu.healthy
                and gpu.active_jobs == 0
                and gpu.router_inflight == 0
                and gpu.router_inflight_since is None
            )

    async def register_gpu(self, request: GpuRegisterRequest) -> None:
        config = GpuConfig(
            gpu_id=request.gpu_id,
            host=request.host,
            port=request.port,
            url=str(request.url).rstrip("/"),
            max_jobs=request.max_jobs,
            priority=request.priority,
            metadata=request.metadata,
        )
        async with self.lock:
            existing = self.gpus.get(config.gpu_id)
            self.gpus[config.gpu_id] = GpuRuntime(
                config=config,
                healthy=existing.healthy if existing else False,
                active_jobs=existing.active_jobs if existing else 0,
                max_jobs=config.max_jobs,
                router_inflight=existing.router_inflight if existing else 0,
                router_inflight_since=existing.router_inflight_since if existing else None,
                gpu_metrics=existing.gpu_metrics if existing else {},
                last_seen=existing.last_seen if existing else None,
                last_error=existing.last_error if existing else None,
                success_count=existing.success_count if existing else 0,
                failure_count=existing.failure_count if existing else 0,
                worker_completed_jobs=existing.worker_completed_jobs if existing else 0,
                worker_failed_jobs=existing.worker_failed_jobs if existing else 0,
                unhealthy_until=existing.unhealthy_until if existing else 0.0,
            )
            self.condition.notify_all()

    async def unregister_gpu(self, gpu_id: str) -> bool:
        async with self.lock:
            removed = self.gpus.pop(gpu_id, None) is not None
            if removed:
                self.condition.notify_all()
            return removed

    async def snapshot(self) -> dict[str, Any]:
        async with self.lock:
            return {
                "started_at": self.started_at,
                "checked_at": utc_now(),
                "router": self.topology.router.model_dump(),
                "topology": {
                    "path": str(self.topology_path),
                    "last_loaded_at": self.topology_last_loaded_at,
                    "last_error": self.topology_last_error,
                    "reload_count": self.topology_reload_count,
                    "fingerprint": list(self.topology_fingerprint),
                },
                "dispatch_queue": list(self.dispatch_queue),
                "dispatch_queue_length": len(self.dispatch_queue),
                "slots": {
                    slot_id: {
                        "config": slot.config.model_dump(),
                        "healthy": slot.healthy,
                        "last_seen": slot.last_seen,
                        "last_error": slot.last_error,
                        "request_count": slot.request_count,
                        "success_count": slot.success_count,
                        "failure_count": slot.failure_count,
                        "health_payload": slot.health_payload,
                    }
                    for slot_id, slot in sorted(self.slots.items())
                },
                "gpus": {
                    gpu_id: {
                        "config": gpu.config.model_dump(),
                        "healthy": gpu.healthy,
                        "active_jobs": gpu.active_jobs,
                        "max_jobs": gpu.max_jobs,
                        "router_inflight": gpu.router_inflight,
                        "router_inflight_since": gpu.router_inflight_since,
                        "free_jobs": gpu.free_jobs,
                        "gpu_metrics": gpu.gpu_metrics,
                        "last_seen": gpu.last_seen,
                        "last_error": gpu.last_error,
                        "success_count": gpu.success_count,
                        "failure_count": gpu.failure_count,
                        "worker_completed_jobs": gpu.worker_completed_jobs,
                        "worker_failed_jobs": gpu.worker_failed_jobs,
                        "unhealthy_until": gpu.unhealthy_until,
                    }
                    for gpu_id, gpu in sorted(self.gpus.items())
                },
            }

    async def verathos_status(
        self,
        client: httpx.AsyncClient,
    ) -> tuple[dict[str, Any] | None, str | None, str | None]:
        now = time.monotonic()
        async with self.lock:
            if (
                self.verathos_dashboard is not None
                and now - self.verathos_checked_monotonic < VERATHOS_DASHBOARD_CACHE_SECONDS
            ):
                return self.verathos_dashboard, self.verathos_error, self.verathos_checked_at

        dashboard: dict[str, Any] | None = None
        error: str | None = None
        checked_at = utc_now()
        try:
            response = await client.get(VERATHOS_DASHBOARD_URL)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise ValueError("Verathos dashboard response was not an object")
            dashboard = data
        except Exception as exc:
            error = f"{exc.__class__.__name__}: {exc}"

        async with self.lock:
            self.verathos_checked_monotonic = now
            self.verathos_checked_at = checked_at
            if dashboard is not None:
                self.verathos_dashboard = dashboard
                self.verathos_error = None
            else:
                self.verathos_error = error
            return self.verathos_dashboard, self.verathos_error, self.verathos_checked_at

    async def chain_epoch_status(
        self,
        client: httpx.AsyncClient,
    ) -> tuple[dict[str, Any] | None, str | None, str | None]:
        now = time.monotonic()
        async with self.lock:
            if (
                self.chain_epoch is not None
                and now - self.chain_epoch_checked_monotonic < DASHBOARD_EPOCH_CACHE_SECONDS
            ):
                return self.chain_epoch, self.chain_epoch_error, self.chain_epoch_checked_at

        epoch: dict[str, Any] | None = None
        error: str | None = None
        checked_at = utc_now()
        try:
            response = await client.post(
                BITTENSOR_RPC_URL,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "chain_getHeader",
                    "params": [],
                },
            )
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict) and data.get("error"):
                raise ValueError(json.dumps(data["error"], sort_keys=True))
            header = data.get("result") if isinstance(data, dict) else None
            number = header.get("number") if isinstance(header, dict) else None
            if isinstance(number, str):
                block_number = int(number, 16)
            elif isinstance(number, int):
                block_number = number
            else:
                raise ValueError("chain_getHeader response did not include a block number")

            epoch_blocks = max(1, DASHBOARD_EPOCH_BLOCKS)
            block_seconds = max(0.1, DASHBOARD_BLOCK_SECONDS)
            blocks_into_epoch = block_number % epoch_blocks
            current_epoch = block_number // epoch_blocks
            remaining_blocks = epoch_blocks - blocks_into_epoch
            next_epoch_block = block_number + remaining_blocks
            remaining_seconds = remaining_blocks * block_seconds
            estimated_next_epoch_at = time.strftime(
                "%Y-%m-%dT%H:%M:%SZ",
                time.gmtime(time.time() + remaining_seconds),
            )
            epoch = {
                "source": BITTENSOR_RPC_URL,
                "current_block": block_number,
                "current_epoch_number": current_epoch,
                "epoch_blocks": epoch_blocks,
                "block_seconds": block_seconds,
                "blocks_into_epoch": blocks_into_epoch,
                "remaining_blocks": remaining_blocks,
                "remaining_seconds": remaining_seconds,
                "next_epoch_number": current_epoch + 1,
                "next_epoch_block": next_epoch_block,
                "estimated_next_epoch_at": estimated_next_epoch_at,
            }
        except Exception as exc:
            error = f"{exc.__class__.__name__}: {exc}"

        async with self.lock:
            self.chain_epoch_checked_monotonic = now
            self.chain_epoch_checked_at = checked_at
            if epoch is not None:
                self.chain_epoch = epoch
                self.chain_epoch_error = None
            else:
                self.chain_epoch_error = error
            return self.chain_epoch, self.chain_epoch_error, self.chain_epoch_checked_at


def _gpu_sort_key(gpu: GpuRuntime) -> tuple[float, int, int, str]:
    used_ratio = max(gpu.active_jobs, gpu.router_inflight) / max(1, gpu.max_jobs)
    utilization = _metric_number(gpu.gpu_metrics, "utilization_gpu_percent", default=0)
    free_vram = _metric_number(gpu.gpu_metrics, "memory_free_mb", default=0)
    return (used_ratio, gpu.config.priority, utilization - int(free_vram / 1024), gpu.config.gpu_id)


def _sse(event: str, data: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def _merge_transport_timing(data: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    merged = dict(data)
    existing = merged.get("transport_timing")
    if not isinstance(existing, dict):
        existing = {}
    existing.update(updates)
    merged["transport_timing"] = existing
    return merged


def _parse_sse_block(block: str) -> tuple[str, dict[str, Any]]:
    event_type = ""
    data_lines: list[str] = []
    for line in block.splitlines():
        if line.startswith("event:"):
            event_type = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].strip())
    if not data_lines:
        return event_type, {}
    raw = "\n".join(data_lines)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = {"raw": raw}
    return event_type or str(data.get("event", "")), data


def _metric_number(metrics: dict[str, Any], key: str, *, default: int) -> int:
    value = metrics.get(key)
    if isinstance(value, (int, float)):
        return int(value)
    return default


def _should_mark_gpu_unhealthy(exc: Exception) -> bool:
    if _is_missing_verathos_backend(exc):
        return False
    if _is_gpu_request_validation_error(exc):
        return False
    if isinstance(exc, httpx.HTTPStatusError):
        return False
    return isinstance(
        exc,
        (
            httpx.ConnectError,
            httpx.ConnectTimeout,
            ValidationError,
            ValueError,
        ),
    )


def _http_status_error_text(exc: httpx.HTTPStatusError) -> str:
    try:
        data = exc.response.json()
    except Exception:
        data = exc.response.text
    return json.dumps(data, sort_keys=True) if isinstance(data, dict) else str(data)


def _is_missing_verathos_backend(exc: Exception) -> bool:
    if not isinstance(exc, httpx.HTTPStatusError):
        return False
    text = _http_status_error_text(exc)
    return (
        "Verathos requests require a GPU-side Verathos miner/proof backend" in text
        or "VERATHOS_MINER_BASE_URL is not configured" in text
    )


def _is_gpu_request_validation_error(exc: Exception) -> bool:
    if not isinstance(exc, httpx.HTTPStatusError):
        return False
    if exc.response.status_code not in {400, 422, 500}:
        return False
    text = _http_status_error_text(exc)
    return (
        "ValidationError" in text
        or "validation error for ChatRequestBody" in text
        or "validation error for InferenceRequestBody" in text
        or "validator_nonce" in text
        or "Missing validator auth headers" in text
        or "X-Validator-Hotkey" in text
    )


def _validate_gpu_execute_response(
    *,
    data: Any,
    expected: GpuExecuteRequest,
    expected_gpu_id: str,
) -> GpuExecuteResponse:
    response = GpuExecuteResponse.model_validate(data)
    if not response.ok:
        raise ValueError(f"GPU {expected_gpu_id} returned ok=false")
    if response.request_id != expected.request_id:
        raise ValueError(
            f"GPU {expected_gpu_id} returned request_id={response.request_id!r}, "
            f"expected {expected.request_id!r}"
        )
    if response.miner_id != expected.miner_id:
        raise ValueError(
            f"GPU {expected_gpu_id} returned miner_id={response.miner_id!r}, "
            f"expected {expected.miner_id!r}"
        )
    if response.slot_id != expected.slot_id:
        raise ValueError(
            f"GPU {expected_gpu_id} returned slot_id={response.slot_id!r}, "
            f"expected {expected.slot_id!r}"
        )
    if expected.slot_url and response.slot_url and response.slot_url.rstrip("/") != expected.slot_url.rstrip("/"):
        raise ValueError(
            f"GPU {expected_gpu_id} returned slot_url={response.slot_url!r}, "
            f"expected {expected.slot_url!r}"
        )
    if response.gpu_id != expected_gpu_id:
        raise ValueError(
            f"GPU endpoint {expected_gpu_id} returned gpu_id={response.gpu_id!r}"
        )
    return response


async def _post_gpu_execute_with_stale_watch(
    *,
    client: httpx.AsyncClient,
    state: RouterState,
    gpu: GpuRuntime,
    execute_request: GpuExecuteRequest,
    timeout: float,
    acquired_at: float,
) -> httpx.Response:
    task = asyncio.create_task(
        client.post(
            f"{gpu.config.url}/execute",
            json=execute_request.model_dump(),
            timeout=timeout,
        )
    )
    try:
        while True:
            done, _pending = await asyncio.wait({task}, timeout=1.0)
            if task in done:
                return task.result()
            if await state.reservation_was_stale_released(
                gpu.config.gpu_id,
                acquired_at=acquired_at,
            ):
                task.cancel()
                raise httpx.ReadTimeout(
                    f"GPU {gpu.config.gpu_id} execute response stalled after worker returned idle"
                )
    finally:
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


async def _open_gpu_execute_stream(
    *,
    client: httpx.AsyncClient,
    gpu: GpuRuntime,
    execute_request: GpuExecuteRequest,
    timeout: float,
) -> httpx.Response:
    request = client.build_request(
        "POST",
        f"{gpu.config.url}/execute/stream",
        json=execute_request.model_dump(),
        timeout=timeout,
    )
    return await client.send(request, stream=True)


async def _relay_gpu_sse_stream(
    *,
    response: httpx.Response,
    state: RouterState,
    gpu_id: str,
    slot_id: str,
    router_started: float,
    gpu_started: float,
    stream_opened: float,
):
    buffer = ""
    saw_done = False
    error: str | None = None
    released = False

    async def release_stream(success: bool, release_error: str | None = None) -> None:
        nonlocal released
        if released:
            return
        released = True
        await state.release_gpu(gpu_id, success=success, error=release_error)
        await state.mark_slot_result(slot_id, success=success, error=release_error)

    try:
        async for chunk in response.aiter_raw():
            if not chunk:
                continue
            buffer += chunk.decode("utf-8", errors="replace")
            while "\n\n" in buffer:
                block, buffer = buffer.split("\n\n", 1)
                event, data = _parse_sse_block(block)
                if event == "done":
                    saw_done = True
                    data = _merge_transport_timing(
                        data,
                        {
                            "router_mode": "streaming",
                            "router_selected_gpu_id": gpu_id,
                            "router_queue_wait_ms": round((gpu_started - router_started) * 1000.0, 3),
                            "router_to_gpu_stream_open_ms": round((stream_opened - gpu_started) * 1000.0, 3),
                            "router_stream_to_done_ms": round((time.monotonic() - stream_opened) * 1000.0, 3),
                            "router_total_to_done_ms": round((time.monotonic() - router_started) * 1000.0, 3),
                        },
                    )
                elif event == "error":
                    error = str(data.get("error", data))
                yield _sse(event or "message", data)

        if buffer.strip():
            event, data = _parse_sse_block(buffer)
            if event == "done":
                saw_done = True
                data = _merge_transport_timing(
                    data,
                    {
                        "router_mode": "streaming",
                        "router_selected_gpu_id": gpu_id,
                        "router_queue_wait_ms": round((gpu_started - router_started) * 1000.0, 3),
                        "router_to_gpu_stream_open_ms": round((stream_opened - gpu_started) * 1000.0, 3),
                        "router_stream_to_done_ms": round((time.monotonic() - stream_opened) * 1000.0, 3),
                        "router_total_to_done_ms": round((time.monotonic() - router_started) * 1000.0, 3),
                    },
                )
            elif event == "error":
                error = str(data.get("error", data))
            yield _sse(event or "message", data)

        if not saw_done and error is None:
            error = "GPU stream ended without Verathos done event"
            await release_stream(False, error)
            yield _sse("error", {"error": error})
    except asyncio.CancelledError:
        error = "client disconnected before GPU stream completed"
        raise
    except Exception as exc:
        error = f"{exc.__class__.__name__}: {exc}"
        await release_stream(False, error)
        yield _sse("error", {"error": error})
    finally:
        await response.aclose()
        await release_stream(saw_done, error)


async def _replay_buffered_verathos_result(result: dict[str, Any]):
    events = result.get("sse_events") or result.get("verathos_events")
    if isinstance(events, list):
        saw_done = False
        for item in events:
            if not isinstance(item, dict):
                continue
            event = item.get("event")
            data = item.get("data")
            if not isinstance(event, str) or not event:
                event = str(data.get("event", "message")) if isinstance(data, dict) else "message"
            if not isinstance(data, dict):
                data = {"raw": data}
            if event == "done":
                saw_done = True
            yield _sse(event, data)
        if saw_done:
            return

    text = result.get("text")
    if isinstance(text, str) and text:
        yield _sse("token", {"text": text})
    done = result.get("sse_done") or result.get("verathos_done") or result.get("done")
    if isinstance(done, dict):
        yield _sse("done", done)
        return
    yield _sse("error", {"error": "buffered GPU response did not include Verathos done event"})


async def _probe_slot(client: httpx.AsyncClient, state: RouterState, slot_id: str, slot: SlotRuntime) -> None:
    try:
        response = await client.get(f"{slot.config.url}/health")
        response.raise_for_status()
        data = response.json()
        ok = data.get("status") == "ok"
        try:
            debug_response = await client.get(f"{slot.config.url}/slot")
            debug_response.raise_for_status()
            debug = debug_response.json()
            if isinstance(debug, dict):
                data = {**data, "_slot_debug": debug}
        except Exception:
            pass
    except Exception as exc:
        async with state.lock:
            current = state.slots.get(slot_id)
            if current:
                current.healthy = False
                current.last_error = f"{exc.__class__.__name__}: {exc}"
        return
    async with state.lock:
        current = state.slots.get(slot_id)
        if current:
            current.healthy = ok
            current.last_seen = utc_now()
            current.last_error = None if ok else "health endpoint did not return ok"
            current.health_payload = data if isinstance(data, dict) else {}


async def _probe_gpu(client: httpx.AsyncClient, state: RouterState, gpu_id: str, gpu: GpuRuntime) -> None:
    try:
        response = await client.get(f"{gpu.config.url}/health")
        response.raise_for_status()
        data = response.json()
        ok = data.get("status") == "ok"
    except Exception as exc:
        async with state.lock:
            current = state.gpus.get(gpu_id)
            if current:
                busy = current.active_jobs > 0 or current.router_inflight > 0 or current.free_jobs == 0
                timeout_while_busy = busy and isinstance(exc, httpx.TimeoutException)
                current.last_error = f"{exc.__class__.__name__}: {exc}"
                if timeout_while_busy:
                    current.last_error += " (busy; keeping last health state)"
                else:
                    current.healthy = False
                state.condition.notify_all()
        return

    async with state.lock:
        current = state.gpus.get(gpu_id)
        if current:
            old_healthy = current.healthy
            old_free_jobs = current.free_jobs
            reported_active_jobs = int(data.get("active_jobs") or data.get("active_requests") or 0)
            reported_proof_pending = int(data.get("proof_pending") or 0)
            now = time.monotonic()
            cooling_down = now < current.unhealthy_until
            current.healthy = ok and not cooling_down
            current.active_jobs = reported_active_jobs
            if (
                ROUTER_STALE_INFLIGHT_SECONDS > 0
                and
                current.healthy
                and reported_active_jobs == 0
                and reported_proof_pending == 0
                and current.router_inflight > 0
            ):
                if current.router_inflight_since is None:
                    current.router_inflight_since = now
                elif now - current.router_inflight_since >= ROUTER_STALE_INFLIGHT_SECONDS:
                    current.router_inflight = 0
                    current.router_inflight_since = None
            reported_max_jobs = max(
                1,
                int(data.get("max_jobs") or data.get("max_requests") or current.config.max_jobs),
            )
            current.max_jobs = max(1, min(current.config.max_jobs, reported_max_jobs))
            worker_stats_available = "completed_jobs" in data or "failed_jobs" in data
            current.worker_completed_jobs = int(data.get("completed_jobs") or current.worker_completed_jobs)
            current.worker_failed_jobs = int(data.get("failed_jobs") or current.worker_failed_jobs)
            if isinstance(data.get("gpu_metrics"), dict):
                gpu_metrics = dict(data["gpu_metrics"])
                gpu_metrics.setdefault("source", "gpu_metrics")
                gpu_metrics["worker_stats_available"] = worker_stats_available
                for key in ("kv_pool_tokens", "kv_used_tokens", "kv_free_tokens", "kv_utilization_pct"):
                    if key in data and key not in gpu_metrics:
                        gpu_metrics[key] = data.get(key)
                current.gpu_metrics = gpu_metrics
            elif isinstance(data.get("hardware"), dict):
                hardware = data["hardware"]
                vram_gb = hardware.get("vram_gb") or 0
                try:
                    memory_total_mb = int(float(vram_gb) * 1024)
                except (TypeError, ValueError):
                    memory_total_mb = 0
                kv_utilization = data.get("kv_utilization_pct")
                current.gpu_metrics = {
                    "available": ok,
                    "source": "backend_health",
                    "nvidia_metrics_available": False,
                    "worker_stats_available": worker_stats_available,
                    "name": hardware.get("gpu_name") or current.config.metadata.get("name") or gpu_id,
                    "memory_total_mb": memory_total_mb,
                    "memory_used_mb": None,
                    "memory_free_mb": None,
                    "utilization_gpu_percent": None,
                    "temperature_gpu_c": None,
                    "kv_pool_tokens": data.get("kv_pool_tokens"),
                    "kv_used_tokens": data.get("kv_used_tokens"),
                    "kv_free_tokens": data.get("kv_free_tokens"),
                    "kv_utilization_pct": kv_utilization,
                    "compute_capability": hardware.get("compute_capability"),
                    "gpu_uuids": hardware.get("gpu_uuids"),
                }
            else:
                current.gpu_metrics = {
                    "source": "health",
                    "worker_stats_available": worker_stats_available,
                }
            current.last_seen = utc_now()
            if cooling_down:
                remaining = max(0.0, current.unhealthy_until - now)
                current.last_error = f"cooling down after execute failure ({remaining:.1f}s remaining)"
            else:
                current.last_error = None if ok else "health endpoint did not return ok"
            if (current.healthy and not old_healthy) or current.free_jobs > old_free_jobs:
                state.condition.notify_all()


async def _reload_topology_if_changed(state: RouterState) -> dict[str, Any] | None:
    fingerprint = _topology_fingerprint(state.topology_path)
    if fingerprint == state.topology_fingerprint:
        return None
    try:
        topology = load_topology(state.topology_path)
    except Exception as exc:
        error = f"{exc.__class__.__name__}: {exc}"
        await state.mark_topology_reload_error(error)
        return {
            "ok": False,
            "source": "watch",
            "error": error,
            "checked_at": utc_now(),
        }
    return await state.reconcile_topology(
        topology,
        fingerprint=fingerprint,
        source="watch",
    )


async def _monitor_loop(app: FastAPI) -> None:
    state: RouterState = app.state.router_state
    next_topology_check = 0.0
    async with httpx.AsyncClient(timeout=PROBE_TIMEOUT_SECONDS) as client:
        while True:
            now = time.monotonic()
            if ROUTER_TOPOLOGY_RELOAD_SECONDS > 0 and now >= next_topology_check:
                result = await _reload_topology_if_changed(state)
                if result is not None:
                    print(f"router topology reload: {json.dumps(result, sort_keys=True)}", flush=True)
                next_topology_check = now + ROUTER_TOPOLOGY_RELOAD_SECONDS
            async with state.lock:
                slots = list(state.slots.items())
                gpus = list(state.gpus.items())
                interval = state.topology.router.monitor_interval_seconds
            await asyncio.gather(
                *[_probe_slot(client, state, slot_id, slot) for slot_id, slot in slots],
                *[_probe_gpu(client, state, gpu_id, gpu) for gpu_id, gpu in gpus],
                return_exceptions=True,
            )
            await asyncio.sleep(interval)


def _clamp_score(value: float) -> int:
    return max(0, min(100, round(value)))


def _score_gpu(gpu: dict[str, Any]) -> int:
    if not gpu.get("healthy"):
        return 0
    score = 100.0
    if int(gpu.get("free_jobs") or 0) <= 0:
        score -= 15
    if gpu.get("last_error"):
        score -= 20
    failure_count = int(gpu.get("failure_count") or 0)
    score -= min(25, failure_count * 5)
    metrics = gpu.get("gpu_metrics") if isinstance(gpu.get("gpu_metrics"), dict) else {}
    if metrics.get("available") is False:
        score -= 10
    if int(metrics.get("utilization_gpu_percent") or 0) >= 95:
        score -= 10
    if int(metrics.get("temperature_gpu_c") or 0) >= 85:
        score -= 10
    return _clamp_score(score)


def _gpu_status(gpu: dict[str, Any]) -> str:
    if not gpu.get("healthy"):
        return "down"
    if int(gpu.get("free_jobs") or 0) <= 0:
        return "busy"
    if gpu.get("last_error"):
        return "degraded"
    return "ok"


def _detect_chain_processes() -> dict[str, bool]:
    try:
        result = subprocess.run(
            ["ps", "-eo", "args"],
            capture_output=True,
            text=True,
            timeout=1.5,
            check=True,
        )
    except Exception:
        return {}
    commands = result.stdout.splitlines()
    detected: dict[str, bool] = {}
    for command in commands:
        if "--wallet.hotkey" not in command:
            continue
        parts = command.split()
        for index, part in enumerate(parts):
            if part == "--wallet.hotkey" and index + 1 < len(parts):
                detected[parts[index + 1]] = True
            elif part.startswith("--wallet.hotkey="):
                detected[part.split("=", 1)[1]] = True
    return detected


def _wallet_hotkey_ss58(coldkey: str | None, hotkey: str | None) -> str | None:
    if not coldkey or not hotkey:
        return None
    keyfile = Path.home() / ".bittensor" / "wallets" / str(coldkey) / "hotkeys" / str(hotkey)
    try:
        data = json.loads(keyfile.read_text(encoding="utf-8"))
    except Exception:
        return None
    ss58 = data.get("ss58Address")
    return ss58 if isinstance(ss58, str) and ss58 else None


def _normalize_endpoint(url: Any) -> str:
    return str(url or "").rstrip("/")


def _public_slot_urls() -> dict[str, str]:
    manifest = Path("run/slot_https_registration.json")
    try:
        data = json.loads(manifest.read_text(encoding="utf-8"))
    except Exception:
        return {}
    urls: dict[str, str] = {}
    for slot in data.get("slots", []):
        if not isinstance(slot, dict):
            continue
        slot_id = slot.get("slot_id")
        https_url = slot.get("https_url")
        if isinstance(slot_id, str) and isinstance(https_url, str):
            urls[slot_id] = _normalize_endpoint(https_url)
    return urls


def _verathos_indexes(verathos: dict[str, Any] | None) -> dict[str, dict[str, list[dict[str, Any]]]]:
    indexes: dict[str, dict[str, list[dict[str, Any]]]] = {
        "by_ss58": {},
        "by_endpoint": {},
        "by_gpu_uuid": {},
    }
    if not isinstance(verathos, dict):
        return indexes
    for miner in verathos.get("miners", []):
        if not isinstance(miner, dict):
            continue
        ss58 = miner.get("ss58_address")
        if isinstance(ss58, str) and ss58:
            indexes["by_ss58"].setdefault(ss58, []).append(miner)
        endpoint = _normalize_endpoint(miner.get("endpoint"))
        if endpoint:
            indexes["by_endpoint"].setdefault(endpoint, []).append(miner)
        for gpu_uuid in miner.get("gpu_uuids") or []:
            if isinstance(gpu_uuid, str) and gpu_uuid:
                indexes["by_gpu_uuid"].setdefault(gpu_uuid, []).append(miner)
    return indexes


def _verathos_slot_matches(
    slot: dict[str, Any],
    indexes: dict[str, dict[str, list[dict[str, Any]]]],
    public_urls: dict[str, str],
) -> list[dict[str, Any]]:
    config = slot["config"]
    slot_id = config["slot_id"]
    candidates: list[dict[str, Any]] = []
    candidates.extend(indexes["by_gpu_uuid"].get(slot_id, []))
    candidates.extend(indexes["by_endpoint"].get(_normalize_endpoint(config.get("url")), []))
    public_url = public_urls.get(slot_id)
    if public_url:
        candidates.extend(indexes["by_endpoint"].get(public_url, []))

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[Any, Any, Any]] = set()
    for item in candidates:
        key = (item.get("uid"), item.get("endpoint"), item.get("model_index"))
        if key not in seen:
            seen.add(key)
            deduped.append(item)
    return deduped


def _verathos_score(rows: list[dict[str, Any]]) -> float | None:
    scores = [float(row.get("score") or 0.0) for row in rows if row.get("score") is not None]
    if not scores:
        return None
    return sum(scores)


def _verathos_requests(rows: list[dict[str, Any]]) -> int:
    total = 0
    for row in rows:
        stats = row.get("organic_stats") if isinstance(row.get("organic_stats"), dict) else {}
        total += int(stats.get("requests") or 0)
    return total


def _verathos_tokens(rows: list[dict[str, Any]]) -> int:
    total = 0
    for row in rows:
        stats = row.get("organic_stats") if isinstance(row.get("organic_stats"), dict) else {}
        total += int(stats.get("tokens") or 0)
    return total


def _slot_log_name(slot: dict[str, Any]) -> str:
    return f"slot-{slot['config']['miner_id']}-{int(slot['config']['slot_index']):02d}.log"


def _available_log_names(snapshot: dict[str, Any]) -> list[str]:
    requested = ["router.log", "supervisor.log"]
    requested.extend(
        _slot_log_name(slot)
        for slot in snapshot["slots"].values()
    )
    available = sorted(
        path.name
        for path in DASHBOARD_LOG_DIR.glob("*.log")
        if path.is_file()
    )
    ordered: list[str] = []
    for name in requested + available:
        if name in available and name not in ordered:
            ordered.append(name)
    return ordered


def _read_log_tail(name: str, *, lines: int) -> str:
    if Path(name).name != name:
        raise HTTPException(status_code=400, detail="log name must be a file name")
    path = DASHBOARD_LOG_DIR / name
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"unknown log file: {name}")
    max_bytes = min(262_144, max(8192, lines * 512))
    with path.open("rb") as fh:
        fh.seek(0, os.SEEK_END)
        size = fh.tell()
        fh.seek(max(0, size - max_bytes))
        data = fh.read()
    text = data.decode("utf-8", errors="replace")
    return "\n".join(text.splitlines()[-lines:])


async def _proxy_model_spec_from_gpu(
    state: RouterState,
    client: httpx.AsyncClient,
) -> Response:
    async with state.lock:
        candidates = sorted(
            state.gpus.values(),
            key=lambda gpu: (
                not gpu.healthy,
                gpu.config.priority,
                gpu.config.gpu_id,
            ),
        )

    last_error: str | None = None
    for gpu in candidates:
        try:
            response = await client.get(f"{gpu.config.url}/model_spec", timeout=30.0)
            if response.status_code in {404, 503}:
                detail = ""
                try:
                    payload = response.json()
                    if isinstance(payload, dict):
                        detail = str(payload.get("error") or payload.get("detail") or "")
                except Exception:
                    detail = response.text[:200]
                last_error = (
                    f"{gpu.config.gpu_id}: /model_spec unavailable"
                    + (f" ({detail})" if detail else "")
                )
                continue
            if response.status_code >= 500:
                last_error = (
                    f"{gpu.config.gpu_id}: /model_spec returned "
                    f"HTTP {response.status_code}"
                )
                continue
            content_type = response.headers.get("content-type", "application/json")
            return Response(
                content=response.content,
                status_code=response.status_code,
                media_type=content_type.split(";", 1)[0],
            )
        except Exception as exc:
            last_error = f"{gpu.config.gpu_id}: {exc.__class__.__name__}: {exc}"

    return JSONResponse(
        status_code=503,
        content={
            "error": "Model not loaded",
            "last_error": last_error,
        },
    )


def _pid_alive(pid: Any) -> bool:
    try:
        numeric_pid = int(pid)
    except (TypeError, ValueError):
        return False
    if numeric_pid <= 0:
        return False
    try:
        os.kill(numeric_pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _status_paths(primary: Path, pattern: str) -> list[Path]:
    paths: list[Path] = []
    if primary.exists():
        paths.append(primary)
    try:
        candidates = sorted(primary.parent.glob(pattern), key=lambda item: item.name)
    except OSError:
        candidates = []
    for path in candidates:
        if path.is_file() and path not in paths:
            paths.append(path)
    return paths


def _numeric_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _min_number(values: list[Any]) -> float | None:
    numbers = [
        number
        for number in (_numeric_or_none(value) for value in values)
        if number is not None
    ]
    return min(numbers) if numbers else None


def _max_text(values: list[Any]) -> str | None:
    texts = [str(value) for value in values if value]
    return max(texts) if texts else None


def _sum_int(rows: list[dict[str, Any]], key: str) -> int:
    total = 0
    for row in rows:
        try:
            total += int(row.get(key) or 0)
        except (TypeError, ValueError):
            continue
    return total


def _unique_texts(rows: list[dict[str, Any]], key: str) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for row in rows:
        value = row.get(key)
        if value is None:
            continue
        text = str(value)
        if text and text not in seen:
            values.append(text)
            seen.add(text)
    return values


def _single_or_list(values: list[str]) -> str | list[str] | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    return values


def _slot_sort_key(slot: dict[str, Any]) -> tuple[int, str]:
    try:
        slot_index = int(slot.get("slot_index"))
    except (TypeError, ValueError):
        slot_index = 999999
    return slot_index, str(slot.get("slot_id") or slot.get("endpoint") or "")


def _merged_status_slots(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    slots: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        hotkey = row.get("hotkey")
        for slot in row.get("slots", []):
            if not isinstance(slot, dict):
                continue
            key = str(slot.get("slot_id") or slot.get("endpoint") or slot.get("slot_index") or "")
            if key in seen:
                continue
            seen.add(key)
            item = dict(slot)
            if hotkey and "watcher_hotkey" not in item:
                item["watcher_hotkey"] = hotkey
            slots.append(item)
    return sorted(slots, key=_slot_sort_key)


def _timestamp_fields(rows: list[dict[str, Any]], key: str, iso_key: str) -> dict[str, Any]:
    best_row: dict[str, Any] | None = None
    best_value: float | None = None
    for row in rows:
        value = _numeric_or_none(row.get(key))
        if value is None:
            continue
        if best_value is None or value < best_value:
            best_value = value
            best_row = row
    if best_value is None:
        return {key: None, iso_key: None}
    output: dict[str, Any] = {key: int(best_value) if best_value.is_integer() else best_value}
    output[iso_key] = best_row.get(iso_key) if best_row else None
    return output


def _aggregate_status_value(rows: list[dict[str, Any]]) -> str:
    statuses = [str(row.get("status") or "unknown") for row in rows]
    if statuses and all(status == "ok" for status in statuses):
        return "ok"
    if statuses and all(status == "missing" for status in statuses):
        return "missing"
    if any(status == "error" for status in statuses):
        return "error"
    if any(status == "stale" for status in statuses):
        return "stale"
    return "warn"


def _aggregate_status_errors(rows: list[dict[str, Any]]) -> str | None:
    messages: list[str] = []
    for row in rows:
        status = row.get("status")
        last_error = row.get("last_error")
        if status == "ok" and not last_error:
            continue
        prefix = str(row.get("status_path") or row.get("hotkey") or "watcher")
        detail = str(last_error or status or "unknown issue")
        messages.append(f"{prefix}: {detail}")
    return "; ".join(messages) if messages else None


_vast_instances_cache: dict[str, Any] = {
    "expires_at": 0.0,
    "instances": {},
    "error": None,
}


def _vast_api_key() -> str | None:
    for name in ("VASTAI_API_KEY", "VAST_API_KEY"):
        value = os.getenv(name)
        if value:
            return value.strip()
    for path in (
        Path.home() / ".config" / "vastai" / "vast_api_key",
        Path.home() / ".vast_api_key",
    ):
        try:
            if path.exists():
                value = path.read_text(encoding="utf-8").strip()
                if value:
                    return value
        except OSError:
            continue
    return None


def _vast_instances_by_id() -> tuple[dict[str, dict[str, Any]], str | None]:
    now = time.monotonic()
    if now < float(_vast_instances_cache.get("expires_at") or 0):
        return (
            _vast_instances_cache.get("instances", {}),
            _vast_instances_cache.get("error"),
        )

    key = _vast_api_key()
    if not key:
        _vast_instances_cache.update(
            {
                "expires_at": now + VAST_DASHBOARD_CACHE_SECONDS,
                "instances": {},
                "error": "Vast API key not configured",
            }
        )
        return {}, "Vast API key not configured"

    url = "https://console.vast.ai/api/v0/instances?" + urllib.parse.urlencode({"owner": '"me"'})
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {key}"})
    try:
        with urllib.request.urlopen(request, timeout=VAST_DASHBOARD_TIMEOUT_SECONDS) as response:
            payload = json.load(response)
        rows = payload.get("instances") if isinstance(payload, dict) else payload
        if not isinstance(rows, list):
            raise ValueError("Vast instances response did not contain a list")
        instances = {
            str(row.get("id")): row
            for row in rows
            if isinstance(row, dict) and row.get("id") is not None
        }
        _vast_instances_cache.update(
            {
                "expires_at": now + VAST_DASHBOARD_CACHE_SECONDS,
                "instances": instances,
                "error": None,
            }
        )
        return instances, None
    except Exception as exc:
        error = f"{exc.__class__.__name__}: {exc}"
        _vast_instances_cache.update(
            {
                "expires_at": now + min(VAST_DASHBOARD_CACHE_SECONDS, 10.0),
                "instances": _vast_instances_cache.get("instances", {}),
                "error": error,
            }
        )
        return _vast_instances_cache.get("instances", {}), error


def _supervisor_status() -> dict[str, Any]:
    if not SUPERVISOR_STATUS_PATH.exists():
        return {
            "status": "missing",
            "running": False,
            "status_path": str(SUPERVISOR_STATUS_PATH),
            "last_error": "supervisor status file not found",
            "summary": {},
            "processes": [],
        }
    try:
        data = json.loads(SUPERVISOR_STATUS_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "status": "error",
            "running": False,
            "status_path": str(SUPERVISOR_STATUS_PATH),
            "last_error": f"{exc.__class__.__name__}: {exc}",
            "summary": {},
            "processes": [],
        }
    if not isinstance(data, dict):
        return {
            "status": "error",
            "running": False,
            "status_path": str(SUPERVISOR_STATUS_PATH),
            "last_error": "supervisor status is not an object",
            "summary": {},
            "processes": [],
        }
    data.setdefault("status_path", str(SUPERVISOR_STATUS_PATH))
    data["pid_alive"] = _pid_alive(data.get("pid"))
    data["running"] = bool(data["pid_alive"])
    try:
        updated_epoch = float(data.get("updated_at_epoch"))
    except (TypeError, ValueError):
        updated_epoch = 0.0
    stale = bool(updated_epoch and time.time() - updated_epoch > SUPERVISOR_STATUS_STALE_SECONDS)
    data["stale"] = stale
    if not data["pid_alive"]:
        data["status"] = "stale"
        data["running"] = False
        data["last_error"] = "supervisor pid is no longer running"
    elif stale and data.get("status") == "ok":
        data["status"] = "stale"
        data["last_error"] = "supervisor status is stale"
    data.setdefault("summary", {})
    data.setdefault("processes", [])
    return data


def _supervisor_processes_by_slot(supervisor_status: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(supervisor_status, dict):
        return {}
    rows: dict[str, dict[str, Any]] = {}
    for process in supervisor_status.get("processes", []):
        if not isinstance(process, dict):
            continue
        slot_id = process.get("slot_id")
        if process.get("kind") == "slot" and isinstance(slot_id, str) and slot_id:
            rows[slot_id] = process
    return rows


def _read_lease_watcher_status(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "status": "missing",
            "running": False,
            "status_path": str(path),
            "last_error": "lease watcher status file not found",
        }
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "status": "error",
            "running": False,
            "status_path": str(path),
            "last_error": f"{exc.__class__.__name__}: {exc}",
        }
    if not isinstance(data, dict):
        return {
            "status": "error",
            "running": False,
            "status_path": str(path),
            "last_error": "lease watcher status is not an object",
        }
    data.setdefault("status_path", str(path))
    data["pid_alive"] = _pid_alive(data.get("pid"))
    try:
        poll_seconds = int(data.get("poll_seconds") or 300)
    except (TypeError, ValueError):
        poll_seconds = 300
    try:
        next_check_at = float(data.get("next_check_at"))
    except (TypeError, ValueError):
        next_check_at = None
    stale = bool(next_check_at and time.time() > next_check_at + max(90, poll_seconds * 2))
    data["stale"] = stale
    if data.get("running") and not data["pid_alive"]:
        data["status"] = "stale"
        data["running"] = False
        data["last_error"] = "lease watcher pid is no longer running"
    elif stale and data.get("status") == "ok":
        data["status"] = "stale"
        data["last_error"] = "lease watcher status is stale"
    return data


def _aggregate_lease_watcher_status(watchers: list[dict[str, Any]]) -> dict[str, Any]:
    if len(watchers) == 1:
        return watchers[0]
    slots = _merged_status_slots(watchers)
    paths = [str(row.get("status_path")) for row in watchers if row.get("status_path")]
    hotkeys = _unique_texts(watchers, "hotkey")
    pids = [row.get("pid") for row in watchers if row.get("pid")]
    last_transactions: list[Any] = []
    warnings: list[str] = []
    for row in watchers:
        transactions = row.get("last_transactions")
        if isinstance(transactions, list):
            last_transactions.extend(transactions)
        row_warnings = row.get("warnings")
        if isinstance(row_warnings, list):
            warnings.extend(str(item) for item in row_warnings if item)

    min_remaining = _min_number([row.get("min_remaining_seconds") for row in watchers])
    aggregate: dict[str, Any] = {
        "status": _aggregate_status_value(watchers),
        "running": bool(watchers) and all(bool(row.get("running")) for row in watchers),
        "pid": None,
        "pids": pids,
        "pid_alive": bool(watchers) and all(bool(row.get("pid_alive")) for row in watchers),
        "status_path": ", ".join(paths),
        "status_paths": paths,
        "watchers": watchers,
        "hotkey": ", ".join(hotkeys),
        "hotkeys": hotkeys,
        "wallet": _single_or_list(_unique_texts(watchers, "wallet")),
        "uid": _single_or_list(_unique_texts(watchers, "uid")),
        "evm_address": _single_or_list(_unique_texts(watchers, "evm_address")),
        "evm_mirror_ss58": _single_or_list(_unique_texts(watchers, "evm_mirror_ss58")),
        "model_id": _single_or_list(_unique_texts(watchers, "model_id")),
        "quant": _single_or_list(_unique_texts(watchers, "quant")),
        "rpc_url": _single_or_list(_unique_texts(watchers, "rpc_url")),
        "netuid": _single_or_list(_unique_texts(watchers, "netuid")),
        "slot_count": len(slots),
        "matched_slots": _sum_int(watchers, "matched_slots"),
        "model_count": _sum_int(watchers, "model_count"),
        "due_count": _sum_int(watchers, "due_count"),
        "lease_seconds": _single_or_list(_unique_texts(watchers, "lease_seconds")),
        "renew_interval_seconds": _min_number([row.get("renew_interval_seconds") for row in watchers]),
        "renew_when_remaining_seconds": _min_number([row.get("renew_when_remaining_seconds") for row in watchers]),
        "poll_seconds": _min_number([row.get("poll_seconds") for row in watchers]),
        "checked_at": _max_text([row.get("checked_at") for row in watchers]),
        "min_remaining_seconds": int(min_remaining) if min_remaining is not None else None,
        "last_transactions": last_transactions,
        "warnings": warnings,
        "last_error": _aggregate_status_errors(watchers),
        "stale": any(bool(row.get("stale")) for row in watchers),
        "slots": slots,
    }
    aggregate.update(_timestamp_fields(watchers, "next_check_at", "next_check_at_iso"))
    aggregate.update(_timestamp_fields(watchers, "next_renew_at", "next_renew_at_iso"))
    if aggregate["last_error"] and aggregate["status"] == "ok":
        aggregate["status"] = "warn"
    return aggregate


def _lease_watcher_status() -> dict[str, Any]:
    paths = _status_paths(LEASE_WATCHER_STATUS_PATH, "lease_watcher*_status.json")
    if not paths:
        return _read_lease_watcher_status(LEASE_WATCHER_STATUS_PATH)
    return _aggregate_lease_watcher_status([_read_lease_watcher_status(path) for path in paths])


def _configured_receipt_slots(snapshot: dict[str, Any], public_urls: dict[str, str]) -> list[dict[str, Any]]:
    slots: list[dict[str, Any]] = []
    for slot in snapshot["slots"].values():
        config = slot.get("config") if isinstance(slot.get("config"), dict) else {}
        slot_id = config.get("slot_id")
        if not slot_id:
            continue
        if public_urls and slot_id not in public_urls:
            continue
        slots.append(
            {
                "slot_id": slot_id,
                "slot_index": config.get("slot_index"),
                "miner_id": config.get("miner_id"),
                "hotkey": config.get("hotkey"),
                "endpoint": public_urls.get(slot_id) or config.get("url"),
            }
        )
    return sorted(slots, key=lambda item: int(item.get("slot_index") or 0))


def _read_receipt_integrity_status_path(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if not path.exists():
        return None, "receipt watcher status file not found"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return None, f"{exc.__class__.__name__}: {exc}"
    if not isinstance(data, dict):
        return None, "receipt watcher status is not an object"
    data.setdefault("status_path", str(path))
    data["pid_alive"] = _pid_alive(data.get("pid"))
    try:
        poll_seconds = int(data.get("poll_seconds") or 30)
    except (TypeError, ValueError):
        poll_seconds = 30
    try:
        next_check_at = float(data.get("next_check_at"))
    except (TypeError, ValueError):
        next_check_at = None
    stale = bool(next_check_at and time.time() > next_check_at + max(60, poll_seconds * 2))
    data["stale"] = stale
    if data.get("running") and not data["pid_alive"]:
        return data, "receipt watcher pid is no longer running"
    if stale:
        return data, "receipt watcher status is stale"
    return data, None


def _merge_proof_failures_by_epoch(watchers: list[dict[str, Any]]) -> list[dict[str, int]]:
    merged: dict[int, int] = {}
    for row in watchers:
        failures = row.get("proof_failures_by_epoch")
        if isinstance(failures, list):
            for item in failures:
                if not isinstance(item, dict):
                    continue
                try:
                    epoch = int(item.get("epoch"))
                    count = int(item.get("proof_failures") or 0)
                except (TypeError, ValueError):
                    continue
                merged[epoch] = merged.get(epoch, 0) + count
            continue
        if not isinstance(failures, dict):
            continue
        for epoch, value in failures.items():
            try:
                epoch_number = int(epoch)
                count = int((value.get("proof_failures") if isinstance(value, dict) else value) or 0)
            except (TypeError, ValueError):
                continue
            merged[epoch_number] = merged.get(epoch_number, 0) + count
    return [
        {"epoch": epoch, "proof_failures": count}
        for epoch, count in sorted(merged.items())
        if count > 0
    ]


def _aggregate_receipt_integrity_status(watchers: list[dict[str, Any]]) -> dict[str, Any]:
    if len(watchers) == 1:
        return watchers[0]
    slots = _merged_status_slots(watchers)
    paths = [str(row.get("status_path")) for row in watchers if row.get("status_path")]
    pids = [row.get("pid") for row in watchers if row.get("pid")]
    latest_epochs: set[int] = set()
    warnings: list[str] = []
    cross_slot_duplicates: list[Any] = []
    for row in watchers:
        for epoch in row.get("latest_epochs", []):
            try:
                latest_epochs.add(int(epoch))
            except (TypeError, ValueError):
                continue
        row_warnings = row.get("warnings")
        if isinstance(row_warnings, list):
            warnings.extend(str(item) for item in row_warnings if item)
        duplicates = row.get("cross_slot_duplicates")
        if isinstance(duplicates, list):
            cross_slot_duplicates.extend(duplicates)

    aggregate: dict[str, Any] = {
        "status": _aggregate_status_value(watchers),
        "running": bool(watchers) and all(bool(row.get("running")) for row in watchers),
        "pid": None,
        "pids": pids,
        "pid_alive": bool(watchers) and all(bool(row.get("pid_alive")) for row in watchers),
        "status_path": ", ".join(paths),
        "status_paths": paths,
        "watchers": watchers,
        "checked_at": _max_text([row.get("checked_at") for row in watchers]),
        "lock_path": _single_or_list(_unique_texts(watchers, "lock_path")),
        "note": watchers[0].get("note"),
        "poll_seconds": _min_number([row.get("poll_seconds") for row in watchers]),
        "receipt_dir": _single_or_list(_unique_texts(watchers, "receipt_dir")),
        "source": "receipt_watcher",
        "stale_seconds": _single_or_list(_unique_texts(watchers, "stale_seconds")),
        "slot_count": len(slots),
        "ok_slots": _sum_int(watchers, "ok_slots"),
        "bad_slots": _sum_int(watchers, "bad_slots"),
        "warn_slots": _sum_int(watchers, "warn_slots"),
        "total_receipts": _sum_int(watchers, "total_receipts"),
        "duplicate_signatures": _sum_int(watchers, "duplicate_signatures"),
        "cross_slot_duplicate_signatures": _sum_int(watchers, "cross_slot_duplicate_signatures"),
        "cross_slot_duplicates": cross_slot_duplicates,
        "wrong_model_index": _sum_int(watchers, "wrong_model_index"),
        "proof_failures": _sum_int(watchers, "proof_failures"),
        "latest_proof_requests": _sum_int(watchers, "latest_proof_requests"),
        "latest_proof_passes": _sum_int(watchers, "latest_proof_passes"),
        "latest_proof_failures": _sum_int(watchers, "latest_proof_failures"),
        "historical_proof_failures": _sum_int(watchers, "historical_proof_failures"),
        "latest_epochs": sorted(latest_epochs),
        "proof_failures_by_epoch": _merge_proof_failures_by_epoch(watchers),
        "warnings": warnings,
        "last_error": _aggregate_status_errors(watchers),
        "stale": any(bool(row.get("stale")) for row in watchers),
        "slots": slots,
    }
    aggregate.update(_timestamp_fields(watchers, "next_check_at", "next_check_at_iso"))
    if aggregate["last_error"] and aggregate["status"] == "ok":
        aggregate["status"] = "warn"
    return aggregate


def _read_receipt_integrity_status() -> tuple[dict[str, Any] | None, str | None]:
    paths = _status_paths(RECEIPT_INTEGRITY_STATUS_PATH, "receipt_integrity*_status.json")
    if not paths:
        return _read_receipt_integrity_status_path(RECEIPT_INTEGRITY_STATUS_PATH)

    watchers: list[dict[str, Any]] = []
    issues: list[str] = []
    for path in paths:
        data, issue = _read_receipt_integrity_status_path(path)
        if data is not None:
            watchers.append(data)
        if issue:
            issues.append(f"{path}: {issue}")
    if not watchers:
        return None, "; ".join(issues) if issues else "receipt watcher status file not found"

    aggregate = _aggregate_receipt_integrity_status(watchers)
    if issues:
        aggregate.setdefault("warnings", []).extend(issues)
        if aggregate.get("status") == "ok":
            aggregate["status"] = "warn"
        return aggregate, "; ".join(issues)
    return aggregate, None


def _receipt_integrity_status(
    snapshot: dict[str, Any],
    *,
    public_urls: dict[str, str],
    lease_status: dict[str, Any],
) -> dict[str, Any]:
    data, issue = _read_receipt_integrity_status()
    if data is not None and issue is None:
        return data

    fallback = audit_receipts(
        slots=_configured_receipt_slots(snapshot, public_urls),
        receipt_dir=RECEIPT_DIR,
        lease_status=lease_status,
        stale_seconds=RECEIPT_STALE_SECONDS,
        source="router_fallback",
    )
    fallback["running"] = False
    fallback["status_path"] = str(RECEIPT_INTEGRITY_STATUS_PATH)
    fallback["watcher_issue"] = issue
    if data is not None:
        fallback["watcher_status"] = {
            "status": data.get("status"),
            "pid": data.get("pid"),
            "pid_alive": data.get("pid_alive"),
            "checked_at": data.get("checked_at"),
            "stale": data.get("stale"),
            "last_error": data.get("last_error"),
        }
    if issue:
        fallback.setdefault("warnings", []).append(issue)
        if fallback.get("status") == "ok":
            fallback["status"] = "warn"
    return fallback


def _miner_dashboard_rows(
    snapshot: dict[str, Any],
    *,
    verathos: dict[str, Any] | None,
    verathos_error: str | None,
    verathos_checked_at: str | None,
    indexes: dict[str, dict[str, list[dict[str, Any]]]],
    public_urls: dict[str, str],
    receipt_integrity: dict[str, Any] | None,
    supervisor_status: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    slots_by_miner: dict[str, list[dict[str, Any]]] = {}
    for slot in snapshot["slots"].values():
        slots_by_miner.setdefault(slot["config"]["miner_id"], []).append(slot)

    receipts_by_slot = _receipt_rows_by_slot(receipt_integrity)
    supervisor_by_slot = _supervisor_processes_by_slot(supervisor_status)
    chain_processes = _detect_chain_processes()
    rows: list[dict[str, Any]] = []
    for miner in snapshot["router"].get("miners", []):
        # The router config snapshot does not contain miners; keep this path for
        # future compatibility and build from topology below.
        _ = miner

    topology: Topology | None = None
    # This function is used from router request handlers where `snapshot` is
    # detached from the RouterState, so miner metadata is copied into each slot.
    miner_ids = sorted(slots_by_miner)
    for miner_id in miner_ids:
        miner_slots = sorted(slots_by_miner[miner_id], key=lambda item: item["config"]["slot_index"])
        first_slot = miner_slots[0]
        first_health = first_slot.get("health_payload") if isinstance(first_slot.get("health_payload"), dict) else {}
        first_debug = first_health.get("_slot_debug") if isinstance(first_health.get("_slot_debug"), dict) else {}
        hotkey = first_slot["config"].get("hotkey") or first_debug.get("hotkey") or miner_id
        coldkey = first_slot["config"].get("coldkey") or first_debug.get("coldkey")
        hotkey_ss58 = _wallet_hotkey_ss58(coldkey, hotkey)
        healthy_slots = sum(1 for slot in miner_slots if slot.get("healthy"))
        total_slots = len(miner_slots)
        chain_configured = bool(hotkey and coldkey)
        chain_running = bool(chain_processes.get(str(hotkey))) if chain_configured else None
        matched_rows: list[dict[str, Any]] = []
        if hotkey_ss58:
            matched_rows.extend(indexes["by_ss58"].get(hotkey_ss58, []))
        for slot in miner_slots:
            matched_rows.extend(_verathos_slot_matches(slot, indexes, public_urls))
        deduped_matches: list[dict[str, Any]] = []
        seen: set[tuple[Any, Any, Any]] = set()
        for item in matched_rows:
            key = (item.get("uid"), item.get("endpoint"), item.get("model_index"))
            if key not in seen:
                seen.add(key)
                deduped_matches.append(item)
        real_score = _verathos_score(deduped_matches)
        chain_registered = bool(deduped_matches)
        chain_healthy = any(bool(row.get("healthy")) for row in deduped_matches)
        if verathos_error:
            chain_status = "unknown"
        elif chain_healthy:
            chain_status = "active"
        elif chain_registered:
            chain_status = "registered"
        elif chain_configured:
            chain_status = "not registered"
        else:
            chain_status = "not set"
        if healthy_slots == total_slots:
            status = "ok"
        elif healthy_slots:
            status = "degraded"
        else:
            status = "down"
        count_rows = [
            _slot_request_counts(slot, receipts_by_slot.get(slot["config"]["slot_id"]))
            for slot in miner_slots
        ]
        supervisor_rows = [
            supervisor_by_slot.get(slot["config"]["slot_id"], {})
            for slot in miner_slots
        ]
        supervised_slots = sum(
            1
            for row in supervisor_rows
            if row.get("status") == "running" and row.get("managed_by_supervisor")
        )
        external_slots = sum(1 for row in supervisor_rows if row.get("status") == "external")
        restarting_slots = sum(1 for row in supervisor_rows if row.get("status") == "restarting")
        process_issue_slots = sum(
            1
            for row in supervisor_rows
            if row and row.get("status") not in {"running", "external"}
        )
        rows.append(
            {
                "miner_id": miner_id,
                "hotkey": hotkey,
                "hotkey_ss58": hotkey_ss58,
                "coldkey": coldkey,
                "chain_configured": chain_configured,
                "chain_running": chain_running,
                "chain_registered": chain_registered,
                "chain_healthy": chain_healthy,
                "chain_status": chain_status,
                "chain_uid": deduped_matches[0].get("uid") if deduped_matches else None,
                "status": status,
                "score": real_score,
                "score_source": "verathos" if real_score is not None else ("error" if verathos_error else "unlisted"),
                "verathos_rows": len(deduped_matches),
                "verathos_checked_at": verathos_checked_at,
                "verathos_error": verathos_error,
                "verathos_requests": _verathos_requests(deduped_matches),
                "verathos_tokens": _verathos_tokens(deduped_matches),
                "healthy_slots": healthy_slots,
                "total_slots": total_slots,
                "ports": [slot["config"]["port"] for slot in miner_slots],
                "request_count": sum(int(row.get("request_count") or 0) for row in count_rows),
                "success_count": sum(int(row.get("success_count") or 0) for row in count_rows),
                "failure_count": sum(int(row.get("failure_count") or 0) for row in count_rows),
                "count_source": "receipts" if any(row.get("count_source") == "receipts" for row in count_rows) else "live",
                "live_request_count": sum(int(row.get("live_request_count") or 0) for row in count_rows),
                "live_success_count": sum(int(row.get("live_success_count") or 0) for row in count_rows),
                "live_failure_count": sum(int(row.get("live_failure_count") or 0) for row in count_rows),
                "supervised_slots": supervised_slots,
                "external_slots": external_slots,
                "restarting_slots": restarting_slots,
                "process_issue_slots": process_issue_slots,
            }
        )
    return rows


def _receipt_rows_by_slot(receipt_integrity: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    if not isinstance(receipt_integrity, dict):
        return rows
    for row in receipt_integrity.get("slots", []):
        if not isinstance(row, dict):
            continue
        slot_id = row.get("slot_id")
        if isinstance(slot_id, str) and slot_id:
            rows[slot_id] = row
    return rows


def _slot_request_counts(slot: dict[str, Any], receipt_row: dict[str, Any] | None) -> dict[str, Any]:
    live_request_count = int(slot.get("request_count") or 0)
    live_success_count = int(slot.get("success_count") or 0)
    live_failure_count = int(slot.get("failure_count") or 0)
    if not receipt_row:
        return {
            "request_count": live_request_count,
            "success_count": live_success_count,
            "failure_count": live_failure_count,
            "count_source": "live",
            "live_request_count": live_request_count,
            "live_success_count": live_success_count,
            "live_failure_count": live_failure_count,
        }

    total_receipts = int(receipt_row.get("total_receipts") or 0)
    integrity_failures = (
        int(receipt_row.get("proof_failures") or 0)
        + int(receipt_row.get("wrong_model_index") or 0)
        + int(receipt_row.get("duplicate_signatures") or 0)
        + int(receipt_row.get("cross_slot_duplicate_signatures") or 0)
    )
    failure_count = min(total_receipts, max(0, integrity_failures))
    return {
        "request_count": total_receipts,
        "success_count": max(0, total_receipts - failure_count),
        "failure_count": failure_count,
        "count_source": "receipts",
        "live_request_count": live_request_count,
        "live_success_count": live_success_count,
        "live_failure_count": live_failure_count,
        "receipt_total": total_receipts,
        "receipt_success_count": max(0, total_receipts - failure_count),
        "receipt_failure_count": failure_count,
        "latest_epoch": receipt_row.get("latest_epoch"),
        "latest_epoch_receipts": int(receipt_row.get("latest_epoch_receipts") or 0),
        "latest_proof_failures": int(receipt_row.get("latest_proof_failures") or 0),
        "historical_proof_failures": int(receipt_row.get("historical_proof_failures") or 0),
    }


def _slot_dashboard_rows(
    snapshot: dict[str, Any],
    *,
    indexes: dict[str, dict[str, list[dict[str, Any]]]],
    public_urls: dict[str, str],
    receipt_integrity: dict[str, Any] | None,
    supervisor_status: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    rows = []
    receipts_by_slot = _receipt_rows_by_slot(receipt_integrity)
    supervisor_by_slot = _supervisor_processes_by_slot(supervisor_status)
    for slot in sorted(snapshot["slots"].values(), key=lambda item: (item["config"]["miner_id"], item["config"]["slot_index"])):
        config = slot["config"]
        health_payload = slot.get("health_payload") if isinstance(slot.get("health_payload"), dict) else {}
        slot_debug = health_payload.get("_slot_debug") if isinstance(health_payload.get("_slot_debug"), dict) else {}
        verathos_rows = _verathos_slot_matches(slot, indexes, public_urls)
        verathos_score = _verathos_score(verathos_rows)
        counts = _slot_request_counts(slot, receipts_by_slot.get(config["slot_id"]))
        supervisor_process = supervisor_by_slot.get(config["slot_id"], {})
        rows.append(
            {
                "miner_id": config["miner_id"],
                "hotkey": config.get("hotkey") or slot_debug.get("hotkey") or config["miner_id"],
                "coldkey": config.get("coldkey") or slot_debug.get("coldkey"),
                "slot_id": config["slot_id"],
                "slot_index": config["slot_index"],
                "host": config["host"],
                "port": config["port"],
                "url": config["url"],
                "healthy": bool(slot.get("healthy")),
                "last_seen": slot.get("last_seen"),
                "last_error": slot.get("last_error"),
                **counts,
                "public_url": public_urls.get(config["slot_id"]),
                "verathos_score": verathos_score,
                "verathos_healthy": any(bool(row.get("healthy")) for row in verathos_rows) if verathos_rows else None,
                "verathos_rows": len(verathos_rows),
                "verathos_requests": _verathos_requests(verathos_rows),
                "verathos_tokens": _verathos_tokens(verathos_rows),
                "verathos_consecutive_failures": max(
                    [int(row.get("consecutive_failures") or 0) for row in verathos_rows],
                    default=0,
                ),
                "process_status": supervisor_process.get("status") or "unknown",
                "process_pid": supervisor_process.get("pid"),
                "process_managed_by_supervisor": bool(supervisor_process.get("managed_by_supervisor")),
                "process_external_active": bool(supervisor_process.get("external_active")),
                "process_restart_count": int(supervisor_process.get("restart_count") or 0),
                "process_next_restart_seconds": supervisor_process.get("next_restart_seconds"),
                "process_last_error": supervisor_process.get("last_error"),
                "process_external_last_seen": supervisor_process.get("external_last_seen"),
            }
        )
    return rows


def _gpu_dashboard_rows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    vast_instances, vast_error = _vast_instances_by_id()
    for gpu_id, gpu in sorted(snapshot["gpus"].items()):
        config = gpu["config"]
        metadata = config.get("metadata", {}) if isinstance(config.get("metadata"), dict) else {}
        instance_id = metadata.get("instance_id") or metadata.get("vast_instance_id")
        vast_instance = vast_instances.get(str(instance_id)) if instance_id is not None else None
        metrics = gpu.get("gpu_metrics") if isinstance(gpu.get("gpu_metrics"), dict) else {}
        total_value = _numeric_or_none(metrics.get("memory_total_mb"))
        used_value = _numeric_or_none(metrics.get("memory_used_mb"))
        free_value = _numeric_or_none(metrics.get("memory_free_mb"))
        utilization_value = _numeric_or_none(metrics.get("utilization_gpu_percent"))
        temperature_value = _numeric_or_none(metrics.get("temperature_gpu_c"))
        metrics_source = metrics.get("source")
        nvidia_metrics_available = metrics.get("nvidia_metrics_available")
        if isinstance(vast_instance, dict):
            vast_total = _numeric_or_none(vast_instance.get("gpu_ram") or vast_instance.get("gpu_totalram"))
            vast_used_percent = _numeric_or_none(vast_instance.get("vmem_usage"))
            if vast_total is not None:
                total_value = vast_total
                if vast_used_percent is not None:
                    used_value = vast_total * max(0.0, min(100.0, vast_used_percent)) / 100.0
                    free_value = max(0.0, vast_total - used_value)
            vast_utilization = _numeric_or_none(vast_instance.get("gpu_util"))
            vast_temperature = _numeric_or_none(vast_instance.get("gpu_temp"))
            if vast_utilization is not None:
                utilization_value = vast_utilization
            if vast_temperature is not None:
                temperature_value = vast_temperature
            metrics_source = "vast_api"
            nvidia_metrics_available = True
        total_mb = int(total_value) if total_value is not None else None
        used_mb = int(used_value) if used_value is not None else None
        free_mb = int(free_value) if free_value is not None else None
        cooldown_seconds = max(0.0, float(gpu.get("unhealthy_until") or 0.0) - time.monotonic())
        rows.append(
            {
                "gpu_id": gpu_id,
                "status": _gpu_status(gpu),
                "score": _score_gpu(gpu),
                "healthy": bool(gpu.get("healthy")),
                "active_jobs": int(gpu.get("active_jobs") or 0),
                "max_jobs": int(gpu.get("max_jobs") or config.get("max_jobs") or 1),
                "router_inflight": int(gpu.get("router_inflight") or 0),
                "free_jobs": int(gpu.get("free_jobs") or 0),
                "gpu_name": metrics.get("name") or metadata.get("name") or gpu_id,
                "model": metadata.get("model"),
                "provider": metadata.get("provider"),
                "instance_id": instance_id,
                "memory_total_mb": total_mb,
                "memory_used_mb": used_mb,
                "memory_free_mb": free_mb,
                "memory_used_percent": (used_mb / total_mb * 100.0) if used_mb is not None and total_mb else None,
                "utilization_gpu_percent": round(utilization_value, 1) if utilization_value is not None else None,
                "temperature_gpu_c": round(temperature_value, 1) if temperature_value is not None else None,
                "metrics_source": metrics_source,
                "metrics_error": vast_error if vast_instance is None and instance_id is not None else None,
                "nvidia_metrics_available": nvidia_metrics_available,
                "kv_pool_tokens": metrics.get("kv_pool_tokens"),
                "kv_used_tokens": metrics.get("kv_used_tokens"),
                "kv_free_tokens": metrics.get("kv_free_tokens"),
                "kv_utilization_pct": metrics.get("kv_utilization_pct"),
                "success_count": int(gpu.get("success_count") or 0),
                "failure_count": int(gpu.get("failure_count") or 0),
                "worker_completed_jobs": int(gpu.get("worker_completed_jobs") or 0),
                "worker_failed_jobs": int(gpu.get("worker_failed_jobs") or 0),
                "worker_stats_available": bool(metrics.get("worker_stats_available")),
                "cooldown_seconds": round(cooldown_seconds, 1),
                "last_seen": gpu.get("last_seen"),
                "last_error": gpu.get("last_error"),
                "url": config.get("url"),
            }
        )
    return rows


def _dashboard_payload(
    snapshot: dict[str, Any],
    *,
    verathos: dict[str, Any] | None = None,
    verathos_error: str | None = None,
    verathos_checked_at: str | None = None,
    chain_epoch: dict[str, Any] | None = None,
    chain_epoch_error: str | None = None,
    chain_epoch_checked_at: str | None = None,
) -> dict[str, Any]:
    healthy_gpus = sum(1 for gpu in snapshot["gpus"].values() if gpu["healthy"])
    healthy_slots = sum(1 for slot in snapshot["slots"].values() if slot["healthy"])
    log_files = _available_log_names(snapshot)
    indexes = _verathos_indexes(verathos)
    public_urls = _public_slot_urls()
    supervisor_status = _supervisor_status()
    lease_status = _lease_watcher_status()
    receipt_integrity = _receipt_integrity_status(
        snapshot,
        public_urls=public_urls,
        lease_status=lease_status,
    )
    health_status = "ok"
    supervisor_summary = supervisor_status.get("summary") if isinstance(supervisor_status.get("summary"), dict) else {}
    if (
        supervisor_status.get("status") not in {"ok"}
        or int(supervisor_summary.get("down") or 0) > 0
        or int(supervisor_summary.get("restarting") or 0) > 0
    ):
        health_status = "warn"
    return {
        "checked_at": utc_now(),
        "health": {
            "status": health_status,
            "healthy_gpus": healthy_gpus,
            "total_gpus": len(snapshot["gpus"]),
            "healthy_slots": healthy_slots,
            "total_slots": len(snapshot["slots"]),
            "dispatch_queue_length": snapshot.get("dispatch_queue_length", 0),
        },
        "verathos": {
            "source": VERATHOS_DASHBOARD_URL,
            "checked_at": verathos_checked_at,
            "error": verathos_error,
            "epoch_number": verathos.get("epoch_number") if isinstance(verathos, dict) else None,
            "proxy_connected": verathos.get("proxy_connected") if isinstance(verathos, dict) else None,
            "network": verathos.get("network") if isinstance(verathos, dict) else None,
        },
        "epoch": {
            **(chain_epoch or {}),
            "status": "ok" if chain_epoch_error is None and chain_epoch is not None else "warn",
            "checked_at": chain_epoch_checked_at,
            "error": chain_epoch_error,
            "verathos_epoch_number": verathos.get("epoch_number") if isinstance(verathos, dict) else None,
        },
        "topology": snapshot.get("topology", {}),
        "supervisor": supervisor_status,
        "lease_watcher": lease_status,
        "receipt_integrity": receipt_integrity,
        "miners": _miner_dashboard_rows(
            snapshot,
            verathos=verathos,
            verathos_error=verathos_error,
            verathos_checked_at=verathos_checked_at,
            indexes=indexes,
            public_urls=public_urls,
            receipt_integrity=receipt_integrity,
            supervisor_status=supervisor_status,
        ),
        "slots": _slot_dashboard_rows(
            snapshot,
            indexes=indexes,
            public_urls=public_urls,
            receipt_integrity=receipt_integrity,
            supervisor_status=supervisor_status,
        ),
        "gpus": _gpu_dashboard_rows(snapshot),
        "logs": {
            "default": "router.log" if "router.log" in log_files else (log_files[0] if log_files else ""),
            "files": log_files,
        },
    }


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _models_payload_from_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    entries: dict[str, dict[str, Any]] = {}
    for gpu_id, gpu in snapshot.get("gpus", {}).items():
        config = gpu.get("config") if isinstance(gpu, dict) else {}
        metadata = config.get("metadata") if isinstance(config, dict) else {}
        if not isinstance(metadata, dict):
            metadata = {}
        model_id = str(
            metadata.get("runtime_checkpoint")
            or metadata.get("model_checkpoint")
            or metadata.get("model")
            or metadata.get("model_id")
            or "unknown"
        )
        quant = str(metadata.get("quant") or metadata.get("dtype") or "")
        max_context_len = _int_or_none(
            metadata.get("max_context_len")
            or metadata.get("max_model_len")
            or metadata.get("context_length")
        )
        entry = entries.setdefault(
            model_id,
            {
                "id": model_id,
                "object": "model",
                "created": 0,
                "owned_by": "verathos",
                "quant": quant or None,
                "max_context_len": max_context_len,
                "supported_parameters": [
                    "tools",
                    "tool_choice",
                    "parallel_tool_calls",
                ],
                "gpu_ids": [],
                "healthy_gpus": 0,
                "total_gpus": 0,
            },
        )
        if not entry.get("quant") and quant:
            entry["quant"] = quant
        if entry.get("max_context_len") is None and max_context_len is not None:
            entry["max_context_len"] = max_context_len
        entry["gpu_ids"].append(gpu_id)
        entry["total_gpus"] += 1
        if gpu.get("healthy"):
            entry["healthy_gpus"] += 1
    return {
        "object": "list",
        "data": sorted(entries.values(), key=lambda item: item["id"]),
    }


def create_app() -> FastAPI:
    topology_path = topology_path_from_env().resolve()
    topology = load_topology(topology_path)
    app = FastAPI(title="Miner GPU Router", version="0.1.0")
    app.state.router_state = RouterState(topology, topology_path=topology_path)
    app.state.monitor_task = None

    @app.on_event("startup")
    async def startup() -> None:
        app.state.dispatch_client = httpx.AsyncClient(
            timeout=topology.router.gpu_request_timeout_seconds,
        )
        app.state.external_client = httpx.AsyncClient(timeout=5.0)
        app.state.monitor_task = asyncio.create_task(_monitor_loop(app))

    @app.on_event("shutdown")
    async def shutdown() -> None:
        task = app.state.monitor_task
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        client = getattr(app.state, "dispatch_client", None)
        if client is not None:
            await client.aclose()
        external_client = getattr(app.state, "external_client", None)
        if external_client is not None:
            await external_client.aclose()

    @app.get("/health")
    async def health() -> dict[str, Any]:
        state: RouterState = app.state.router_state
        snapshot = await state.snapshot()
        healthy_gpus = sum(1 for gpu in snapshot["gpus"].values() if gpu["healthy"])
        healthy_slots = sum(1 for slot in snapshot["slots"].values() if slot["healthy"])
        return {
            "status": "ok",
            "healthy_gpus": healthy_gpus,
            "total_gpus": len(snapshot["gpus"]),
            "healthy_slots": healthy_slots,
            "total_slots": len(snapshot["slots"]),
            "dispatch_queue_length": snapshot.get("dispatch_queue_length", 0),
            "checked_at": utc_now(),
        }

    @app.get("/dashboard", response_class=HTMLResponse)
    async def dashboard_page() -> HTMLResponse:
        return HTMLResponse(DASHBOARD_HTML)

    @app.get("/", response_class=HTMLResponse)
    async def dashboard_root() -> HTMLResponse:
        return HTMLResponse(DASHBOARD_HTML)

    @app.get("/v1/state")
    async def state_endpoint() -> dict[str, Any]:
        state: RouterState = app.state.router_state
        return await state.snapshot()

    @app.post("/v1/topology/reload")
    async def topology_reload_endpoint() -> dict[str, Any]:
        state: RouterState = app.state.router_state
        fingerprint = _topology_fingerprint(state.topology_path)
        try:
            topology = load_topology(state.topology_path)
        except Exception as exc:
            error = f"{exc.__class__.__name__}: {exc}"
            await state.mark_topology_reload_error(error)
            raise HTTPException(status_code=422, detail=error) from exc
        return await state.reconcile_topology(
            topology,
            fingerprint=fingerprint,
            source="manual",
        )

    @app.get("/v1/dashboard")
    async def dashboard_endpoint() -> dict[str, Any]:
        state: RouterState = app.state.router_state
        verathos, verathos_error, verathos_checked_at = await state.verathos_status(
            app.state.external_client,
        )
        chain_epoch, chain_epoch_error, chain_epoch_checked_at = await state.chain_epoch_status(
            app.state.external_client,
        )
        return _dashboard_payload(
            await state.snapshot(),
            verathos=verathos,
            verathos_error=verathos_error,
            verathos_checked_at=verathos_checked_at,
            chain_epoch=chain_epoch,
            chain_epoch_error=chain_epoch_error,
            chain_epoch_checked_at=chain_epoch_checked_at,
        )

    @app.get("/v1/dashboard/logs")
    async def dashboard_logs_endpoint(
        name: str = Query(default="router.log", min_length=1, max_length=160),
        lines: int = Query(default=180, ge=1, le=1000),
    ) -> dict[str, Any]:
        return {
            "name": name,
            "lines": lines,
            "content": _read_log_tail(name, lines=lines),
            "checked_at": utc_now(),
        }

    @app.get("/v1/model_spec")
    async def model_spec_endpoint() -> Response:
        state: RouterState = app.state.router_state
        return await _proxy_model_spec_from_gpu(state, app.state.dispatch_client)

    @app.get("/models")
    @app.get("/v1/models")
    async def models_endpoint() -> dict[str, Any]:
        state: RouterState = app.state.router_state
        return _models_payload_from_snapshot(await state.snapshot())

    @app.get("/v1/slots")
    async def slots_endpoint() -> dict[str, Any]:
        state: RouterState = app.state.router_state
        return (await state.snapshot())["slots"]

    @app.get("/v1/gpus")
    async def gpus_endpoint() -> dict[str, Any]:
        state: RouterState = app.state.router_state
        return (await state.snapshot())["gpus"]

    @app.post("/v1/gpus/register")
    async def register_gpu(request: GpuRegisterRequest) -> dict[str, Any]:
        state: RouterState = app.state.router_state
        await state.register_gpu(request)
        return {"ok": True, "gpu_id": request.gpu_id, "registered_at": utc_now()}

    @app.delete("/v1/gpus/{gpu_id}")
    async def unregister_gpu(gpu_id: str) -> dict[str, Any]:
        state: RouterState = app.state.router_state
        removed = await state.unregister_gpu(gpu_id)
        if not removed:
            raise HTTPException(status_code=404, detail=f"unknown gpu_id: {gpu_id}")
        return {"ok": True, "gpu_id": gpu_id, "unregistered_at": utc_now()}

    @app.post("/v1/dispatch/stream")
    async def dispatch_stream(request: DispatchRequest) -> StreamingResponse:
        state: RouterState = app.state.router_state
        slot = state.slots.get(request.slot_id)
        if slot is None:
            raise HTTPException(status_code=404, detail=f"unknown slot_id: {request.slot_id}")
        if slot.config.miner_id != request.miner_id:
            raise HTTPException(
                status_code=403,
                detail={
                    "error": "miner_id does not match slot owner",
                    "miner_id": request.miner_id,
                    "slot_id": request.slot_id,
                    "expected_miner_id": slot.config.miner_id,
                },
            )
        await state.mark_slot_request(request.slot_id)

        started = time.monotonic()
        timeout = request.timeout_seconds or state.topology.router.gpu_request_timeout_seconds
        deadline = time.monotonic() + min(timeout, state.topology.router.dispatch_wait_seconds)
        attempts: list[DispatchAttempt] = []
        attempted_gpu_ids: set[str] = set()
        missing_verathos_backend = False

        while time.monotonic() < deadline:
            if len(attempted_gpu_ids) >= max(1, len(state.gpus)):
                if missing_verathos_backend:
                    break
                attempted_gpu_ids.clear()
            gpu = await state.acquire_gpu(
                request_id=request.request_id,
                exclude=attempted_gpu_ids,
                deadline=deadline,
            )
            if gpu is None:
                break

            gpu_id = gpu.config.gpu_id
            gpu_started = time.monotonic()
            execute_request = GpuExecuteRequest(
                request_id=request.request_id,
                miner_id=request.miner_id,
                slot_id=request.slot_id,
                slot_url=slot.config.url,
                payload=request.payload,
                timeout_seconds=timeout,
            )
            try:
                client: httpx.AsyncClient = app.state.dispatch_client
                response = await _open_gpu_execute_stream(
                    client=client,
                    gpu=gpu,
                    execute_request=execute_request,
                    timeout=timeout,
                )
                if response.status_code in {404, 405}:
                    await response.aclose()
                    buffered_response = await _post_gpu_execute_with_stale_watch(
                        client=client,
                        state=state,
                        gpu=gpu,
                        execute_request=execute_request,
                        timeout=timeout,
                        acquired_at=gpu_started,
                    )
                    buffered_response.raise_for_status()
                    data = _validate_gpu_execute_response(
                        data=buffered_response.json(),
                        expected=execute_request,
                        expected_gpu_id=gpu_id,
                    )
                    await state.release_gpu(gpu_id, success=True)
                    await state.mark_slot_result(request.slot_id, success=True)
                    attempts.append(
                        DispatchAttempt(
                            gpu_id=gpu_id,
                            status="ok-buffered-fallback",
                            elapsed_ms=round((time.monotonic() - gpu_started) * 1000.0, 3),
                        )
                    )
                    return StreamingResponse(
                        _replay_buffered_verathos_result(data.result),
                        media_type="text/event-stream",
                        headers={
                            "Cache-Control": "no-cache",
                            "X-Accel-Buffering": "no",
                            "X-Router-GPU-ID": gpu_id,
                            "X-Router-Mode": "buffered-fallback",
                        },
                    )
                if response.status_code >= 400:
                    body = await response.aread()
                    await response.aclose()
                    raise httpx.HTTPStatusError(
                        f"GPU stream endpoint returned HTTP {response.status_code}: "
                        f"{body.decode('utf-8', errors='replace')[:500]}",
                        request=response.request,
                        response=response,
                    )

                attempts.append(
                    DispatchAttempt(
                        gpu_id=gpu_id,
                        status="streaming",
                        elapsed_ms=round((time.monotonic() - gpu_started) * 1000.0, 3),
                    )
                )
                return StreamingResponse(
                    _relay_gpu_sse_stream(
                        response=response,
                        state=state,
                        gpu_id=gpu_id,
                        slot_id=request.slot_id,
                        router_started=started,
                        gpu_started=gpu_started,
                        stream_opened=time.monotonic(),
                    ),
                    media_type="text/event-stream",
                    headers={
                        "Cache-Control": "no-cache",
                        "X-Accel-Buffering": "no",
                        "X-Router-GPU-ID": gpu_id,
                        "X-Router-Elapsed-Ms": str(round((time.monotonic() - started) * 1000.0, 3)),
                    },
                )
            except Exception as exc:
                attempted_gpu_ids.add(gpu_id)
                if _is_missing_verathos_backend(exc):
                    missing_verathos_backend = True
                error = f"{exc.__class__.__name__}: {exc}"
                await state.release_gpu(
                    gpu_id,
                    success=False,
                    error=error,
                    unhealthy=_should_mark_gpu_unhealthy(exc),
                )
                attempts.append(
                    DispatchAttempt(
                        gpu_id=gpu_id,
                        status="failed",
                        error=error,
                        elapsed_ms=round((time.monotonic() - gpu_started) * 1000.0, 3),
                    )
                )
                await asyncio.sleep(0.05)

        await state.cancel_dispatch(request.request_id)
        final_error = (
            "no GPU-side Verathos miner/proof backend configured"
            if missing_verathos_backend
            else "no healthy GPU capacity available before dispatch deadline"
        )
        await state.mark_slot_result(
            request.slot_id,
            success=False,
            error=final_error,
        )
        raise HTTPException(
            status_code=503,
            detail={
                "error": final_error,
                "request_id": request.request_id,
                "attempts": [attempt.model_dump() for attempt in attempts],
            },
        )

    @app.post("/v1/dispatch")
    async def dispatch(request: DispatchRequest) -> DispatchResponse:
        state: RouterState = app.state.router_state
        slot = state.slots.get(request.slot_id)
        if slot is None:
            raise HTTPException(status_code=404, detail=f"unknown slot_id: {request.slot_id}")
        if slot.config.miner_id != request.miner_id:
            raise HTTPException(
                status_code=403,
                detail={
                    "error": "miner_id does not match slot owner",
                    "miner_id": request.miner_id,
                    "slot_id": request.slot_id,
                    "expected_miner_id": slot.config.miner_id,
                },
            )
        await state.mark_slot_request(request.slot_id)

        started = time.monotonic()
        timeout = request.timeout_seconds or state.topology.router.gpu_request_timeout_seconds
        deadline = time.monotonic() + min(timeout, state.topology.router.dispatch_wait_seconds)
        attempts: list[DispatchAttempt] = []
        attempted_gpu_ids: set[str] = set()
        missing_verathos_backend = False

        while time.monotonic() < deadline:
            if len(attempted_gpu_ids) >= max(1, len(state.gpus)):
                if missing_verathos_backend:
                    break
                attempted_gpu_ids.clear()
            gpu = await state.acquire_gpu(
                request_id=request.request_id,
                exclude=attempted_gpu_ids,
                deadline=deadline,
            )
            if gpu is None:
                break

            gpu_id = gpu.config.gpu_id
            gpu_started = time.monotonic()
            try:
                execute_request = GpuExecuteRequest(
                    request_id=request.request_id,
                    miner_id=request.miner_id,
                    slot_id=request.slot_id,
                    slot_url=slot.config.url,
                    payload=request.payload,
                    timeout_seconds=timeout,
                )
                client: httpx.AsyncClient = app.state.dispatch_client
                response = await _post_gpu_execute_with_stale_watch(
                    client=client,
                    state=state,
                    gpu=gpu,
                    execute_request=execute_request,
                    timeout=timeout,
                    acquired_at=gpu_started,
                )
                response.raise_for_status()
                data = _validate_gpu_execute_response(
                    data=response.json(),
                    expected=execute_request,
                    expected_gpu_id=gpu_id,
                )
                await state.release_gpu(gpu_id, success=True)
                await state.mark_slot_result(request.slot_id, success=True)
                attempts.append(
                    DispatchAttempt(
                        gpu_id=gpu_id,
                        status="ok",
                        elapsed_ms=round((time.monotonic() - gpu_started) * 1000.0, 3),
                    )
                )
                return DispatchResponse(
                    request_id=request.request_id,
                    miner_id=request.miner_id,
                    slot_id=request.slot_id,
                    gpu_id=gpu_id,
                    result=data.result,
                    attempts=attempts,
                    elapsed_ms=round((time.monotonic() - started) * 1000.0, 3),
                )
            except Exception as exc:
                attempted_gpu_ids.add(gpu_id)
                if _is_missing_verathos_backend(exc):
                    missing_verathos_backend = True
                error = f"{exc.__class__.__name__}: {exc}"
                await state.release_gpu(
                    gpu_id,
                    success=False,
                    error=error,
                    unhealthy=_should_mark_gpu_unhealthy(exc),
                )
                attempts.append(
                    DispatchAttempt(
                        gpu_id=gpu_id,
                        status="failed",
                        error=error,
                        elapsed_ms=round((time.monotonic() - gpu_started) * 1000.0, 3),
                    )
                )
                await asyncio.sleep(0.05)

        await state.cancel_dispatch(request.request_id)
        final_error = (
            "no GPU-side Verathos miner/proof backend configured"
            if missing_verathos_backend
            else "no healthy GPU capacity available before dispatch deadline"
        )
        await state.mark_slot_result(
            request.slot_id,
            success=False,
            error=final_error,
        )
        raise HTTPException(
            status_code=503,
            detail={
                "error": final_error,
                "request_id": request.request_id,
                "attempts": [attempt.model_dump() for attempt in attempts],
            },
        )

    return app
