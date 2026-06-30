from __future__ import annotations

import asyncio
import base64
import json
import os
import subprocess
import time
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, Response

from miner_gpu_control.models import GpuExecuteRequest, GpuExecuteResponse, utc_now


GPU_ID = os.getenv("GPU_ID", "gpu-local")
GPU_NAME = os.getenv("GPU_NAME", GPU_ID)
GPU_MAX_JOBS = max(1, int(os.getenv("GPU_MAX_JOBS", "1")))
GPU_DEVICE_INDEX = os.getenv("GPU_DEVICE_INDEX", "0")
MOCK_LATENCY_SECONDS = float(os.getenv("GPU_MOCK_LATENCY_SECONDS", "0.05"))

OPENAI_BASE_URL = os.getenv("OPENAI_COMPATIBLE_BASE_URL", "").rstrip("/")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "")
OPENAI_DEFAULT_MAX_TOKENS = int(os.getenv("OPENAI_DEFAULT_MAX_TOKENS", "512"))
OPENAI_DISABLE_THINKING = os.getenv("OPENAI_DISABLE_THINKING", "1").lower() not in {
    "0",
    "false",
    "no",
}
VERATHOS_MINER_BASE_URL = os.getenv("VERATHOS_MINER_BASE_URL", "").rstrip("/")
GPU_REQUIRE_PROOF = os.getenv("GPU_REQUIRE_PROOF", "0").lower() in {
    "1",
    "true",
    "yes",
}
BACKEND_HEALTH_TIMEOUT_SECONDS = float(os.getenv("GPU_BACKEND_HEALTH_TIMEOUT_SECONDS", "2.0"))

app = FastAPI(title=f"GPU Worker {GPU_ID}", version="0.1.0")
_semaphore = asyncio.Semaphore(GPU_MAX_JOBS)
_active_jobs = 0
_completed_jobs = 0
_failed_jobs = 0
_lock = asyncio.Lock()


def _query_nvidia_smi() -> dict[str, Any]:
    query = (
        "index,name,memory.total,memory.used,utilization.gpu,temperature.gpu"
    )
    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                f"--query-gpu={query}",
                "--format=csv,noheader,nounits",
                "-i",
                GPU_DEVICE_INDEX,
            ],
            capture_output=True,
            text=True,
            timeout=1.5,
            check=True,
        )
    except Exception as exc:
        return {
            "available": False,
            "error": exc.__class__.__name__,
        }

    line = result.stdout.strip().splitlines()[0] if result.stdout.strip() else ""
    parts = [part.strip() for part in line.split(",")]
    if len(parts) != 6:
        return {"available": False, "error": "unexpected nvidia-smi output"}
    total = int(float(parts[2]))
    used = int(float(parts[3]))
    return {
        "available": True,
        "device_index": parts[0],
        "name": parts[1],
        "memory_total_mb": total,
        "memory_used_mb": used,
        "memory_free_mb": max(0, total - used),
        "utilization_gpu_percent": int(float(parts[4])),
        "temperature_gpu_c": int(float(parts[5])),
    }


def _rough_token_count(text: str) -> int:
    return max(1, len(text.split()))


def _walk_dicts(value: Any, *, depth: int = 0, max_depth: int = 5):
    if depth > max_depth:
        return
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from _walk_dicts(item, depth=depth + 1, max_depth=max_depth)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_dicts(item, depth=depth + 1, max_depth=max_depth)


def _first_dict_field(value: Any, names: tuple[str, ...]) -> dict[str, Any] | None:
    for item in _walk_dicts(value):
        for name in names:
            candidate = item.get(name)
            if isinstance(candidate, dict):
                return candidate
    return None


def _extract_openai_text(data: dict[str, Any]) -> str:
    choices = data.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict):
            message = first.get("message")
            if isinstance(message, dict):
                content = message.get("content")
                if isinstance(content, str):
                    return content
            text = first.get("text")
            if isinstance(text, str):
                return text
    output_text = data.get("output_text")
    if isinstance(output_text, str):
        return output_text
    return ""


def _extract_usage_tokens(data: dict[str, Any], *, prompt_text: str, output_text: str) -> tuple[int, int]:
    usage = data.get("usage")
    if isinstance(usage, dict):
        input_tokens = usage.get("prompt_tokens") or usage.get("input_tokens")
        output_tokens = usage.get("completion_tokens") or usage.get("output_tokens")
        if isinstance(input_tokens, int) and isinstance(output_tokens, int):
            return input_tokens, output_tokens
    return _rough_token_count(prompt_text), _rough_token_count(output_text)


def _extract_proof_fields(data: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any]]:
    commitment = _first_dict_field(data, ("commitment", "inference_commitment"))
    proof_bundle = _first_dict_field(data, ("proof_bundle", "proofBundle"))
    if proof_bundle is None:
        proof = _first_dict_field(data, ("proof",))
        if proof is not None and (
            "layer_proofs" in proof
            or "sampling_proofs" in proof
            or "router_commitments" in proof
        ):
            proof_bundle = proof
    timing = _first_dict_field(data, ("timing", "metrics")) or {}
    return commitment, proof_bundle, timing


def _build_done_event(
    *,
    output_text: str,
    input_text: str,
    response_data: dict[str, Any],
    inference_ms: float,
) -> dict[str, Any]:
    commitment, proof_bundle, timing = _extract_proof_fields(response_data)
    input_tokens, output_tokens = _extract_usage_tokens(
        response_data,
        prompt_text=input_text,
        output_text=output_text,
    )
    proof_available = isinstance(commitment, dict) and isinstance(proof_bundle, dict)

    done = {
        "output_text": output_text,
        "input_tokens": int(timing.get("input_tokens", input_tokens)),
        "output_tokens": int(timing.get("output_tokens", output_tokens)),
        "inference_ms": float(timing.get("inference_ms", round(inference_ms, 1))),
        "ttft_ms": float(timing.get("ttft_ms", 0)),
        "commitment_ms": float(timing.get("commitment_ms", 0)),
        "beacon_ms": float(timing.get("beacon_ms", 0)),
        "challenge_ms": float(timing.get("challenge_ms", 0)),
        "prove_ms": float(timing.get("prove_ms", 0)),
        "proof_available": proof_available,
        "proof_source": "backend" if proof_available else "unavailable",
    }
    if proof_available:
        done["commitment"] = commitment
        done["proof_bundle"] = proof_bundle
    else:
        done["proof_error"] = (
            "openai_compatible backend did not return Verathos commitment/proof_bundle"
        )
    return done


async def _run_openai_compatible(payload: dict[str, Any], timeout_seconds: float) -> dict[str, Any]:
    if not OPENAI_BASE_URL:
        raise RuntimeError("OPENAI_COMPATIBLE_BASE_URL is not configured")
    started = time.monotonic()
    body = dict(payload)
    if OPENAI_MODEL and "model" not in body:
        body["model"] = OPENAI_MODEL
    if "messages" not in body:
        content = payload.get("text") if isinstance(payload.get("text"), str) else str(payload)
        body["messages"] = [{"role": "user", "content": content}]
    if OPENAI_DEFAULT_MAX_TOKENS > 0 and "max_tokens" not in body:
        body["max_tokens"] = OPENAI_DEFAULT_MAX_TOKENS
    if OPENAI_DISABLE_THINKING:
        chat_template_kwargs = body.setdefault("chat_template_kwargs", {})
        if isinstance(chat_template_kwargs, dict):
            chat_template_kwargs.setdefault("enable_thinking", False)
    headers = {"Content-Type": "application/json"}
    if OPENAI_API_KEY:
        headers["Authorization"] = f"Bearer {OPENAI_API_KEY}"
    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        response = await client.post(
            f"{OPENAI_BASE_URL}/chat/completions",
            headers=headers,
            json=body,
        )
        response.raise_for_status()
        data = response.json()
    output_text = _extract_openai_text(data)
    input_text = str(payload.get("text") or body.get("messages") or "")
    done = _build_done_event(
        output_text=output_text,
        input_text=input_text,
        response_data=data,
        inference_ms=(time.monotonic() - started) * 1000.0,
    )
    if GPU_REQUIRE_PROOF and not done.get("proof_available"):
        raise RuntimeError(str(done.get("proof_error") or "proof unavailable"))
    return {
        "mode": "openai_compatible",
        "gpu_id": GPU_ID,
        "model": body.get("model"),
        "text": output_text,
        "proof_available": bool(done.get("proof_available")),
        "sse_done": done,
        "response": data,
    }


async def _parse_sse(response: httpx.Response):
    event_type = None
    data_lines: list[str] = []
    async for line in response.aiter_lines():
        if line.startswith("event:"):
            event_type = line[len("event:"):].strip()
        elif line.startswith("data:"):
            data_lines.append(line[len("data:"):].strip())
        elif line == "":
            if data_lines:
                raw = "\n".join(data_lines)
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    data = {"raw": raw}
                yield event_type or data.get("event", ""), data
            event_type = None
            data_lines = []
    if data_lines:
        raw = "\n".join(data_lines)
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            data = {"raw": raw}
        yield event_type or data.get("event", ""), data


async def _run_verathos_miner(payload: dict[str, Any], timeout_seconds: float) -> dict[str, Any]:
    if not VERATHOS_MINER_BASE_URL:
        raise RuntimeError("VERATHOS_MINER_BASE_URL is not configured")

    path = str(payload.get("verathos_path") or "")
    if path not in {"/chat", "/inference"}:
        path = "/chat" if isinstance(payload.get("messages"), list) else "/inference"

    raw_body_b64 = payload.get("verathos_raw_body_b64")
    if isinstance(raw_body_b64, str) and raw_body_b64:
        body = base64.b64decode(raw_body_b64.encode("ascii"))
    else:
        body_obj = {
            key: value
            for key, value in payload.items()
            if not key.startswith("verathos_") and key not in {"text"}
        }
        if path == "/inference" and "prompt" not in body_obj:
            body_obj["prompt"] = payload.get("text", "")
        body = json.dumps(body_obj, separators=(",", ":")).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
    }
    forwarded = payload.get("verathos_headers")
    if isinstance(forwarded, dict):
        for key, value in forwarded.items():
            if isinstance(key, str) and isinstance(value, str) and key.lower().startswith("x-validator-"):
                headers[key] = value

    full_text = ""
    done: dict[str, Any] | None = None
    events: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=timeout_seconds, verify=False) as client:
        async with client.stream(
            "POST",
            f"{VERATHOS_MINER_BASE_URL}{path}",
            content=body,
            headers=headers,
        ) as response:
            response.raise_for_status()
            async for event, data in _parse_sse(response):
                events.append({"event": event, "data": data})
                if event == "token":
                    full_text += str(data.get("text", ""))
                elif event == "done":
                    done = data
                elif event == "error":
                    raise RuntimeError(f"VeraLLM miner error: {data.get('error', data)}")

    if done is None:
        raise RuntimeError("VeraLLM miner stream ended without done event")
    output_text = done.get("output_text")
    if isinstance(output_text, str) and output_text:
        full_text = output_text

    return {
        "mode": "verathos_miner",
        "gpu_id": GPU_ID,
        "path": path,
        "text": full_text,
        "proof_available": bool(done.get("commitment") and done.get("proof_bundle")),
        "sse_done": done,
        "sse_events": events,
    }


async def _verathos_backend_health() -> tuple[bool, dict[str, Any] | None, str | None]:
    if not VERATHOS_MINER_BASE_URL:
        return True, None, None
    try:
        async with httpx.AsyncClient(timeout=BACKEND_HEALTH_TIMEOUT_SECONDS, verify=False) as client:
            response = await client.get(f"{VERATHOS_MINER_BASE_URL}/health")
            response.raise_for_status()
            data = response.json()
        return data.get("status") == "ok", data if isinstance(data, dict) else {}, None
    except Exception as exc:
        return False, None, f"{exc.__class__.__name__}: {exc}"


async def _run_mock(payload: dict[str, Any]) -> dict[str, Any]:
    await asyncio.sleep(MOCK_LATENCY_SECONDS)
    return {
        "mode": "mock",
        "gpu_id": GPU_ID,
        "gpu_name": GPU_NAME,
        "received_payload": payload,
        "proof_available": False,
        "completed_at": utc_now(),
    }


@app.get("/health")
async def health() -> dict[str, Any]:
    async with _lock:
        active_jobs = _active_jobs
        completed_jobs = _completed_jobs
        failed_jobs = _failed_jobs
    if VERATHOS_MINER_BASE_URL:
        backend = "verathos_miner"
    elif OPENAI_BASE_URL:
        backend = "openai_compatible"
    else:
        backend = "mock"
    backend_ok, backend_health, backend_error = await _verathos_backend_health()
    return {
        "status": "ok" if backend_ok else "error",
        "gpu_id": GPU_ID,
        "gpu_name": GPU_NAME,
        "active_jobs": active_jobs,
        "max_jobs": GPU_MAX_JOBS,
        "available_jobs": max(0, GPU_MAX_JOBS - active_jobs),
        "completed_jobs": completed_jobs,
        "failed_jobs": failed_jobs,
        "backend": backend,
        "backend_url": VERATHOS_MINER_BASE_URL or None,
        "backend_health": backend_health,
        "backend_error": backend_error,
        "require_proof": GPU_REQUIRE_PROOF,
        "gpu_metrics": _query_nvidia_smi(),
        "checked_at": utc_now(),
    }


@app.get("/model_spec")
async def model_spec() -> Response:
    if not VERATHOS_MINER_BASE_URL:
        return JSONResponse(status_code=503, content={"error": "Model not loaded"})
    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            response = await client.get(f"{VERATHOS_MINER_BASE_URL}/model_spec")
    except Exception:
        return JSONResponse(status_code=503, content={"error": "Model not loaded"})
    content_type = response.headers.get("content-type", "application/json")
    return Response(
        content=response.content,
        status_code=response.status_code,
        media_type=content_type.split(";", 1)[0],
    )


@app.post("/execute", response_model=GpuExecuteResponse)
async def execute(request: GpuExecuteRequest) -> GpuExecuteResponse:
    global _active_jobs, _completed_jobs, _failed_jobs
    try:
        await asyncio.wait_for(_semaphore.acquire(), timeout=0.01)
    except asyncio.TimeoutError as exc:
        raise HTTPException(status_code=429, detail="gpu worker has no free job capacity") from exc

    started = time.monotonic()
    async with _lock:
        _active_jobs += 1
    try:
        timeout = request.timeout_seconds or 300.0
        if request.payload.get("verathos_endpoint"):
            if not VERATHOS_MINER_BASE_URL:
                raise RuntimeError(
                    "Verathos requests require a GPU-side Verathos miner/proof backend; "
                    "openai_compatible and mock backends cannot produce ZK proof"
                )
            result = await _run_verathos_miner(request.payload, timeout)
        elif OPENAI_BASE_URL:
            result = await _run_openai_compatible(request.payload, timeout)
        else:
            result = await _run_mock(request.payload)
        async with _lock:
            _completed_jobs += 1
        return GpuExecuteResponse(
            ok=True,
            request_id=request.request_id,
            miner_id=request.miner_id,
            slot_id=request.slot_id,
            slot_url=request.slot_url,
            gpu_id=GPU_ID,
            elapsed_ms=round((time.monotonic() - started) * 1000.0, 3),
            result=result,
        )
    except Exception as exc:
        async with _lock:
            _failed_jobs += 1
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(exc),
                "exception": exc.__class__.__name__,
                "gpu_id": GPU_ID,
            },
        ) from exc
    finally:
        async with _lock:
            _active_jobs -= 1
        _semaphore.release()
