from __future__ import annotations

import os
import asyncio
import contextlib
import hashlib
import json
import time
import base64
import sqlite3
import threading
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, ValidationError

from miner_gpu_control.config import router_url_from_env
from miner_gpu_control.models import (
    DispatchRequest,
    DispatchResponse,
    EntrypointRequest,
    SlotRequest,
    ValidatorQuery,
    ValidatorResponse,
    utc_now,
)
from miner_gpu_control.verathos_auth import DEFAULT_VALIDATORS_PATH, ValidatorAuthMiddleware


MINER_ID = os.getenv("MINER_ID", "miner-local")
HOTKEY = os.getenv("HOTKEY", MINER_ID)
COLDKEY = os.getenv("COLDKEY", "")
SLOT_ID = os.getenv("SLOT_ID", "slot-local")
SLOT_INDEX = int(os.getenv("SLOT_INDEX", "1"))
ROUTER_URL = router_url_from_env()
REQUEST_TIMEOUT_SECONDS = float(os.getenv("SLOT_REQUEST_TIMEOUT_SECONDS", "930"))
STREAM_KEEPALIVE_SECONDS = float(os.getenv("SLOT_STREAM_KEEPALIVE_SECONDS", "15"))
STREAM_STATUS_EVENT = os.getenv("SLOT_STREAM_STATUS_EVENT", "status").strip() or "status"
STREAM_STATUS_TEXT = os.getenv("SLOT_STREAM_STATUS_TEXT", "").strip()
OPEN_STREAM_STATUS_EVENT = "token"
OPEN_STREAM_STATUS_TEXT = "Ok"
OPEN_STREAM_STATUS_DELAY_SECONDS = float(os.getenv("SLOT_OPEN_STREAM_STATUS_DELAY_SECONDS", "0"))
MODEL_SPEC_URL = os.getenv("SLOT_MODEL_SPEC_URL", "").rstrip("/")
MODEL_SPEC_CACHE_PATH = Path(
    os.getenv("SLOT_MODEL_SPEC_CACHE_PATH", str(Path("run") / "model_spec_cache.json"))
)
MODEL_ID = os.getenv("SLOT_MODEL_ID", "Qwen/Qwen3.5-9B")
QUANT = os.getenv("SLOT_QUANT", "fp16")
MAX_CONTEXT_LEN = int(os.getenv("SLOT_MAX_CONTEXT_LEN", "262144"))
GPU_NAME = os.getenv("SLOT_GPU_NAME", "")
GPU_UUIDS = [item for item in os.getenv("SLOT_GPU_UUIDS", SLOT_ID).split(",") if item]
PROOF_MODE = os.getenv("SLOT_PROOF_MODE", "gpu_zk_required")
REQUIRE_GPU_ZK_PROOF = os.getenv("SLOT_REQUIRE_GPU_ZK_PROOF", "1").lower() not in {
    "0",
    "false",
    "no",
}
MAX_REQUESTS = max(1, int(os.getenv("SLOT_MAX_REQUESTS", "1")))
SLOT_VRAM_GB = int(os.getenv("SLOT_VRAM_GB", "0"))
RECEIPT_DB_PATH = Path(
    os.getenv("SLOT_RECEIPT_DB", str(Path("run") / "receipts" / f"{SLOT_ID}.db"))
)

app = FastAPI(title=f"Miner Slot {SLOT_ID}", version="0.1.0")
app.add_middleware(ValidatorAuthMiddleware)
_request_count = 0
_error_count = 0
_active_requests = 0
_active_requests_lock = asyncio.Lock()
_receipt_store: ReceiptStore | None = None
_evm_identity: dict[str, str] | None = None
_model_spec_cache: tuple[bytes, str] | None = None


class InferenceRequestBody(BaseModel):
    model_config = ConfigDict(extra="allow")

    prompt: str
    validator_nonce: str
    max_new_tokens: int = 4096
    do_sample: bool = False
    temperature: float = 1.0
    sampling_verification_bps: int = 0
    enable_thinking: bool = True
    presence_penalty: float | None = None
    top_k: int | None = None
    top_p: float | None = None
    min_p: float | None = None


class ChatMessage(BaseModel):
    model_config = ConfigDict(extra="allow")

    role: str
    content: Any | None = ""
    tool_calls: list[dict[str, Any]] | None = None
    tool_call_id: str | None = None
    name: str | None = None


class ChatRequestBody(BaseModel):
    model_config = ConfigDict(extra="allow")

    messages: list[ChatMessage]
    validator_nonce: str
    max_new_tokens: int = 4096
    do_sample: bool = False
    temperature: float = 1.0
    sampling_verification_bps: int = 0
    enable_thinking: bool = True
    presence_penalty: float | None = None
    top_k: int | None = None
    top_p: float | None = None
    min_p: float | None = None
    tools: list[dict[str, Any]] | None = None
    tool_choice: Any | None = None
    parallel_tool_calls: bool | None = None


class IdentityChallengeBody(BaseModel):
    nonce: str


class EpochReceiptBody(BaseModel):
    miner_address: str
    model_id: str
    model_index: int
    epoch_number: int
    commitment_hash: str
    timestamp: int
    ttft_ms: float
    tokens_generated: int
    generation_time_ms: float
    tokens_per_sec: float
    prompt_tokens: int = 0
    proof_verified: bool = False
    proof_requested: bool = False
    tee_attestation_verified: bool | None = None
    is_canary: bool = False
    validator_hotkey: str
    validator_signature: str


class TeeChatRequestBody(BaseModel):
    model_config = ConfigDict(extra="allow")

    envelope: dict[str, Any]
    validator_nonce: str


def _receipt_json(receipt: dict[str, Any]) -> str:
    return json.dumps(receipt, sort_keys=True, separators=(",", ":"))


def _receipt_hash(receipt: dict[str, Any]) -> str:
    return hashlib.sha256(_receipt_json(receipt).encode("utf-8")).hexdigest()


class ReceiptStore:
    def __init__(self, db_path: Path):
        self._db_path = db_path
        self._lock = threading.Lock()
        self._cache: dict[int, list[dict[str, Any]]] = {}
        self._conn: sqlite3.Connection | None = None
        self._init_db()
        self._load_recent()

    def _init_db(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS receipts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                epoch INTEGER NOT NULL,
                receipt_json TEXT NOT NULL,
                receipt_hash TEXT
            )
            """
        )
        columns = {
            row[1]
            for row in self._conn.execute("PRAGMA table_info(receipts)").fetchall()
        }
        if "receipt_hash" not in columns:
            self._conn.execute("ALTER TABLE receipts ADD COLUMN receipt_hash TEXT")
        self._backfill_receipt_hashes()
        self._delete_duplicate_receipts()
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_receipts_epoch ON receipts(epoch)")
        self._conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_receipts_hash ON receipts(receipt_hash) "
            "WHERE receipt_hash IS NOT NULL"
        )
        self._conn.commit()

    def _backfill_receipt_hashes(self) -> None:
        rows = self._conn.execute(
            "SELECT id, receipt_json FROM receipts WHERE receipt_hash IS NULL OR receipt_hash = ''"
        ).fetchall()
        for row_id, raw in rows:
            try:
                receipt = json.loads(raw)
                receipt_hash = _receipt_hash(receipt)
            except Exception:
                receipt_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
            self._conn.execute(
                "UPDATE receipts SET receipt_hash = ? WHERE id = ?",
                (receipt_hash, row_id),
            )

    def _delete_duplicate_receipts(self) -> None:
        self._conn.execute(
            """
            DELETE FROM receipts
            WHERE receipt_hash IS NOT NULL
              AND id NOT IN (
                  SELECT MIN(id)
                  FROM receipts
                  WHERE receipt_hash IS NOT NULL
                  GROUP BY receipt_hash
              )
            """
        )

    def _epoch_count(self, epoch: int) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) FROM receipts WHERE epoch = ?",
            (epoch,),
        ).fetchone()
        return int(row[0]) if row is not None else 0

    def _load_recent(self) -> None:
        with self._lock:
            cursor = self._conn.execute("SELECT DISTINCT epoch FROM receipts ORDER BY epoch DESC LIMIT 3")
            for row in cursor.fetchall():
                epoch = int(row[0])
                rows = self._conn.execute(
                    "SELECT receipt_json FROM receipts WHERE epoch = ?",
                    (epoch,),
                ).fetchall()
                self._cache[epoch] = [json.loads(item[0]) for item in rows]

    def add(self, epoch: int, receipt: dict[str, Any]) -> int:
        with self._lock:
            receipt_hash = _receipt_hash(receipt)
            receipt_json = _receipt_json(receipt)
            cursor = self._conn.execute(
                "INSERT OR IGNORE INTO receipts (epoch, receipt_json, receipt_hash) VALUES (?, ?, ?)",
                (epoch, receipt_json, receipt_hash),
            )
            self._conn.commit()
            if cursor.rowcount:
                self._cache.setdefault(epoch, []).append(receipt)
            return self._epoch_count(epoch)

    def get(self, epoch: int) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._cache.get(epoch, []))

    def gc(self, current_epoch: int) -> None:
        cutoff = current_epoch - 3
        with self._lock:
            for epoch in [epoch for epoch in self._cache if epoch < cutoff]:
                del self._cache[epoch]
            self._conn.execute("DELETE FROM receipts WHERE epoch < ?", (cutoff,))
            self._conn.commit()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


@app.on_event("startup")
async def startup() -> None:
    global _receipt_store
    app.state.router_client = httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS)
    _receipt_store = ReceiptStore(RECEIPT_DB_PATH)


@app.on_event("shutdown")
async def shutdown() -> None:
    client = getattr(app.state, "router_client", None)
    if client is not None:
        await client.aclose()
    if _receipt_store is not None:
        _receipt_store.close()


def _validator_auth_status() -> dict[str, Any]:
    path = Path(os.getenv("VERATHOS_VALIDATORS_PATH", DEFAULT_VALIDATORS_PATH))
    status: dict[str, Any] = {
        "mode": os.getenv("SLOT_VALIDATOR_AUTH_MODE", "strict"),
        "validators_path": str(path),
        "allowlist_loaded": False,
        "validator_count": 0,
    }
    if not path.exists():
        return status
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        validators = data.get("validators", [])
        status.update(
            {
                "allowlist_loaded": bool(validators),
                "validator_count": len(validators),
                "updated_at": data.get("updated_at"),
                "netuid": data.get("netuid"),
            }
        )
    except Exception as exc:
        status["error"] = f"{exc.__class__.__name__}: {exc}"
    return status


def _debug_health() -> dict[str, Any]:
    return {
        "miner_id": MINER_ID,
        "hotkey": HOTKEY,
        "coldkey": COLDKEY,
        "slot_id": SLOT_ID,
        "slot_index": SLOT_INDEX,
        "quant": QUANT,
        "proof_mode": PROOF_MODE,
        "router_url": ROUTER_URL,
        "request_count": _request_count,
        "error_count": _error_count,
        "auth": _validator_auth_status(),
        "checked_at": utc_now(),
    }


async def _try_reserve_request() -> bool:
    global _active_requests
    async with _active_requests_lock:
        if _active_requests >= MAX_REQUESTS:
            return False
        _active_requests += 1
        return True


async def _release_request() -> None:
    global _active_requests
    async with _active_requests_lock:
        _active_requests = max(0, _active_requests - 1)


def _model_dump_with_extra(model: BaseModel) -> dict[str, Any]:
    data = model.model_dump(exclude_none=True)
    extra = getattr(model, "model_extra", None) or {}
    data.update(extra)
    return data


async def _raw_request_body_b64(request: Request) -> str:
    return base64.b64encode(await request.body()).decode("ascii")


def _validator_headers(request: Request) -> dict[str, str]:
    headers: dict[str, str] = {}
    for name in (
        "x-validator-hotkey",
        "x-validator-signature",
        "x-validator-timestamp",
    ):
        value = request.headers.get(name)
        if value:
            headers[name] = value
    return headers


@app.get("/health")
async def health() -> dict[str, Any]:
    result: dict[str, Any] = {
        "status": "ok",
        "model": MODEL_ID,
        "moe": False,
        "batch_mode": True,
        "supported_parameters": [
            "tools",
            "tool_choice",
            "parallel_tool_calls",
        ],
        "capture_backend": PROOF_MODE,
        "max_model_len": MAX_CONTEXT_LEN,
    }
    if GPU_NAME or GPU_UUIDS:
        result["hardware"] = {
            "gpu_name": GPU_NAME or "slot-proxy",
            "gpu_count": len(GPU_UUIDS) or 1,
            "vram_gb": SLOT_VRAM_GB,
            "compute_capability": "",
            "gpu_uuids": GPU_UUIDS,
        }
    if result["batch_mode"]:
        result["active_requests"] = _active_requests
        result["max_requests"] = MAX_REQUESTS
        result["kv_pool_tokens"] = MAX_CONTEXT_LEN
        result["kv_used_tokens"] = 0
        result["kv_free_tokens"] = MAX_CONTEXT_LEN
        result["kv_utilization_pct"] = 0
        result["can_accept_max_context"] = _active_requests < MAX_REQUESTS
        result["max_context"] = MAX_CONTEXT_LEN
        result["proof_pending"] = 0
        result["proof_max_pending"] = 0
    return result


def _models_payload() -> dict[str, Any]:
    return {
        "object": "list",
        "data": [
            {
                "id": MODEL_ID,
                "object": "model",
                "created": 0,
                "owned_by": "verathos",
                "quant": QUANT,
                "max_context_len": MAX_CONTEXT_LEN,
                "supported_parameters": [
                    "tools",
                    "tool_choice",
                    "parallel_tool_calls",
                ],
                "slot_id": SLOT_ID,
                "slot_index": SLOT_INDEX,
                "gpu_uuids": GPU_UUIDS,
            }
        ],
    }


@app.get("/slot")
async def slot_info() -> dict[str, Any]:
    result = await health()
    result.update(_debug_health())
    return result


@app.post("/receive")
async def receive_slot(raw_request: dict[str, Any]) -> dict[str, Any]:
    global _request_count
    _request_count += 1
    return {
        "ok": True,
        "received": True,
        "dispatched": False,
        "miner_id": MINER_ID,
        "hotkey": HOTKEY,
        "coldkey": COLDKEY,
        "slot_id": SLOT_ID,
        "slot_index": SLOT_INDEX,
        "payload": raw_request,
        "checked_at": utc_now(),
    }


@app.get("/models")
@app.get("/v1/models")
async def models() -> dict[str, Any]:
    return _models_payload()


@app.get("/model_spec")
async def model_spec() -> Response:
    cached = _cached_model_spec_response()
    if cached is not None:
        return cached

    target = MODEL_SPEC_URL or f"{ROUTER_URL}/v1/model_spec"
    try:
        client: httpx.AsyncClient = app.state.router_client
        response = await client.get(target, timeout=min(10.0, REQUEST_TIMEOUT_SECONDS))
    except Exception:
        return JSONResponse(status_code=503, content={"error": "Model not loaded"})
    content_type = response.headers.get("content-type", "application/json")
    media_type = content_type.split(";", 1)[0]
    if response.status_code == 200 and media_type == "application/json":
        _store_model_spec_cache(response.content, media_type)
    elif response.status_code in {404, 503} or response.status_code >= 500:
        cached = _cached_model_spec_response()
        if cached is not None:
            return cached
    return Response(
        content=response.content,
        status_code=response.status_code,
        media_type=media_type,
    )


def _store_model_spec_cache(content: bytes, media_type: str) -> None:
    global _model_spec_cache
    if not content:
        return
    _model_spec_cache = (content, media_type)
    try:
        MODEL_SPEC_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        MODEL_SPEC_CACHE_PATH.write_bytes(content)
    except Exception:
        pass


def _cached_model_spec_response() -> Response | None:
    global _model_spec_cache
    if _model_spec_cache is None and MODEL_SPEC_CACHE_PATH.exists():
        try:
            _model_spec_cache = (
                MODEL_SPEC_CACHE_PATH.read_bytes(),
                "application/json",
            )
        except Exception:
            _model_spec_cache = None
    if _model_spec_cache is None:
        return None
    content, media_type = _model_spec_cache
    return Response(
        content=content,
        status_code=200,
        media_type=media_type,
        headers={"x-slot-model-spec-cache": "hit"},
    )


def _hotkey_seed() -> bytes:
    if not COLDKEY or not HOTKEY:
        raise RuntimeError("COLDKEY and HOTKEY are required for identity challenge")
    keyfile = Path.home() / ".bittensor" / "wallets" / COLDKEY / "hotkeys" / HOTKEY
    data = json.loads(keyfile.read_text(encoding="utf-8"))
    secret_seed = data.get("secretSeed")
    if not isinstance(secret_seed, str) or not secret_seed:
        raise RuntimeError(f"hotkey secretSeed not found in {keyfile}")
    return bytes.fromhex(secret_seed.removeprefix("0x"))


def _load_evm_identity() -> dict[str, str]:
    global _evm_identity
    if _evm_identity is not None:
        return _evm_identity
    try:
        from eth_account import Account
        from eth_utils import keccak
    except ImportError as exc:
        raise RuntimeError("eth-account is required for identity challenge") from exc

    evm_private_key = (
        os.getenv("SLOT_EVM_PRIVATE_KEY")
        or os.getenv("VERATHOS_EVM_PRIVATE_KEY")
        or keccak(_hotkey_seed()).hex()
    )
    account = Account.from_key(evm_private_key)
    configured_address = os.getenv("SLOT_EVM_ADDRESS") or os.getenv("VERATHOS_EVM_ADDRESS")
    if configured_address and configured_address.lower() != account.address.lower():
        raise RuntimeError(
            "configured EVM address does not match configured/derived private key"
        )
    _evm_identity = {
        "address": account.address,
        "private_key": evm_private_key,
    }
    return _evm_identity


@app.post("/identity/challenge")
async def identity_challenge(body: IdentityChallengeBody):
    nonce_hex = body.nonce
    try:
        nonce = bytes.fromhex(nonce_hex)
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "Invalid hex nonce"})
    if len(nonce) != 32:
        return JSONResponse(status_code=400, content={"error": "Nonce must be 32 bytes (64 hex chars)"})

    try:
        identity = _load_evm_identity()
        from eth_account import Account
        from eth_account.messages import encode_defunct
    except Exception as exc:
        return JSONResponse(status_code=501, content={"error": str(exc)})

    address = identity["address"]
    message = nonce + bytes.fromhex(address[2:])
    signed = Account.sign_message(
        encode_defunct(primitive=message),
        private_key=identity["private_key"],
    )
    return {
        "address": address,
        "signature": signed.signature.hex(),
    }


@app.get("/tee/info")
async def tee_info() -> JSONResponse:
    return JSONResponse(status_code=404, content={"error": "TEE not enabled"})


@app.post("/tee/reattest")
async def tee_reattest(_request: Request) -> JSONResponse:
    return JSONResponse(status_code=404, content={"error": "TEE not enabled"})


@app.post("/tee/chat")
async def tee_chat(_body: TeeChatRequestBody, _request: Request) -> JSONResponse:
    return JSONResponse(status_code=404, content={"error": "TEE not enabled"})


def _sse(event: str, data: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def _sse_keepalive() -> str:
    return f": keepalive {int(time.time())}\n\n"


def _stream_status(phase: str, slot_started: float, **extra: Any) -> str:
    payload: dict[str, Any] = {
        "text": OPEN_STREAM_STATUS_TEXT,
        "phase": phase,
        "slot_id": SLOT_ID,
        "slot_index": SLOT_INDEX,
        "router_url": ROUTER_URL,
        "elapsed_ms": round((time.monotonic() - slot_started) * 1000.0, 3),
    }
    payload.update(extra)
    return _sse(OPEN_STREAM_STATUS_EVENT, payload)


async def _wait_for_open_stream_delay(slot_started: float) -> None:
    remaining = OPEN_STREAM_STATUS_DELAY_SECONDS - (time.monotonic() - slot_started)
    if remaining > 0:
        await asyncio.sleep(remaining)


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


def _message_text(message: dict[str, Any]) -> str:
    content = message.get("content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict) and isinstance(item.get("text"), str):
                parts.append(item["text"])
            elif isinstance(item, str):
                parts.append(item)
        return "\n".join(parts)
    return str(content)


def _chat_prompt(body: dict[str, Any]) -> str:
    messages = body.get("messages")
    if not isinstance(messages, list) or not messages:
        raise ValueError("messages must be a non-empty list")
    lines = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        role = str(message.get("role", "user"))
        text = _message_text(message).strip()
        if text:
            lines.append(f"{role}: {text}")
    if not lines:
        raise ValueError("messages contain no text content")
    return "\n".join(lines)


def _validate_verathos_nonce(body: dict[str, Any]) -> None:
    nonce = body.get("validator_nonce")
    if not isinstance(nonce, str):
        raise ValueError("validator_nonce is required")
    try:
        nonce_bytes = bytes.fromhex(nonce)
    except ValueError as exc:
        raise ValueError("validator_nonce must be hex") from exc
    if len(nonce_bytes) != 32:
        raise ValueError("validator_nonce must be 32 bytes")


def _rough_token_count(text: str) -> int:
    return max(1, len(text.split()))


async def _execute_verathos_request(
    prompt: str,
    body: dict[str, Any],
    *,
    verathos_path: str,
    raw_body_b64: str,
    validator_headers: dict[str, str],
    ) -> tuple[str, float, float, dict[str, Any] | None, list[dict[str, Any]] | None]:
    started = time.monotonic()
    timeout_seconds = float(body.get("timeout_seconds") or REQUEST_TIMEOUT_SECONDS)
    if not await _try_reserve_request():
        raise HTTPException(
            status_code=429,
            detail={
                "error": "slot is at max concurrent requests",
                "slot_id": SLOT_ID,
                "slot_index": SLOT_INDEX,
                "active_requests": _active_requests,
                "max_requests": MAX_REQUESTS,
            },
        )
    try:
        data, elapsed_ms = await _dispatch_payload(
            payload={
                "text": prompt,
                "messages": body.get("messages"),
                "max_new_tokens": body.get("max_new_tokens"),
                "temperature": body.get("temperature"),
                "do_sample": body.get("do_sample"),
                "enable_thinking": body.get("enable_thinking"),
                "validator_nonce": body.get("validator_nonce"),
                "verathos_endpoint": True,
                "verathos_path": verathos_path,
                "verathos_raw_body_b64": raw_body_b64,
                "verathos_headers": validator_headers,
            },
            timeout_seconds=timeout_seconds,
        )
    finally:
        await _release_request()
    return (
        _extract_response_text(data),
        elapsed_ms,
        started,
        _extract_verathos_done(data),
        _extract_verathos_events(data),
    )


def _extract_verathos_done(router_response: dict[str, Any]) -> dict[str, Any] | None:
    result = router_response.get("result")
    if isinstance(result, dict):
        for key in ("sse_done", "verathos_done", "done"):
            value = result.get(key)
            if isinstance(value, dict):
                return value
    return None


def _extract_verathos_events(router_response: dict[str, Any]) -> list[dict[str, Any]] | None:
    result = router_response.get("result")
    if not isinstance(result, dict):
        return None
    events = result.get("sse_events") or result.get("verathos_events")
    if not isinstance(events, list):
        return None
    normalized: list[dict[str, Any]] = []
    for item in events:
        if not isinstance(item, dict):
            continue
        event = item.get("event")
        data = item.get("data")
        if not isinstance(event, str):
            event = ""
        if not isinstance(data, dict):
            data = {"raw": data}
        normalized.append({"event": event, "data": data})
    return normalized or None


def _has_verathos_proof(done: dict[str, Any]) -> bool:
    commitment = done.get("commitment")
    proof_bundle = done.get("proof_bundle")
    if not isinstance(commitment, dict) or not commitment:
        return False
    if not isinstance(proof_bundle, dict) or not proof_bundle:
        return False
    layer_proofs = proof_bundle.get("layer_proofs")
    sampling_proofs = proof_bundle.get("sampling_proofs")
    return bool(layer_proofs or sampling_proofs or proof_bundle.get("commitment"))


async def _open_verathos_router_stream(
    prompt: str,
    body: dict[str, Any],
    *,
    verathos_path: str,
    raw_body_b64: str,
    validator_headers: dict[str, str],
) -> httpx.Response:
    global _request_count, _error_count
    timeout_seconds = float(body.get("timeout_seconds") or REQUEST_TIMEOUT_SECONDS)
    dispatch_data: dict[str, Any] = {
        "miner_id": MINER_ID,
        "slot_id": SLOT_ID,
        "payload": {
            "text": prompt,
            "messages": body.get("messages"),
            "max_new_tokens": body.get("max_new_tokens"),
            "temperature": body.get("temperature"),
            "do_sample": body.get("do_sample"),
            "enable_thinking": body.get("enable_thinking"),
            "validator_nonce": body.get("validator_nonce"),
            "verathos_endpoint": True,
            "verathos_path": verathos_path,
            "verathos_raw_body_b64": raw_body_b64,
            "verathos_headers": validator_headers,
        },
        "timeout_seconds": timeout_seconds,
    }
    dispatch = DispatchRequest.model_validate(dispatch_data)
    _request_count += 1
    if not await _try_reserve_request():
        raise HTTPException(
            status_code=429,
            detail={
                "error": "slot is at max concurrent requests",
                "slot_id": SLOT_ID,
                "slot_index": SLOT_INDEX,
                "active_requests": _active_requests,
                "max_requests": MAX_REQUESTS,
            },
        )
    try:
        client: httpx.AsyncClient = app.state.router_client
        request = client.build_request(
            "POST",
            f"{ROUTER_URL}/v1/dispatch/stream",
            json=dispatch.model_dump(),
            timeout=timeout_seconds,
        )
        response = await client.send(request, stream=True)
        if response.status_code >= 400:
            _error_count += 1
            body_text = (await response.aread()).decode("utf-8", errors="replace")
            await response.aclose()
            raise HTTPException(
                status_code=response.status_code,
                detail={
                    "error": "router stream dispatch failed",
                    "router_url": ROUTER_URL,
                    "status_code": response.status_code,
                    "body": body_text[:2000],
                },
            )
        return response
    except BaseException:
        await _release_request()
        raise


async def _relay_router_verathos_stream(
    response: httpx.Response,
    *,
    slot_started: float,
    router_stream_open_ms: float,
):
    global _error_count
    buffer = ""
    saw_done = False
    router_gpu_id = response.headers.get("X-Router-GPU-ID", "")
    router_mode = response.headers.get("X-Router-Mode", "streaming")
    router_elapsed_header = response.headers.get("X-Router-Elapsed-Ms")
    try:
        if STREAM_STATUS_TEXT:
            yield _sse(
                STREAM_STATUS_EVENT,
                {
                    "text": STREAM_STATUS_TEXT,
                    "phase": "slot_stream_open",
                    "slot_id": SLOT_ID,
                    "slot_index": SLOT_INDEX,
                    "router_url": ROUTER_URL,
                    "slot_to_router_stream_open_ms": round(router_stream_open_ms, 3),
                },
            )
        async for chunk in response.aiter_text():
            if not chunk:
                continue
            buffer += chunk
            while "\n\n" in buffer:
                block, buffer = buffer.split("\n\n", 1)
                event, data = _parse_sse_block(block)
                if not data:
                    continue
                event = event or str(data.get("event") or "message")
                if event == "done":
                    saw_done = True
                    data = _merge_transport_timing(
                        data,
                        {
                            "slot_to_router_stream_open_ms": round(router_stream_open_ms, 3),
                            "slot_total_to_done_ms": round((time.monotonic() - slot_started) * 1000.0, 3),
                            "slot_router_gpu_id": router_gpu_id,
                            "slot_router_mode": router_mode,
                            "slot_router_elapsed_header_ms": router_elapsed_header,
                        },
                    )
                    if REQUIRE_GPU_ZK_PROOF and not _has_verathos_proof(data):
                        _error_count += 1
                        yield _sse(
                            "error",
                            {
                                "error": "GPU did not return a real Verathos ZK proof",
                                "proof_required": True,
                            },
                        )
                        return
                elif event == "error":
                    _error_count += 1
                yield _sse(event, data)
                if event == "error":
                    return

        if buffer.strip():
            event, data = _parse_sse_block(buffer)
            if data:
                event = event or str(data.get("event") or "message")
                if event == "done":
                    saw_done = True
                    data = _merge_transport_timing(
                        data,
                        {
                            "slot_to_router_stream_open_ms": round(router_stream_open_ms, 3),
                            "slot_total_to_done_ms": round((time.monotonic() - slot_started) * 1000.0, 3),
                            "slot_router_gpu_id": router_gpu_id,
                            "slot_router_mode": router_mode,
                            "slot_router_elapsed_header_ms": router_elapsed_header,
                        },
                    )
                    if REQUIRE_GPU_ZK_PROOF and not _has_verathos_proof(data):
                        _error_count += 1
                        yield _sse(
                            "error",
                            {
                                "error": "GPU did not return a real Verathos ZK proof",
                                "proof_required": True,
                            },
                        )
                        return
                elif event == "error":
                    _error_count += 1
                yield _sse(event, data)

        if not saw_done:
            _error_count += 1
            yield _sse(
                "error",
                {
                    "error": "Router/GPU SSE stream did not include Verathos done proof data",
                    "proof_required": REQUIRE_GPU_ZK_PROOF,
                },
            )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _error_count += 1
        yield _sse(
            "error",
            {
                "error": str(exc),
                "exception": exc.__class__.__name__,
                "router_url": ROUTER_URL,
            },
        )
    finally:
        await response.aclose()
        await _release_request()


async def _open_status_then_relay_verathos_stream(
    prompt: str,
    body: dict[str, Any],
    *,
    verathos_path: str,
    raw_body_b64: str,
    validator_headers: dict[str, str],
    slot_started: float,
):
    global _error_count
    if OPEN_STREAM_STATUS_TEXT:
        await _wait_for_open_stream_delay(slot_started)
        yield _stream_status("slot_stream_open", slot_started)

    router_task = asyncio.create_task(
        _open_verathos_router_stream(
            prompt,
            body,
            verathos_path=verathos_path,
            raw_body_b64=raw_body_b64,
            validator_headers=validator_headers,
        )
    )
    try:
        keepalive_seconds = max(0.0, STREAM_KEEPALIVE_SECONDS)
        while not router_task.done():
            if keepalive_seconds <= 0:
                break
            try:
                await asyncio.wait_for(
                    asyncio.shield(router_task),
                    timeout=keepalive_seconds,
                )
            except asyncio.TimeoutError:
                yield _sse_keepalive()
        router_response = await router_task
        router_stream_open_ms = (time.monotonic() - slot_started) * 1000.0
    except HTTPException as exc:
        _error_count += 1
        detail = exc.detail if isinstance(exc.detail, dict) else {"detail": exc.detail}
        yield _sse(
            "error",
            {
                "error": "router stream dispatch failed",
                "status_code": exc.status_code,
                **detail,
            },
        )
        return
    except Exception as exc:
        _error_count += 1
        yield _sse(
            "error",
            {
                "error": str(exc),
                "exception": exc.__class__.__name__,
                "router_url": ROUTER_URL,
            },
        )
        return
    finally:
        if not router_task.done():
            router_task.cancel()
            with contextlib.suppress(BaseException):
                await router_task

    async for item in _relay_router_verathos_stream(
        router_response,
        slot_started=slot_started,
        router_stream_open_ms=router_stream_open_ms,
    ):
        yield item


async def _verathos_sse_stream(
    prompt: str,
    body: dict[str, Any],
    text: str,
    elapsed_ms: float,
    started: float,
    done_override: dict[str, Any] | None = None,
    events_override: list[dict[str, Any]] | None = None,
):
    if events_override:
        saw_done = False
        for item in events_override:
            event = str(item.get("event") or "")
            data = item.get("data")
            if not isinstance(data, dict):
                data = {"raw": data}
            event = event or str(data.get("event") or "message")
            if event == "done":
                saw_done = True
                data = dict(data)
                data.setdefault("output_text", text)
                if REQUIRE_GPU_ZK_PROOF and not _has_verathos_proof(data):
                    yield _sse(
                        "error",
                        {
                            "error": "GPU did not return a real Verathos ZK proof",
                            "proof_required": True,
                        },
                    )
                    return
            yield _sse(event, data)
            if event == "error":
                return
        if saw_done:
            return
        yield _sse(
            "error",
            {
                "error": "Router/GPU SSE stream did not include Verathos done proof data",
                "proof_required": REQUIRE_GPU_ZK_PROOF,
            },
        )
        return

    if text:
        yield _sse("token", {"text": text})

    if done_override is not None:
        done = dict(done_override)
        done.setdefault("output_text", text)
        if REQUIRE_GPU_ZK_PROOF and not _has_verathos_proof(done):
            yield _sse(
                "error",
                {
                    "error": "GPU did not return a real Verathos ZK proof",
                    "proof_required": True,
                },
            )
            return
        yield _sse("done", done)
        return

    yield _sse(
        "error",
        {
            "error": "Router/GPU response did not include Verathos done proof data",
            "proof_required": REQUIRE_GPU_ZK_PROOF,
        },
    )


@app.post("/chat")
async def chat(body: ChatRequestBody, request: Request):
    slot_started = time.monotonic()
    body_data = _model_dump_with_extra(body)
    try:
        _validate_verathos_nonce(body_data)
        prompt = _chat_prompt(body_data)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    body_data["validator_hotkey"] = getattr(request.state, "validator_hotkey", "")
    raw_body_b64 = await _raw_request_body_b64(request)
    validator_headers = _validator_headers(request)
    return StreamingResponse(
        _open_status_then_relay_verathos_stream(
            prompt,
            body_data,
            verathos_path="/chat",
            raw_body_b64=raw_body_b64,
            validator_headers=validator_headers,
            slot_started=slot_started,
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/inference")
async def inference(body: InferenceRequestBody, request: Request):
    slot_started = time.monotonic()
    body_data = _model_dump_with_extra(body)
    try:
        _validate_verathos_nonce(body_data)
        prompt = body_data.get("prompt")
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError("prompt must be a non-empty string")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    body_data["validator_hotkey"] = getattr(request.state, "validator_hotkey", "")
    raw_body_b64 = await _raw_request_body_b64(request)
    validator_headers = _validator_headers(request)
    return StreamingResponse(
        _open_status_then_relay_verathos_stream(
            prompt,
            body_data,
            verathos_path="/inference",
            raw_body_b64=raw_body_b64,
            validator_headers=validator_headers,
            slot_started=slot_started,
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/epoch/receipt")
async def receive_epoch_receipt(body: EpochReceiptBody):
    epoch = body.epoch_number
    receipt = body.model_dump()
    try:
        address = _load_evm_identity()["address"]
    except Exception:
        address = ""
    if address and body.miner_address.lower() != address.lower():
        return JSONResponse(
            status_code=403,
            content={"error": "Receipt address mismatch - this endpoint belongs to a different miner"},
        )
    if _receipt_store is None:
        raise HTTPException(status_code=503, detail="receipt store not ready")
    count = _receipt_store.add(epoch, receipt)
    _receipt_store.gc(epoch)
    return {
        "status": "accepted",
        "epoch": epoch,
        "count": count,
    }


@app.get("/epoch/{epoch_number}/receipts")
async def get_epoch_receipts(epoch_number: int) -> dict[str, Any]:
    if _receipt_store is None:
        raise HTTPException(status_code=503, detail="receipt store not ready")
    receipts = _receipt_store.get(epoch_number)
    return {
        "epoch": epoch_number,
        "receipt_count": len(receipts),
        "receipts": receipts,
    }


def _extract_response_text(router_response: dict[str, Any]) -> str:
    result = router_response.get("result")
    if isinstance(result, dict):
        text = result.get("text")
        if isinstance(text, str) and text.strip():
            return text

        response = result.get("response")
        if isinstance(response, dict):
            choices = response.get("choices")
            if isinstance(choices, list) and choices:
                first = choices[0]
                if isinstance(first, dict):
                    message = first.get("message")
                    if isinstance(message, dict):
                        content = message.get("content")
                        if isinstance(content, str) and content.strip():
                            return content

        received_payload = result.get("received_payload")
        if isinstance(received_payload, dict):
            received_text = received_payload.get("text")
            if isinstance(received_text, str) and received_text.strip():
                return f"GPU {router_response.get('gpu_id')} processed: {received_text}"

        return json.dumps(result, sort_keys=True)
    if isinstance(result, str) and result.strip():
        return result
    return json.dumps(router_response, sort_keys=True)


async def _dispatch_payload(
    *,
    payload: dict[str, Any],
    request_id: str | None = None,
    timeout_seconds: float | None = None,
) -> tuple[dict[str, Any], float]:
    global _request_count, _error_count
    _request_count += 1
    started = time.monotonic()
    dispatch_data: dict[str, Any] = {
        "miner_id": MINER_ID,
        "slot_id": SLOT_ID,
        "payload": payload,
        "timeout_seconds": timeout_seconds,
    }
    if request_id is not None:
        dispatch_data["request_id"] = request_id
    dispatch = DispatchRequest.model_validate(dispatch_data)
    try:
        client: httpx.AsyncClient = app.state.router_client
        response = await client.post(
            f"{ROUTER_URL}/v1/dispatch",
            json=dispatch.model_dump(),
        )
        response.raise_for_status()
        data = response.json()
        dispatch_response = DispatchResponse.model_validate(data)
        if dispatch_response.request_id != dispatch.request_id:
            raise ValueError(
                f"router returned request_id={dispatch_response.request_id!r}, "
                f"expected {dispatch.request_id!r}"
            )
        if dispatch_response.miner_id != MINER_ID:
            raise ValueError(
                f"router returned miner_id={dispatch_response.miner_id!r}, "
                f"expected {MINER_ID!r}"
            )
        if dispatch_response.slot_id != SLOT_ID:
            raise ValueError(
                f"router returned slot_id={dispatch_response.slot_id!r}, "
                f"expected {SLOT_ID!r}"
            )
    except httpx.HTTPStatusError as exc:
        _error_count += 1
        detail: object
        try:
            detail = exc.response.json()
        except ValueError:
            detail = exc.response.text
        raise HTTPException(status_code=exc.response.status_code, detail=detail) from exc
    except Exception as exc:
        _error_count += 1
        raise HTTPException(
            status_code=502,
            detail={
                "error": str(exc),
                "exception": exc.__class__.__name__,
                "router_url": ROUTER_URL,
            },
        ) from exc
    return dispatch_response.model_dump(), round((time.monotonic() - started) * 1000.0, 3)


async def _query_validator(query: ValidatorQuery) -> ValidatorResponse:
    data, _elapsed_ms = await _dispatch_payload(payload=query.model_dump())
    return ValidatorResponse(text=_extract_response_text(data))


@app.post("/query", response_model=ValidatorResponse, response_model_exclude_none=True)
async def query_slot(query: ValidatorQuery) -> ValidatorResponse:
    return await _query_validator(query)


@app.post("/entry/query")
async def entry_query_slot(request: EntrypointRequest) -> dict[str, Any]:
    query = ValidatorQuery.model_validate(request.payload)
    response = await _query_validator(query)
    return {"ok": True, "result": response.model_dump(exclude_none=True)}


@app.post("/request")
async def request_slot(raw_request: dict[str, Any]) -> dict[str, Any]:
    if "text" in raw_request and "payload" not in raw_request:
        try:
            query = ValidatorQuery.model_validate(raw_request)
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail=exc.errors()) from exc
        response = await _query_validator(query)
        return response.model_dump(exclude_none=True)

    try:
        request = SlotRequest.model_validate(raw_request)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    data, elapsed_ms = await _dispatch_payload(
        payload=request.payload,
        request_id=request.request_id,
        timeout_seconds=request.timeout_seconds,
    )
    return {
        "ok": True,
        "miner_id": MINER_ID,
        "slot_id": SLOT_ID,
        "request_id": request.request_id,
        "elapsed_ms": elapsed_ms,
        "router_response": data,
    }
