from __future__ import annotations

import collections
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse


DEFAULT_VALIDATORS_PATH = "/tmp/verathos_validators.json"
PUBLIC_ENDPOINTS = {
    "/health",
    "/slot",
    "/model_spec",
    "/models",
    "/v1/models",
    "/docs",
    "/openapi.json",
    "/identity/challenge",
    "/tee/info",
}
UNLIMITED_PUBLIC_ENDPOINTS = {
    "/health",
    "/slot",
    "/model_spec",
    "/identity/challenge",
}
FILE_RELOAD_INTERVAL_SECONDS = 60.0
MAX_CLOCK_SKEW_SECONDS = 60
PUBLIC_RATE_LIMIT = int(os.getenv("SLOT_PUBLIC_RATE_LIMIT", "60"))
PUBLIC_RATE_WINDOW_SECONDS = 60.0
_MAX_TRACKED_IPS = 10_000


def build_signing_message(method: str, path: str, body: bytes, timestamp: str) -> bytes:
    body_hash = hashlib.sha256(body).hexdigest()
    return f"{method}:{path}:{body_hash}:{timestamp}".encode("utf-8")


def verify_validator_request(
    *,
    method: str,
    path: str,
    body: bytes,
    hotkey_ss58: str,
    signature_hex: str,
    timestamp_str: str,
) -> tuple[bool, str]:
    try:
        timestamp = int(timestamp_str)
    except (TypeError, ValueError):
        return False, "invalid timestamp"

    skew = abs(int(time.time()) - timestamp)
    if skew > MAX_CLOCK_SKEW_SECONDS:
        return False, f"timestamp too old ({skew}s skew, max {MAX_CLOCK_SKEW_SECONDS}s)"

    try:
        signature = bytes.fromhex(signature_hex.removeprefix("0x"))
    except ValueError:
        return False, "malformed hex in signature"
    if len(signature) != 64:
        return False, f"signature must be 64 bytes, got {len(signature)}"

    message = build_signing_message(method, path, body, timestamp_str)
    try:
        try:
            from substrateinterface import Keypair  # type: ignore
        except Exception:
            from bittensor_wallet import Keypair  # type: ignore

        keypair = Keypair(ss58_address=hotkey_ss58)
        if not keypair.verify(message, signature):
            return False, "bad signature"
    except Exception as exc:
        return False, f"verification error: {exc}"

    return True, "ok"


class _PublicRateLimiter:
    def __init__(
        self,
        limit: int = PUBLIC_RATE_LIMIT,
        window: float = PUBLIC_RATE_WINDOW_SECONDS,
    ) -> None:
        self._limit = limit
        self._window = window
        self._hits: dict[str, collections.deque[float]] = {}

    def is_allowed(self, ip: str) -> bool:
        now = time.monotonic()
        bucket = self._hits.get(ip)
        if bucket is None:
            if len(self._hits) >= _MAX_TRACKED_IPS:
                oldest_ip = next(iter(self._hits))
                del self._hits[oldest_ip]
            bucket = collections.deque()
            self._hits[ip] = bucket

        cutoff = now - self._window
        while bucket and bucket[0] < cutoff:
            bucket.popleft()

        if len(bucket) >= self._limit:
            return False

        bucket.append(now)
        return True


class ValidatorAuthMiddleware(BaseHTTPMiddleware):
    """Verathos validator auth for slot proxy endpoints.

    Modes:
    - strict: official miner behavior; deny non-public requests until the
      validator allowlist exists and the request signature verifies.
    - auto: enforce when the allowlist has entries; temporarily allow when the
      allowlist has not been generated yet.
    - off: disable auth for local contract tests.
    """

    def __init__(self, app: Any, validators_path: str | None = None, mode: str | None = None):
        super().__init__(app)
        self._validators_path = Path(
            validators_path
            or os.environ.get("VERATHOS_VALIDATORS_PATH", DEFAULT_VALIDATORS_PATH)
        )
        self._mode = (mode or os.environ.get("SLOT_VALIDATOR_AUTH_MODE", "strict")).lower()
        self._allowed_ss58: set[str] = set()
        self._last_load = 0.0
        self._public_limiter = _PublicRateLimiter()
        self._load_validators(force=True)

    @property
    def status(self) -> dict[str, Any]:
        return {
            "mode": self._mode,
            "validators_path": str(self._validators_path),
            "validator_count": len(self._allowed_ss58),
            "allowlist_loaded": bool(self._allowed_ss58),
        }

    def _load_validators(self, *, force: bool = False) -> None:
        now = time.time()
        if not force and now - self._last_load < FILE_RELOAD_INTERVAL_SECONDS:
            return
        self._last_load = now

        if not self._validators_path.exists():
            self._allowed_ss58 = set()
            return

        try:
            data = json.loads(self._validators_path.read_text(encoding="utf-8"))
            validators = data.get("validators", [])
            self._allowed_ss58 = {
                item["hotkey_ss58"]
                for item in validators
                if isinstance(item, dict) and item.get("hotkey_ss58")
            }
        except Exception:
            self._allowed_ss58 = set()

    def _client_ip(self, request: Request) -> str:
        for header in ("cf-connecting-ip", "x-forwarded-for"):
            value = request.headers.get(header)
            if value:
                return value.split(",", 1)[0].strip()
        return request.client.host if request.client else "unknown"

    async def dispatch(self, request: Request, call_next):
        if os.environ.get("VERATHOS_NO_VALIDATOR_AUTH") == "1" or self._mode == "off":
            return await call_next(request)

        path = request.url.path
        if path in PUBLIC_ENDPOINTS:
            if (
                path not in UNLIMITED_PUBLIC_ENDPOINTS
                and PUBLIC_RATE_LIMIT > 0
                and not self._public_limiter.is_allowed(self._client_ip(request))
            ):
                return JSONResponse(
                    status_code=429,
                    content={"error": "Rate limit exceeded"},
                    headers={"Retry-After": str(int(PUBLIC_RATE_WINDOW_SECONDS))},
                )
            return await call_next(request)

        self._load_validators()

        if not self._allowed_ss58:
            if self._mode == "auto":
                request.state.validator_hotkey = ""
                return await call_next(request)
            return JSONResponse(
                status_code=503,
                content={"error": "Miner is starting up - validator allowlist not yet loaded"},
            )

        hotkey_ss58 = request.headers.get("x-validator-hotkey", "")
        signature_hex = request.headers.get("x-validator-signature", "")
        timestamp_str = request.headers.get("x-validator-timestamp", "")
        if not hotkey_ss58 or not signature_hex or not timestamp_str:
            return JSONResponse(
                status_code=401,
                content={
                    "error": (
                        "Missing validator auth headers "
                        "(X-Validator-Hotkey, X-Validator-Signature, X-Validator-Timestamp)"
                    )
                },
            )

        if hotkey_ss58 not in self._allowed_ss58:
            return JSONResponse(
                status_code=403,
                content={"error": "Hotkey is not a registered validator on this subnet"},
            )

        body = await request.body()
        ok, reason = verify_validator_request(
            method=request.method,
            path=path,
            body=body,
            hotkey_ss58=hotkey_ss58,
            signature_hex=signature_hex,
            timestamp_str=timestamp_str,
        )
        if not ok:
            return JSONResponse(
                status_code=401,
                content={"error": f"Validator signature verification failed: {reason}"},
            )

        request.state.validator_hotkey = hotkey_ss58
        return await call_next(request)
