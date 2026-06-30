from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


_VALIDATOR_STRICT_CONFIG = ConfigDict(
    extra="forbid",
    frozen=True,
    strict=True,
    str_strip_whitespace=True,
)

_INTERNAL_CONTRACT_CONFIG = ConfigDict(
    extra="forbid",
    str_strip_whitespace=True,
)


class ValidatorQuery(BaseModel):
    model_config = _VALIDATOR_STRICT_CONFIG

    text: str = Field(min_length=1)


class ValidatorCitationSlice(BaseModel):
    model_config = _VALIDATOR_STRICT_CONFIG

    start: int = Field(ge=0)
    end: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_offsets(self) -> ValidatorCitationSlice:
        if self.end <= self.start:
            raise ValueError("citation slice end must be greater than start")
        return self


class ValidatorCitationRef(BaseModel):
    model_config = _VALIDATOR_STRICT_CONFIG

    receipt_id: str = Field(min_length=1)
    result_id: str = Field(min_length=1)
    slices: list[ValidatorCitationSlice] = Field(default_factory=list)


class ValidatorResponse(BaseModel):
    model_config = _VALIDATOR_STRICT_CONFIG

    text: str = Field(min_length=1, max_length=80_000)
    citations: list[ValidatorCitationRef] | None = Field(default=None, max_length=200)

    @model_validator(mode="after")
    def validate_total_evidence_segments(self) -> ValidatorResponse:
        total_segments = sum(len(citation.slices) if citation.slices else 1 for citation in self.citations or ())
        if total_segments > 400:
            raise ValueError("response citations exceed 400 materialized evidence segments")
        return self


class EntrypointRequest(BaseModel):
    payload: dict[str, Any] = Field(default_factory=dict)
    context: dict[str, Any] = Field(default_factory=dict)
    tool_config: dict[str, Any] | None = None


class SlotConfig(BaseModel):
    miner_id: str
    hotkey: str | None = None
    coldkey: str | None = None
    slot_id: str
    slot_index: int = Field(ge=1)
    host: str = "127.0.0.1"
    port: int = Field(ge=1, le=65535)
    url: str


class MinerConfig(BaseModel):
    miner_id: str
    hotkey: str | None = None
    coldkey: str | None = None
    slots: list[SlotConfig]


class GpuConfig(BaseModel):
    gpu_id: str
    host: str = "127.0.0.1"
    port: int = Field(ge=1, le=65535)
    url: str
    max_jobs: int = Field(default=1, ge=1)
    priority: int = Field(default=100, ge=0)
    metadata: dict[str, Any] = Field(default_factory=dict)


class RouterConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = Field(default=18080, ge=1, le=65535)
    url: str = "http://127.0.0.1:18080"
    monitor_interval_seconds: float = Field(default=2.0, gt=0)
    gpu_request_timeout_seconds: float = Field(default=300.0, gt=0)
    dispatch_wait_seconds: float = Field(default=240.0, gt=0)


class Topology(BaseModel):
    generated_at: str = Field(default_factory=utc_now)
    router: RouterConfig
    miners: list[MinerConfig]
    gpus: list[GpuConfig]

    @property
    def slots(self) -> list[SlotConfig]:
        return [slot for miner in self.miners for slot in miner.slots]


class SlotRequest(BaseModel):
    request_id: str = Field(default_factory=lambda: str(uuid4()))
    payload: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: float | None = Field(default=None, gt=0)


class DispatchRequest(BaseModel):
    miner_id: str
    slot_id: str
    request_id: str = Field(default_factory=lambda: str(uuid4()))
    payload: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: float | None = Field(default=None, gt=0)


class DispatchAttempt(BaseModel):
    model_config = _INTERNAL_CONTRACT_CONFIG

    gpu_id: str
    status: str
    error: str | None = None
    elapsed_ms: float | None = None


class DispatchResponse(BaseModel):
    model_config = _INTERNAL_CONTRACT_CONFIG

    request_id: str
    miner_id: str
    slot_id: str
    gpu_id: str
    result: dict[str, Any]
    attempts: list[DispatchAttempt]
    elapsed_ms: float


class GpuExecuteRequest(BaseModel):
    request_id: str
    miner_id: str
    slot_id: str
    slot_url: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: float | None = Field(default=None, gt=0)


class GpuExecuteResponse(BaseModel):
    model_config = _INTERNAL_CONTRACT_CONFIG

    ok: bool
    request_id: str
    miner_id: str
    slot_id: str
    slot_url: str | None = None
    gpu_id: str
    elapsed_ms: float
    result: dict[str, Any]


class GpuRegisterRequest(BaseModel):
    gpu_id: str
    url: HttpUrl
    host: str = "127.0.0.1"
    port: int = Field(ge=1, le=65535)
    max_jobs: int = Field(default=1, ge=1)
    priority: int = Field(default=100, ge=0)
    metadata: dict[str, Any] = Field(default_factory=dict)
