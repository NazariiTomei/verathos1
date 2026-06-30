from __future__ import annotations

import json
import sqlite3
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_RECEIPT_DIR = Path("run/receipts")
DEFAULT_REGISTRATION_PATH = Path("run/slot_https_registration.json")
DEFAULT_LEASE_STATUS_PATH = Path("run/lease_watcher_status.json")
DEFAULT_STATUS_PATH = Path("run/receipt_integrity_status.json")
DEFAULT_STALE_SECONDS = 2 * 60 * 60


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def iso_from_unix(timestamp: int | float | None) -> str | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(float(timestamp), timezone.utc).isoformat().replace("+00:00", "Z")


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{path} did not contain a JSON object")
    return data


def load_public_slots(path: Path = DEFAULT_REGISTRATION_PATH) -> list[dict[str, Any]]:
    data = load_json(path)
    slots: list[dict[str, Any]] = []
    for slot in data.get("slots", []):
        if not isinstance(slot, dict):
            continue
        slot_id = slot.get("slot_id")
        if not isinstance(slot_id, str) or not slot_id:
            continue
        slots.append(
            {
                "slot_id": slot_id,
                "slot_index": slot.get("slot_index"),
                "miner_id": slot.get("miner_id") or data.get("miner_id"),
                "hotkey": slot.get("hotkey") or data.get("hotkey"),
                "endpoint": (slot.get("https_url") or slot.get("endpoint") or "").rstrip("/"),
            }
        )
    return sorted(slots, key=lambda item: int(item.get("slot_index") or 0))


def _as_int(value: Any, default: int | None = 0) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "ok"}
    return bool(value)


def _load_receipts(db_path: Path) -> list[dict[str, Any]]:
    receipts: list[dict[str, Any]] = []
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=1.0)
    try:
        for (raw,) in connection.execute("SELECT receipt_json FROM receipts"):
            try:
                receipt = json.loads(raw)
            except Exception:
                continue
            if isinstance(receipt, dict):
                receipts.append(receipt)
    finally:
        connection.close()
    return receipts


def _model_index_by_slot(lease_status: dict[str, Any] | None) -> dict[str, int]:
    indexes: dict[str, int] = {}
    if not isinstance(lease_status, dict):
        return indexes
    for slot in lease_status.get("slots", []):
        if not isinstance(slot, dict):
            continue
        slot_id = slot.get("slot_id")
        model_index = _as_int(slot.get("model_index"), default=None)
        if isinstance(slot_id, str) and model_index is not None:
            indexes[slot_id] = model_index
    return indexes


def _slot_status(row: dict[str, Any]) -> str:
    if row.get("missing_db"):
        return "bad"
    if row.get("latest_proof_failures", 0) > 0:
        return "bad"
    if row.get("latest_wrong_model_index", 0) > 0:
        return "bad"
    if row.get("latest_duplicate_signatures", 0) > 0:
        return "bad"
    if row.get("latest_cross_slot_duplicate_signatures", 0) > 0:
        return "bad"
    if row.get("total_receipts", 0) <= 0:
        return "warn"
    if row.get("stale"):
        return "warn"
    if row.get("duplicate_signatures", 0) > 0 or row.get("cross_slot_duplicate_signatures", 0) > 0:
        return "warn"
    return "ok"


def _summarize_slot(
    *,
    slot: dict[str, Any],
    receipt_dir: Path,
    expected_model_index: int | None,
    stale_seconds: int,
    now: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    slot_id = str(slot["slot_id"])
    db_path = receipt_dir / f"{slot_id}.db"
    base = {
        "slot_id": slot_id,
        "slot_index": slot.get("slot_index"),
        "miner_id": slot.get("miner_id"),
        "hotkey": slot.get("hotkey"),
        "endpoint": slot.get("endpoint"),
        "db_path": str(db_path),
        "expected_model_index": expected_model_index,
    }
    if not db_path.exists():
        row = {
            **base,
            "status": "bad",
            "missing_db": True,
            "total_receipts": 0,
            "last_error": "receipt database missing",
        }
        return row, []

    try:
        receipts = _load_receipts(db_path)
    except Exception as exc:
        row = {
            **base,
            "status": "bad",
            "missing_db": False,
            "total_receipts": 0,
            "last_error": f"{exc.__class__.__name__}: {exc}",
        }
        return row, []

    per_epoch: dict[int, dict[str, Any]] = {}
    signatures_seen: set[tuple[int, str]] = set()
    duplicate_signatures = 0
    signature_refs: list[dict[str, Any]] = []
    last_timestamp: int | None = None
    wrong_model_index_total = 0
    proof_failures_total = 0
    proof_requests_total = 0

    for receipt in receipts:
        epoch = _as_int(receipt.get("epoch_number"), default=None)
        if epoch is None:
            continue
        timestamp = _as_int(receipt.get("timestamp"), default=None)
        if timestamp is not None:
            last_timestamp = timestamp if last_timestamp is None else max(last_timestamp, timestamp)
        proof_requested = _as_bool(receipt.get("proof_requested"))
        proof_verified = _as_bool(receipt.get("proof_verified"))
        wrong_model_index = (
            expected_model_index is not None
            and _as_int(receipt.get("model_index"), default=None) != expected_model_index
        )
        if proof_requested:
            proof_requests_total += 1
        if proof_requested and not proof_verified:
            proof_failures_total += 1
        if wrong_model_index:
            wrong_model_index_total += 1

        epoch_row = per_epoch.setdefault(
            epoch,
            {
                "epoch": epoch,
                "total": 0,
                "proof_requests": 0,
                "proof_failures": 0,
                "wrong_model_index": 0,
                "duplicate_signatures": 0,
                "last_timestamp": None,
                "validators": Counter(),
            },
        )
        epoch_row["total"] += 1
        if proof_requested:
            epoch_row["proof_requests"] += 1
        if proof_requested and not proof_verified:
            epoch_row["proof_failures"] += 1
        if wrong_model_index:
            epoch_row["wrong_model_index"] += 1
        if timestamp is not None:
            current = epoch_row["last_timestamp"]
            epoch_row["last_timestamp"] = timestamp if current is None else max(current, timestamp)
        validator = str(receipt.get("validator_hotkey") or "")
        if validator:
            epoch_row["validators"][validator] += 1
        signature = str(receipt.get("validator_signature") or "")
        if signature:
            key = (epoch, signature)
            if key in signatures_seen:
                duplicate_signatures += 1
                epoch_row["duplicate_signatures"] += 1
            else:
                signatures_seen.add(key)
            signature_refs.append(
                {
                    "slot_id": slot_id,
                    "slot_index": slot.get("slot_index"),
                    "epoch": epoch,
                    "signature": signature,
                }
            )

    latest_epoch = max(per_epoch) if per_epoch else None
    latest = per_epoch.get(latest_epoch) if latest_epoch is not None else None
    latest_validator_counts = []
    if latest is not None:
        latest_validator_counts = [
            {"validator": validator, "count": count}
            for validator, count in latest["validators"].most_common(5)
        ]
    epoch_proofs = []
    for epoch in sorted(per_epoch, reverse=True)[:8]:
        item = per_epoch[epoch]
        proof_requests = int(item["proof_requests"])
        proof_failures = int(item["proof_failures"])
        epoch_last_timestamp = item["last_timestamp"]
        epoch_proofs.append(
            {
                "epoch": epoch,
                "receipts": int(item["total"]),
                "proof_requests": proof_requests,
                "proof_passes": max(0, proof_requests - proof_failures),
                "proof_failures": proof_failures,
                "wrong_model_index": int(item["wrong_model_index"]),
                "duplicate_signatures": int(item["duplicate_signatures"]),
                "last_timestamp": epoch_last_timestamp,
                "last_timestamp_iso": iso_from_unix(epoch_last_timestamp),
            }
        )

    row = {
        **base,
        "missing_db": False,
        "total_receipts": len(receipts),
        "epoch_count": len(per_epoch),
        "latest_epoch": latest_epoch,
        "latest_epoch_receipts": int(latest["total"]) if latest else 0,
        "latest_validator_count": len(latest["validators"]) if latest else 0,
        "latest_validator_counts": latest_validator_counts,
        "latest_proof_requests": int(latest["proof_requests"]) if latest else 0,
        "latest_proof_passes": (
            max(0, int(latest["proof_requests"]) - int(latest["proof_failures"]))
            if latest
            else 0
        ),
        "latest_proof_failures": int(latest["proof_failures"]) if latest else 0,
        "latest_wrong_model_index": int(latest["wrong_model_index"]) if latest else 0,
        "latest_duplicate_signatures": int(latest["duplicate_signatures"]) if latest else 0,
        "duplicate_signatures": duplicate_signatures,
        "cross_slot_duplicate_signatures": 0,
        "latest_cross_slot_duplicate_signatures": 0,
        "proof_requests": proof_requests_total,
        "proof_passes": max(0, proof_requests_total - proof_failures_total),
        "proof_failures": proof_failures_total,
        "historical_proof_failures": max(
            0,
            proof_failures_total - (int(latest["proof_failures"]) if latest else 0),
        ),
        "wrong_model_index": wrong_model_index_total,
        "epoch_proofs": epoch_proofs,
        "last_receipt_at": last_timestamp,
        "last_receipt_at_iso": iso_from_unix(last_timestamp),
        "stale": last_timestamp is not None and now - last_timestamp > stale_seconds,
        "last_error": None,
    }
    row["status"] = _slot_status(row)
    return row, signature_refs


def audit_receipts(
    *,
    slots: list[dict[str, Any]],
    receipt_dir: Path = DEFAULT_RECEIPT_DIR,
    lease_status: dict[str, Any] | None = None,
    stale_seconds: int = DEFAULT_STALE_SECONDS,
    poll_seconds: int | None = None,
    source: str = "audit",
) -> dict[str, Any]:
    now = int(time.time())
    model_indexes = _model_index_by_slot(lease_status)
    rows: list[dict[str, Any]] = []
    signature_refs: list[dict[str, Any]] = []

    for slot in slots:
        slot_id = str(slot.get("slot_id") or "")
        if not slot_id:
            continue
        expected_model_index = model_indexes.get(slot_id)
        if expected_model_index is None:
            expected_model_index = _as_int(slot.get("model_index"), default=None)
        row, refs = _summarize_slot(
            slot={**slot, "slot_id": slot_id},
            receipt_dir=receipt_dir,
            expected_model_index=expected_model_index,
            stale_seconds=stale_seconds,
            now=now,
        )
        rows.append(row)
        signature_refs.extend(refs)

    seen: dict[tuple[int, str], dict[str, Any]] = {}
    cross_duplicates: list[dict[str, Any]] = []
    cross_by_slot: Counter[str] = Counter()
    cross_by_slot_epoch: Counter[tuple[str, int]] = Counter()
    for ref in signature_refs:
        key = (int(ref["epoch"]), str(ref["signature"]))
        previous = seen.get(key)
        if previous is None:
            seen[key] = ref
            continue
        if previous["slot_id"] == ref["slot_id"]:
            continue
        cross_duplicates.append(
            {
                "epoch": key[0],
                "signature": key[1],
                "first_slot_id": previous["slot_id"],
                "first_slot_index": previous.get("slot_index"),
                "duplicate_slot_id": ref["slot_id"],
                "duplicate_slot_index": ref.get("slot_index"),
            }
        )
        cross_by_slot[str(previous["slot_id"])] += 1
        cross_by_slot[str(ref["slot_id"])] += 1
        cross_by_slot_epoch[(str(previous["slot_id"]), key[0])] += 1
        cross_by_slot_epoch[(str(ref["slot_id"]), key[0])] += 1

    for row in rows:
        slot_id = str(row["slot_id"])
        latest_epoch = row.get("latest_epoch")
        row["cross_slot_duplicate_signatures"] = cross_by_slot[slot_id]
        if latest_epoch is not None:
            row["latest_cross_slot_duplicate_signatures"] = cross_by_slot_epoch[(slot_id, int(latest_epoch))]
        row["status"] = _slot_status(row)

    status_counts = Counter(row["status"] for row in rows)
    if any(row["status"] == "bad" for row in rows):
        status = "bad"
    elif any(row["status"] == "warn" for row in rows):
        status = "warn"
    else:
        status = "ok"

    warnings = []
    if status_counts["bad"]:
        warnings.append(f"{status_counts['bad']} slot(s) have receipt integrity errors")
    if status_counts["warn"]:
        warnings.append(f"{status_counts['warn']} slot(s) have receipt warnings")
    if cross_duplicates:
        warnings.append(f"{len(cross_duplicates)} cross-slot duplicate receipt signatures detected")

    proof_failures_by_epoch: Counter[int] = Counter()
    for row in rows:
        for epoch in row.get("epoch_proofs", []):
            proof_failures_by_epoch[int(epoch["epoch"])] += int(epoch.get("proof_failures") or 0)

    next_check_at = int(now + poll_seconds) if poll_seconds else None
    latest_proof_requests = sum(int(row.get("latest_proof_requests") or 0) for row in rows)
    latest_proof_failures = sum(int(row.get("latest_proof_failures") or 0) for row in rows)
    return {
        "status": status,
        "source": source,
        "running": True,
        "checked_at": utc_now(),
        "next_check_at": next_check_at,
        "next_check_at_iso": iso_from_unix(next_check_at),
        "poll_seconds": poll_seconds,
        "receipt_dir": str(receipt_dir),
        "stale_seconds": stale_seconds,
        "slot_count": len(rows),
        "ok_slots": status_counts["ok"],
        "warn_slots": status_counts["warn"],
        "bad_slots": status_counts["bad"],
        "total_receipts": sum(int(row.get("total_receipts") or 0) for row in rows),
        "latest_epochs": sorted({row["latest_epoch"] for row in rows if row.get("latest_epoch") is not None}),
        "duplicate_signatures": sum(int(row.get("duplicate_signatures") or 0) for row in rows),
        "cross_slot_duplicate_signatures": len(cross_duplicates),
        "latest_proof_requests": latest_proof_requests,
        "latest_proof_passes": max(0, latest_proof_requests - latest_proof_failures),
        "latest_proof_failures": latest_proof_failures,
        "proof_failures": sum(int(row.get("proof_failures") or 0) for row in rows),
        "historical_proof_failures": sum(int(row.get("historical_proof_failures") or 0) for row in rows),
        "proof_failures_by_epoch": [
            {"epoch": epoch, "proof_failures": count}
            for epoch, count in sorted(proof_failures_by_epoch.items())
            if count > 0
        ],
        "wrong_model_index": sum(int(row.get("wrong_model_index") or 0) for row in rows),
        "warnings": warnings,
        "note": "Validator expected receipt counts are private; this watcher checks local persistence, duplicates, proof failures, model-index mismatches, and stale/no receipts.",
        "slots": rows,
        "cross_slot_duplicates": cross_duplicates[:50],
    }
