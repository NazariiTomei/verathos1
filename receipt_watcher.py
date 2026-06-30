from __future__ import annotations

import argparse
import fcntl
import json
import os
import time
from pathlib import Path
from typing import TextIO

from miner_gpu_control.receipt_audit import (
    DEFAULT_LEASE_STATUS_PATH,
    DEFAULT_RECEIPT_DIR,
    DEFAULT_REGISTRATION_PATH,
    DEFAULT_STALE_SECONDS,
    DEFAULT_STATUS_PATH,
    atomic_write_json,
    audit_receipts,
    load_json,
    load_public_slots,
    utc_now,
    iso_from_unix,
)


DEFAULT_LOCK_PATH = Path("run/receipt_watcher.lock")


def acquire_lock(path: Path) -> TextIO | None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = path.open("w", encoding="utf-8")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_file.close()
        return None
    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(f"{os.getpid()}\n")
    lock_file.flush()
    return lock_file


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Watch Verathos slot receipt integrity.")
    parser.add_argument("--registration", type=Path, default=Path(os.getenv("RECEIPT_REGISTRATION", str(DEFAULT_REGISTRATION_PATH))))
    parser.add_argument("--lease-status", type=Path, default=Path(os.getenv("RECEIPT_LEASE_STATUS", str(DEFAULT_LEASE_STATUS_PATH))))
    parser.add_argument("--receipt-dir", type=Path, default=Path(os.getenv("RECEIPT_DIR", str(DEFAULT_RECEIPT_DIR))))
    parser.add_argument("--status-path", type=Path, default=Path(os.getenv("RECEIPT_STATUS_PATH", str(DEFAULT_STATUS_PATH))))
    parser.add_argument("--lock-path", type=Path, default=Path(os.getenv("RECEIPT_LOCK_PATH", str(DEFAULT_LOCK_PATH))))
    parser.add_argument("--stale-seconds", type=int, default=int(os.getenv("RECEIPT_STALE_SECONDS", str(DEFAULT_STALE_SECONDS))))
    parser.add_argument("--poll-seconds", type=int, default=int(os.getenv("RECEIPT_POLL_SECONDS", "30")))
    parser.add_argument("--once", action="store_true", help="Run one audit and exit.")
    return parser.parse_args(argv)


def _blocked_payload(args: argparse.Namespace, message: str) -> dict[str, object]:
    return {
        "status": "blocked",
        "running": False,
        "pid": os.getpid(),
        "checked_at": utc_now(),
        "next_check_at": None,
        "next_check_at_iso": None,
        "poll_seconds": args.poll_seconds,
        "receipt_dir": str(args.receipt_dir),
        "status_path": str(args.status_path),
        "lock_path": str(args.lock_path),
        "last_error": message,
        "slots": [],
    }


def _error_payload(args: argparse.Namespace, exc: Exception) -> dict[str, object]:
    next_check_at = int(time.time() + args.poll_seconds)
    return {
        "status": "error",
        "running": True,
        "pid": os.getpid(),
        "checked_at": utc_now(),
        "next_check_at": next_check_at,
        "next_check_at_iso": iso_from_unix(next_check_at),
        "poll_seconds": args.poll_seconds,
        "receipt_dir": str(args.receipt_dir),
        "status_path": str(args.status_path),
        "lock_path": str(args.lock_path),
        "last_error": f"{exc.__class__.__name__}: {exc}",
        "slots": [],
    }


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    lock_file = acquire_lock(args.lock_path)
    if lock_file is None:
        payload = _blocked_payload(args, f"another receipt watcher already holds {args.lock_path}")
        atomic_write_json(args.status_path, payload)
        print(json.dumps(payload, sort_keys=True), flush=True)
        return 2

    try:
        while True:
            try:
                slots = load_public_slots(args.registration)
                lease_status = load_json(args.lease_status) if args.lease_status.exists() else None
                payload = audit_receipts(
                    slots=slots,
                    receipt_dir=args.receipt_dir,
                    lease_status=lease_status,
                    stale_seconds=args.stale_seconds,
                    poll_seconds=args.poll_seconds,
                    source="receipt_watcher",
                )
                payload.update(
                    {
                        "running": True,
                        "pid": os.getpid(),
                        "status_path": str(args.status_path),
                        "lock_path": str(args.lock_path),
                    }
                )
            except Exception as exc:
                payload = _error_payload(args, exc)
            atomic_write_json(args.status_path, payload)
            print(json.dumps(payload, sort_keys=True), flush=True)
            if args.once:
                return 0 if payload.get("status") not in {"bad", "error"} else 1
            time.sleep(max(10, int(args.poll_seconds)))
    finally:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
