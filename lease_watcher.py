from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO

import requests
from eth_abi import decode, encode
from eth_account import Account
from eth_utils import keccak


DEFAULT_RPC_URL = "https://lite.chain.opentensor.ai"
DEFAULT_CHAIN_ID = 964
DEFAULT_NETUID = 96
DEFAULT_MODEL_ID = "Qwen/Qwen3.5-9B"
DEFAULT_QUANT = "fp16"
DEFAULT_RENEW_INTERVAL_SECONDS = 12 * 60 * 60
LEASE_SECONDS = 24 * 60 * 60
DEFAULT_LOCK_PATH = Path("run/lease_watcher.lock")


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


def blocked_payload(args: argparse.Namespace, message: str) -> dict[str, Any]:
    return {
        "status": "blocked",
        "running": False,
        "pid": os.getpid(),
        "checked_at": utc_now(),
        "next_check_at": None,
        "next_check_at_iso": None,
        "wallet": args.wallet,
        "hotkey": args.hotkey,
        "uid": args.uid,
        "netuid": args.netuid,
        "model_id": args.model_id,
        "quant": args.quant,
        "renew_interval_seconds": args.renew_interval_seconds,
        "renew_when_remaining_seconds": max(0, LEASE_SECONDS - args.renew_interval_seconds),
        "lease_seconds": LEASE_SECONDS,
        "poll_seconds": args.poll_seconds,
        "status_path": str(args.status_path),
        "lock_path": str(args.lock_path),
        "last_error": message,
        "slots": [],
    }


def rpc(url: str, method: str, params: list[Any]) -> Any:
    last_error: Exception | None = None
    for attempt in range(1, 8):
        try:
            response = requests.post(
                url,
                json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                timeout=30,
            )
            if response.status_code == 429:
                raise RuntimeError("rpc rate limited")
            response.raise_for_status()
            payload = response.json()
            if "error" in payload:
                raise RuntimeError(payload["error"])
            return payload["result"]
        except Exception as exc:
            last_error = exc
            if attempt == 7:
                break
            time.sleep(min(2**attempt, 30))
    raise RuntimeError(f"{method} failed after retries: {last_error}")


def selector(signature: str) -> bytes:
    return keccak(text=signature)[:4]


def call(rpc_url: str, contract: str, data: bytes) -> bytes:
    result = rpc(
        rpc_url,
        "eth_call",
        [{"to": contract, "data": "0x" + data.hex()}, "latest"],
    )
    return bytes.fromhex(result.removeprefix("0x"))


def send_tx(
    *,
    rpc_url: str,
    chain_id: int,
    account: Any,
    private_key: bytes,
    contract: str,
    data: bytes,
) -> str:
    nonce = int(rpc(rpc_url, "eth_getTransactionCount", [account.address, "pending"]), 16)
    gas_price = int(rpc(rpc_url, "eth_gasPrice", []), 16)
    tx_base = {
        "from": account.address,
        "to": contract,
        "value": "0x0",
        "data": "0x" + data.hex(),
    }
    estimated = int(rpc(rpc_url, "eth_estimateGas", [tx_base]), 16)
    tx = {
        "chainId": chain_id,
        "nonce": nonce,
        "to": contract,
        "value": 0,
        "data": data,
        "gas": max(estimated + 30_000, int(estimated * 1.25)),
        "gasPrice": gas_price,
    }
    signed = Account.sign_transaction(tx, private_key)
    raw = getattr(signed, "rawTransaction", None) or signed.raw_transaction
    tx_hash = getattr(signed, "hash", None)
    tx_hash_hex = tx_hash.hex() if tx_hash is not None else ""
    if tx_hash_hex and not tx_hash_hex.startswith("0x"):
        tx_hash_hex = "0x" + tx_hash_hex
    raw_hex = "0x" + raw.hex()
    last_error: Exception | None = None
    for attempt in range(1, 8):
        try:
            response = requests.post(
                rpc_url,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_sendRawTransaction",
                    "params": [raw_hex],
                },
                timeout=30,
            )
            if response.status_code == 429:
                raise RuntimeError("rpc rate limited")
            response.raise_for_status()
            payload = response.json()
            if "error" in payload:
                message = str(payload["error"])
                if "already known" in message.lower() and tx_hash_hex:
                    return tx_hash_hex
                raise RuntimeError(payload["error"])
            return payload["result"]
        except Exception as exc:
            last_error = exc
            if attempt == 7:
                break
            time.sleep(min(2**attempt, 30))
    raise RuntimeError(f"eth_sendRawTransaction failed after retries: {last_error}")


def wait_receipt(rpc_url: str, tx_hash: str) -> dict[str, Any]:
    for _ in range(120):
        receipt = rpc(rpc_url, "eth_getTransactionReceipt", [tx_hash])
        if receipt:
            status = int(receipt.get("status", "0x0"), 16)
            if status != 1:
                raise RuntimeError(f"tx failed: {tx_hash}")
            return receipt
        time.sleep(2)
    raise TimeoutError(f"timed out waiting for {tx_hash}")


def load_hotkey_seed(wallet: str, hotkey: str) -> bytes:
    keyfile = Path.home() / ".bittensor" / "wallets" / wallet / "hotkeys" / hotkey
    data = json.loads(keyfile.read_text(encoding="utf-8"))
    secret_seed = data.get("secretSeed")
    if not isinstance(secret_seed, str) or not secret_seed:
        raise RuntimeError(f"secretSeed not found in {keyfile}")
    return bytes.fromhex(secret_seed.removeprefix("0x"))


def evm_credentials(wallet: str, hotkey: str) -> tuple[bytes, Any]:
    private_key = keccak(load_hotkey_seed(wallet, hotkey))
    return private_key, Account.from_key(private_key)


def ss58_mirror(h160_address: str, ss58_format: int = 42) -> str:
    h160 = bytes.fromhex(h160_address.removeprefix("0x"))
    account_id = hashlib.blake2b(b"evm:" + h160, digest_size=32).digest()
    return ss58_encode(account_id, ss58_format)


def ss58_encode(public_key: bytes, ss58_format: int) -> str:
    if ss58_format < 64:
        prefix = bytes([ss58_format])
    else:
        first = ((ss58_format & 0b0000000011111100) >> 2) | 0b01000000
        second = (ss58_format >> 8) | ((ss58_format & 0b0000000000000011) << 6)
        prefix = bytes([first, second])
    payload = prefix + public_key
    checksum = hashlib.blake2b(b"SS58PRE" + payload, digest_size=64).digest()[:2]
    return base58_encode(payload + checksum)


def base58_encode(data: bytes) -> str:
    alphabet = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    n = int.from_bytes(data, "big")
    result = bytearray()
    while n:
        n, rem = divmod(n, 58)
        result.append(alphabet[rem])
    for byte in data:
        if byte == 0:
            result.append(alphabet[0])
        else:
            break
    return bytes(reversed(result)).decode("ascii")


def read_chain_config(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not data.get("miner_registry_address"):
        raise RuntimeError(f"miner_registry_address missing in {path}")
    return data


def public_slots(path: Path, limit: int) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    slots = []
    for slot in data.get("slots", [])[:limit]:
        endpoint = (slot.get("https_url") or "").rstrip("/")
        slot_id = slot.get("slot_id")
        if endpoint and slot_id:
            slots.append(
                {
                    "slot_index": slot.get("slot_index"),
                    "slot_id": slot_id,
                    "endpoint": endpoint,
                }
            )
    return slots


def evm_registered(rpc_url: str, contract: str, address: str) -> bool:
    data = selector("evmRegistered(address)") + encode(["address"], [address])
    return bool(decode(["bool"], call(rpc_url, contract, data))[0])


def miner_models(rpc_url: str, contract: str, address: str) -> list[tuple[Any, ...]]:
    data = selector("getMinerModels(address)") + encode(["address"], [address])
    raw = call(rpc_url, contract, data)
    return list(decode(["(string,string,bytes32,string,uint32,uint64,bool)[]"], raw)[0])


def renew_model_data(index: int) -> bytes:
    return selector("renewModel(uint256)") + encode(["uint256"], [index])


def build_rows(
    *,
    slots: list[dict[str, Any]],
    models: list[tuple[Any, ...]],
    model_id: str,
    quant: str,
    renew_interval_seconds: int,
) -> list[dict[str, Any]]:
    by_endpoint = {
        model[1].rstrip("/"): (index, model)
        for index, model in enumerate(models)
        if model[0] == model_id and model[3] == quant
    }
    now = int(time.time())
    renew_when_remaining = max(0, LEASE_SECONDS - renew_interval_seconds)
    rows = []
    for slot in slots:
        match = by_endpoint.get(slot["endpoint"].rstrip("/"))
        if match is None:
            rows.append(
                {
                    **slot,
                    "registered": False,
                    "active": False,
                    "model_index": None,
                    "expires_at": None,
                    "expires_at_iso": None,
                    "remaining_seconds": None,
                    "next_renew_at": None,
                    "next_renew_at_iso": None,
                    "due": False,
                }
            )
            continue
        model_index, model = match
        expires_at = int(model[5])
        remaining = expires_at - now
        next_renew_at = expires_at - renew_when_remaining
        rows.append(
            {
                **slot,
                "registered": True,
                "active": bool(model[6]),
                "model_index": model_index,
                "model": model[0],
                "quant": model[3],
                "max_context_len": model[4],
                "expires_at": expires_at,
                "expires_at_iso": iso_from_unix(expires_at),
                "remaining_seconds": remaining,
                "next_renew_at": next_renew_at,
                "next_renew_at_iso": iso_from_unix(next_renew_at),
                "due": bool(model[6]) and remaining <= renew_when_remaining,
            }
        )
    return rows


def run_check(args: argparse.Namespace, *, execute: bool) -> dict[str, Any]:
    chain = read_chain_config(args.chain_config)
    contract = chain["miner_registry_address"]
    chain_id = int(chain.get("chain_id") or DEFAULT_CHAIN_ID)
    slots = public_slots(args.registration, args.slots)
    private_key, account = evm_credentials(args.wallet, args.hotkey)
    balance_wei = int(rpc(args.rpc_url, "eth_getBalance", [account.address, "latest"]), 16)
    registered = evm_registered(args.rpc_url, contract, account.address)
    models = miner_models(args.rpc_url, contract, account.address) if registered else []
    rows = build_rows(
        slots=slots,
        models=models,
        model_id=args.model_id,
        quant=args.quant,
        renew_interval_seconds=args.renew_interval_seconds,
    )
    txs = []
    due_rows = [row for row in rows if row["due"] and row["model_index"] is not None]
    status = "ok"
    last_error = None

    if balance_wei == 0:
        status = "blocked"
        last_error = "no_evm_gas_balance"
    elif not registered:
        status = "blocked"
        last_error = "evm_not_registered"
    elif execute and due_rows:
        status = "renewing"
        for row in due_rows:
            tx_hash = send_tx(
                rpc_url=args.rpc_url,
                chain_id=chain_id,
                account=account,
                private_key=private_key,
                contract=contract,
                data=renew_model_data(int(row["model_index"])),
            )
            receipt = wait_receipt(args.rpc_url, tx_hash)
            txs.append(
                {
                    "slot_index": row["slot_index"],
                    "slot_id": row["slot_id"],
                    "model_index": row["model_index"],
                    "tx_hash": tx_hash,
                    "block_number": int(receipt["blockNumber"], 16),
                    "renewed_at": utc_now(),
                }
            )
        models = miner_models(args.rpc_url, contract, account.address)
        rows = build_rows(
            slots=slots,
            models=models,
            model_id=args.model_id,
            quant=args.quant,
            renew_interval_seconds=args.renew_interval_seconds,
        )
        status = "ok"

    remaining_values = [
        int(row["remaining_seconds"])
        for row in rows
        if row.get("remaining_seconds") is not None and row.get("active")
    ]
    next_due_values = [
        int(row["next_renew_at"])
        for row in rows
        if row.get("next_renew_at") is not None and row.get("active")
    ]
    next_check_at = int(time.time() + args.poll_seconds)
    return {
        "status": status,
        "running": True,
        "pid": os.getpid(),
        "checked_at": utc_now(),
        "next_check_at": next_check_at,
        "next_check_at_iso": iso_from_unix(next_check_at),
        "wallet": args.wallet,
        "hotkey": args.hotkey,
        "uid": args.uid,
        "netuid": args.netuid,
        "model_id": args.model_id,
        "quant": args.quant,
        "rpc_url": args.rpc_url,
        "evm_address": account.address,
        "evm_mirror_ss58": ss58_mirror(account.address),
        "balance_tao": balance_wei / 10**18,
        "evm_registered": registered,
        "model_count": len(models),
        "matched_slots": sum(1 for row in rows if row.get("registered")),
        "due_count": sum(1 for row in rows if row.get("due")),
        "min_remaining_seconds": min(remaining_values) if remaining_values else None,
        "next_renew_at": min(next_due_values) if next_due_values else None,
        "next_renew_at_iso": iso_from_unix(min(next_due_values)) if next_due_values else None,
        "renew_interval_seconds": args.renew_interval_seconds,
        "renew_when_remaining_seconds": max(0, LEASE_SECONDS - args.renew_interval_seconds),
        "lease_seconds": LEASE_SECONDS,
        "poll_seconds": args.poll_seconds,
        "status_path": str(args.status_path),
        "lock_path": str(args.lock_path),
        "last_error": last_error,
        "last_transactions": txs,
        "slots": rows,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Renew Verathos MinerRegistry slot leases.")
    parser.add_argument("--wallet", default=os.getenv("LEASE_WALLET", "96"))
    parser.add_argument("--hotkey", default=os.getenv("LEASE_HOTKEY", "96-1"))
    parser.add_argument("--uid", type=int, default=int(os.getenv("LEASE_UID", "143")))
    parser.add_argument("--netuid", type=int, default=int(os.getenv("LEASE_NETUID", str(DEFAULT_NETUID))))
    parser.add_argument("--rpc-url", default=os.getenv("VERATHOS_RPC_URL", DEFAULT_RPC_URL))
    parser.add_argument("--chain-config", type=Path, default=Path(os.getenv("LEASE_CHAIN_CONFIG", "verathos/chain_config_mainnet.json")))
    parser.add_argument("--registration", type=Path, default=Path(os.getenv("LEASE_REGISTRATION", "run/slot_https_registration.json")))
    parser.add_argument("--status-path", type=Path, default=Path(os.getenv("LEASE_STATUS_PATH", "run/lease_watcher_status.json")))
    parser.add_argument("--lock-path", type=Path, default=Path(os.getenv("LEASE_LOCK_PATH", str(DEFAULT_LOCK_PATH))))
    parser.add_argument("--slots", type=int, default=int(os.getenv("LEASE_SLOTS", "5")))
    parser.add_argument("--model-id", default=os.getenv("LEASE_MODEL_ID", DEFAULT_MODEL_ID))
    parser.add_argument("--quant", default=os.getenv("LEASE_QUANT", DEFAULT_QUANT))
    parser.add_argument("--renew-interval-seconds", type=int, default=int(os.getenv("LEASE_RENEW_INTERVAL_SECONDS", str(DEFAULT_RENEW_INTERVAL_SECONDS))))
    parser.add_argument("--poll-seconds", type=int, default=int(os.getenv("LEASE_POLL_SECONDS", "300")))
    parser.add_argument("--execute", action="store_true", help="Send renewModel transactions when leases are due.")
    parser.add_argument("--once", action="store_true", help="Run one check and exit.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    lock_file = acquire_lock(args.lock_path)
    if lock_file is None:
        payload = blocked_payload(args, f"another lease watcher already holds {args.lock_path}")
        atomic_write_json(args.status_path, payload)
        print(json.dumps(payload, sort_keys=True), flush=True)
        return 2

    try:
        while True:
            try:
                payload = run_check(args, execute=args.execute)
            except Exception as exc:
                payload = {
                    "status": "error",
                    "running": True,
                    "pid": os.getpid(),
                    "checked_at": utc_now(),
                    "next_check_at": int(time.time() + args.poll_seconds),
                    "next_check_at_iso": iso_from_unix(time.time() + args.poll_seconds),
                    "wallet": args.wallet,
                    "hotkey": args.hotkey,
                    "uid": args.uid,
                    "netuid": args.netuid,
                    "model_id": args.model_id,
                    "quant": args.quant,
                    "renew_interval_seconds": args.renew_interval_seconds,
                    "renew_when_remaining_seconds": max(0, LEASE_SECONDS - args.renew_interval_seconds),
                    "lease_seconds": LEASE_SECONDS,
                    "poll_seconds": args.poll_seconds,
                    "status_path": str(args.status_path),
                    "lock_path": str(args.lock_path),
                    "last_error": f"{exc.__class__.__name__}: {exc}",
                    "slots": [],
                }
            atomic_write_json(args.status_path, payload)
            print(json.dumps(payload, sort_keys=True), flush=True)
            if args.once:
                return 0 if payload.get("status") != "error" else 1
            time.sleep(max(30, int(args.poll_seconds)))
    finally:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
