from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

import bittensor as bt
import requests
from eth_abi import decode
from eth_utils import keccak

from miner_gpu_control.verathos_auth import DEFAULT_VALIDATORS_PATH


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_STATUS_PATH = ROOT_DIR / "run" / "validator_allowlist_status.json"


def _selector(signature: str) -> bytes:
    return keccak(text=signature)[:4]


def _rpc(url: str, method: str, params: list[Any]) -> Any:
    response = requests.post(
        url,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        raise RuntimeError(payload["error"])
    return payload["result"]


def _call(rpc_url: str, contract: str, data: bytes) -> bytes:
    result = _rpc(
        rpc_url,
        "eth_call",
        [{"to": contract, "data": "0x" + data.hex()}, "latest"],
    )
    return bytes.fromhex(result.removeprefix("0x"))


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _read_min_validator_stake_tao(chain_config_path: Path, rpc_url: str | None) -> float:
    if not rpc_url:
        return 0.0
    data = _read_json(chain_config_path)
    contract = data.get("validator_registry_address")
    if not contract:
        return 0.0
    raw = _call(rpc_url, contract, _selector("minValidatorStake()"))
    return float(decode(["uint256"], raw)[0]) / 1e9


def _as_int(value: Any) -> int:
    if hasattr(value, "item"):
        return int(value.item())
    return int(value)


def _as_float(value: Any) -> float:
    if hasattr(value, "item"):
        return float(value.item())
    return float(value)


def _manual_validators(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def refresh_allowlist(args: argparse.Namespace) -> dict[str, Any]:
    min_stake = 0.0
    min_stake_error = None
    try:
        min_stake = _read_min_validator_stake_tao(args.chain_config, args.rpc_url)
    except Exception as exc:
        min_stake_error = f"{exc.__class__.__name__}: {exc}"

    subtensor = bt.Subtensor(network=args.subtensor_network)
    metagraph = subtensor.metagraph(netuid=args.netuid)
    count = _as_int(metagraph.n)
    permits = list(metagraph.validator_permit) if hasattr(metagraph, "validator_permit") else []
    stakes = metagraph.S if hasattr(metagraph, "S") else metagraph.stake

    validators: list[dict[str, Any]] = []
    for uid in range(count):
        has_permit = bool(permits[uid]) if uid < len(permits) else False
        stake = _as_float(stakes[uid])
        if has_permit and stake >= min_stake:
            validators.append(
                {
                    "uid": uid,
                    "hotkey_ss58": metagraph.hotkeys[uid],
                    "stake": stake,
                }
            )

    existing = {item["hotkey_ss58"] for item in validators}
    for ss58 in _manual_validators(args.allow_validators):
        if ss58 not in existing:
            validators.append({"uid": -1, "hotkey_ss58": ss58, "stake": 0.0})

    payload = {
        "updated_at": int(time.time()),
        "netuid": args.netuid,
        "subtensor_network": args.subtensor_network,
        "min_validator_stake_tao": min_stake,
        "validators": validators,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = args.output.with_suffix(args.output.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(tmp_path, args.output)

    return {
        "status": "ok",
        "updated_at": payload["updated_at"],
        "netuid": args.netuid,
        "subtensor_network": args.subtensor_network,
        "validators_path": str(args.output),
        "validator_count": len(validators),
        "min_validator_stake_tao": min_stake,
        "min_validator_stake_error": min_stake_error,
    }


def write_status(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp_path, path)


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Refresh Verathos validator allowlist for slot auth.")
    parser.add_argument("--netuid", type=int, default=int(os.getenv("VERATHOS_NETUID", "96")))
    parser.add_argument("--subtensor-network", default=os.getenv("BT_SUBTENSOR_NETWORK", "finney"))
    parser.add_argument("--rpc-url", default=os.getenv("VERATHOS_RPC_URL", "https://lite.chain.opentensor.ai"))
    parser.add_argument(
        "--chain-config",
        type=Path,
        default=Path(os.getenv("VERATHOS_CHAIN_CONFIG", str(ROOT_DIR / "verathos" / "chain_config_mainnet.json"))),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(os.getenv("VERATHOS_VALIDATORS_PATH", DEFAULT_VALIDATORS_PATH)),
    )
    parser.add_argument("--status-path", type=Path, default=DEFAULT_STATUS_PATH)
    parser.add_argument("--interval-seconds", type=float, default=float(os.getenv("VALIDATOR_ALLOWLIST_INTERVAL_SECONDS", "300")))
    parser.add_argument("--allow-validators", default=os.getenv("VERATHOS_ALLOW_VALIDATORS", ""))
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)

    while True:
        started = time.time()
        try:
            status = refresh_allowlist(args)
            status["duration_seconds"] = round(time.time() - started, 3)
        except Exception as exc:
            status = {
                "status": "error",
                "updated_at": int(time.time()),
                "netuid": args.netuid,
                "subtensor_network": args.subtensor_network,
                "validators_path": str(args.output),
                "error": f"{exc.__class__.__name__}: {exc}",
                "duration_seconds": round(time.time() - started, 3),
            }
        write_status(args.status_path, status)
        print(json.dumps(status, sort_keys=True), flush=True)
        if args.once:
            return 0 if status.get("status") == "ok" else 1
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    raise SystemExit(run())
