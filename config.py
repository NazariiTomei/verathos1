from __future__ import annotations

import json
import os
from pathlib import Path

from miner_gpu_control.models import Topology


DEFAULT_TOPOLOGY_PATH = Path("topology.json")


def topology_path_from_env() -> Path:
    return Path(os.getenv("TOPOLOGY_PATH", str(DEFAULT_TOPOLOGY_PATH)))


def load_topology(path: str | Path | None = None) -> Topology:
    resolved = Path(path) if path is not None else topology_path_from_env()
    raw = json.loads(resolved.read_text(encoding="utf-8"))
    return Topology.model_validate(raw)


def router_url_from_env(default: str = "http://127.0.0.1:18080") -> str:
    return os.getenv("ROUTER_URL", default).rstrip("/")

