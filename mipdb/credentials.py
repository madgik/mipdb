import os
from typing import Dict, Any
import toml

CONFIG_PATH = "/home/config.toml"


def credentials_from_config() -> Dict[str, Any]:
    """Return a dict of credentials from *config.toml* or sensible fall‑backs."""

    try:
        return toml.load(os.getenv("CONFIG_PATH", CONFIG_PATH))
    except FileNotFoundError:
        return {
            "DUCKDB_PATH": "",
        }
