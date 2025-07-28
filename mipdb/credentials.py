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
            "DB_IP": "",
            "DB_PORT": "",
            "MONETDB_ENABLED": False,
            "MONETDB_ADMIN_USERNAME": "",
            "MONETDB_LOCAL_USERNAME": "",
            "MONETDB_LOCAL_PASSWORD": "",
            "MONETDB_PUBLIC_USERNAME": "",
            "MONETDB_PUBLIC_PASSWORD": "",
            "DB_NAME": "",
            "SQLITE_DB_PATH": "",
        }
