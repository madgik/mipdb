import os

import toml

CONFIG_PATH = "/home/config.toml"


def credentials_from_config():
    try:
        return toml.load(os.getenv("CONFIG_PATH", CONFIG_PATH))
    except FileNotFoundError:
        return {
            "DB_IP": "",
            "DB_PORT": "",
            "MONETDB_ADMIN_USERNAME": "",
            "MONETDB_LOCAL_USERNAME": "",
            "MONETDB_LOCAL_PASSWORD": "",
            "MONETDB_PUBLIC_USERNAME": "",
            "MONETDB_PUBLIC_PASSWORD": "",
            "DB_NAME": "",
            "SQLITE_DB_PATH": "",
        }
