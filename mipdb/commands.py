from __future__ import annotations

import ipaddress
from pathlib import Path
from typing import Any, Dict, Optional

import click as cl

from mipdb.credentials import credentials_from_config
from mipdb.logger import LOGGER
from mipdb.monetdb.monetdb import MonetDB
from mipdb.sqlite.sqlite import SQLiteDB
from mipdb.reader import JsonFileReader
from mipdb.usecases import (
    AddDataModel,
    AddPropertyToDataModel,
    AddPropertyToDataset,
    Cleanup,
    DeleteDataModel,
    DeleteDataset,
    DisableDataModel,
    DisableDataset,
    EnableDataModel,
    EnableDataset,
    ImportCSV,
    InitDB,
    ListDataModels,
    ListDatasets,
    RemovePropertyFromDataModel,
    RemovePropertyFromDataset,
    TagDataModel,
    TagDataset,
    UntagDataModel,
    UntagDataset,
    ValidateDataModel,
    ValidateDataset,
    ValidateDatasetNoDatabase,
)
from mipdb.exceptions import handle_errors


class IPAddressType(cl.ParamType):
    name = "ip"

    def convert(self, value, param, ctx):  # type: ignore[override]
        if value in (None, ""):
            return None
        try:
            ipaddress.ip_address(value)
            return value
        except ValueError:
            self.fail(f"{value!r} is not a valid IP address", param, ctx)


IP_ADDRESS = IPAddressType()


def _open_sqlite(path: str | Path) -> SQLiteDB:
    return SQLiteDB.from_config({"db_path": str(path)})


def _open_monetdb(enabled: bool, cfg: Dict[str, Any]) -> Optional[MonetDB]:
    if not enabled:
        LOGGER.debug("MonetDB disabled – operating in SQLite‑only mode.")
        return None
    return MonetDB.from_config(cfg)


def with_dbs(func):

    @cl.pass_context
    def _wrapper(ctx: cl.Context, *args, **kwargs):
        kwargs.setdefault("sqlite_db", ctx.obj["sqlite_db"])
        kwargs.setdefault("monetdb", ctx.obj["monetdb"])
        return ctx.invoke(func, *args, **kwargs)

    return cl.decorators.update_wrapper(_wrapper, func)


def resolve_copy_flag(
    copy_from_file: Optional[bool], monetdb: Optional[MonetDB]
) -> bool:

    if monetdb is None:
        if copy_from_file is not None:
            raise cl.BadParameter(
                "--copy/--no-copy is only valid when MonetDB is enabled."
            )
        return True
    if copy_from_file is None:
        copy_from_file = True
    return copy_from_file


def _require(
    name: str,
    cli_val: str | None,
    cfg_key: str,
    cfg: dict[str, str],
    *,
    required: bool,
) -> str | None:
    """
    Return the resolved value for *name* or raise click.BadParameter
    if it's required but not provided in either CLI or config.
    """
    val = cli_val if cli_val not in (None, "") else cfg.get(cfg_key)
    if required and not val:
        raise cl.BadParameter(
            f"Missing required option --{name.replace('_', '-')} "
            f"(also not set as {cfg_key} in config.toml)"
        )
    return val


@cl.group()
@cl.option(
    "--monetdb/--no-monetdb",
    "monetdb_opt",
    default=None,
    help="Enable or disable MonetDB integration (overrides config).",
)
@cl.option("--ip", type=IP_ADDRESS, default=None, help="MonetDB host IP.")
@cl.option("--port", default=None, help="MonetDB port.")
@cl.option("--username", default=None, help="MonetDB admin username.")
@cl.option("--password", default=None, help="MonetDB admin password.")
@cl.option("--db-name", "db_name", default=None, help="MonetDB farm name.")
@cl.option("--sqlite", "sqlite_db_path", default=None, help="SQLite DB file path.")
@cl.pass_context
def cli(
    ctx: cl.Context,
    monetdb_opt: Optional[bool],
    ip: Optional[str],
    port: Optional[str],
    username: Optional[str],
    password: Optional[str],
    db_name: Optional[str],
    sqlite_db_path: Optional[str],
):
    """
    Root command: resolves configuration and stores DB handles in ``ctx.obj``.
    """


    cfg = credentials_from_config()

    monetdb_enabled: bool = (
        monetdb_opt
        if monetdb_opt is not None
        else bool(cfg.get("MONETDB_ENABLED", False))
    )

    ip = _require("ip", ip, "DB_IP", cfg, required=monetdb_enabled)
    port = _require("port", port, "DB_PORT", cfg, required=monetdb_enabled)
    username = _require(
        "username", username, "MONETDB_ADMIN_USERNAME", cfg, required=monetdb_enabled
    )
    password = _require(
        "password", password, "MONETDB_LOCAL_PASSWORD", cfg, required=monetdb_enabled
    )
    db_name = _require("db_name", db_name, "DB_NAME", cfg, required=monetdb_enabled)
    sqlite_db_path = _require(
        "sqlite_db_path", sqlite_db_path, "SQLITE_DB_PATH", cfg, required=True
    )

    ctx.ensure_object(dict)
    ctx.obj["sqlite_db"] = _open_sqlite(sqlite_db_path)
    ctx.obj["monetdb"] = _open_monetdb(
        monetdb_enabled,
        {
            "monetdb": monetdb_enabled,
            "ip": ip,
            "port": port,
            "dbfarm": db_name,
            "username": username,
            "password": password,
        },
    )


@cli.command()
@handle_errors
@with_dbs
def init(sqlite_db: SQLiteDB, **_):
    InitDB(db=sqlite_db).execute()
    LOGGER.info("Database initialized")


COPY_OPT = cl.option(
    "--copy/--no-copy",
    "copy_from_file",
    default=None,
    help="Use MonetDB COPY FROM FILE (MonetDB only).",
)


@cli.command()
@cl.argument("folder", type=cl.Path(exists=True, file_okay=False, path_type=Path))
@handle_errors
def validate_folder(folder: Path):

    if not any(folder.iterdir()):
        raise cl.UsageError(f"The directory {folder} is empty.")

    for meta_path in folder.rglob("CDEsMetadata.json"):
        subdir = meta_path.parent
        LOGGER.info("Validating data-model folder %s", subdir)

        meta_path = subdir / "CDEsMetadata.json"
        if not meta_path.exists():
            LOGGER.warning("  • Skipped (missing CDEsMetadata.json)")
            continue

        meta = JsonFileReader(meta_path).read()
        code, ver = meta["code"], meta["version"]

        ValidateDataModel().execute(meta)
        LOGGER.info("  • Data-model %s:%s OK", code, ver)

        for csv in subdir.glob("*.csv"):
            ValidateDatasetNoDatabase().execute(csv, meta)
            LOGGER.info("    • CSV %s OK", csv.name)

    LOGGER.info("Folder validation completed – all checked files are valid.")


@cli.command("load-folder")
@cl.argument("folder", type=cl.Path(exists=True, file_okay=False, path_type=Path))
@COPY_OPT
@handle_errors
@with_dbs
def load_folder(
    folder: Path,
    copy_from_file: Optional[bool],
    sqlite_db: SQLiteDB,
    monetdb: Optional[MonetDB],
):

    copy_final = resolve_copy_flag(copy_from_file, monetdb)
    Cleanup(sqlite_db, monetdb).execute()

    for meta_path in folder.rglob("CDEsMetadata.json"):
        subdir = meta_path.parent
        LOGGER.info("Processing data-model folder %s", subdir)

        meta = JsonFileReader(meta_path).read()
        code, ver = meta["code"], meta["version"]

        ValidateDataModel().execute(meta)
        AddDataModel(sqlite_db=sqlite_db, monetdb=monetdb).execute(meta)
        for csv in subdir.glob("*.csv"):
            ValidateDataset(sqlite_db=sqlite_db, monetdb=monetdb).execute(
                csv, copy_final, code, ver
            )
            ImportCSV(sqlite_db=sqlite_db, monetdb=monetdb).execute(
                csv, copy_final, code, ver
            )

    LOGGER.info("Folder import finished successfully.")


@cli.command("add-data-model")
@cl.argument("metadata", type=cl.Path(exists=True, dir_okay=False, path_type=Path))
@handle_errors
@with_dbs
def add_data_model(
    metadata: Path,
    sqlite_db: SQLiteDB,
    monetdb: Optional[MonetDB],
):
    data_model_meta = JsonFileReader(metadata).read()
    ValidateDataModel().execute(data_model_meta)
    AddDataModel(sqlite_db=sqlite_db, monetdb=monetdb).execute(data_model_meta)

    LOGGER.info(
        "Data-model %s:%s registered.",
        data_model_meta["code"],
        data_model_meta["version"],
    )


@cli.command("validate-dataset")
@cl.argument("csv", type=cl.Path(exists=True, dir_okay=False, path_type=Path))
@cl.option("-d", "--data-model", "data_model", required=True)
@cl.option("-v", "--version", required=True)
@COPY_OPT
@handle_errors
@with_dbs
def validate_dataset(
    csv: Path,
    data_model: str,
    version: str,
    copy_from_file: Optional[bool],
    sqlite_db: SQLiteDB,
    monetdb: Optional[MonetDB],
):
    copy_final = resolve_copy_flag(copy_from_file, monetdb)
    ValidateDataset(sqlite_db=sqlite_db, monetdb=monetdb).execute(
        csv, copy_final, data_model, version
    )
    LOGGER.info("Dataset %s validated against %s:%s", csv.name, data_model, version)


@cli.command("add-dataset")
@cl.argument("csv", type=cl.Path(exists=True, dir_okay=False, path_type=Path))
@cl.option("-d", "--data-model", "data_model", required=True)
@cl.option("-v", "--version", required=True)
@COPY_OPT
@handle_errors
@with_dbs
def add_dataset(
    csv: Path,
    data_model: str,
    version: str,
    copy_from_file: Optional[bool],
    sqlite_db: SQLiteDB,
    monetdb: Optional[MonetDB],
):
    copy_final = resolve_copy_flag(copy_from_file, monetdb)

    ValidateDataset(sqlite_db=sqlite_db, monetdb=monetdb).execute(
        csv, copy_final, data_model, version
    )
    ImportCSV(sqlite_db=sqlite_db, monetdb=monetdb).execute(
        csv, copy_final, data_model, version
    )

    LOGGER.info("Dataset %s registered under %s:%s", csv.name, data_model, version)


@cli.command("delete-data-model")
@cl.argument("name")
@cl.option("-v", "--version", required=True)
@cl.option(
    "-f", "--force", is_flag=True, help="Also drop datasets based on this model."
)
@handle_errors
@with_dbs
def delete_data_model(
    name: str,
    version: str,
    force: bool,
    sqlite_db: SQLiteDB,
    monetdb: Optional[MonetDB],
):
    DeleteDataModel(sqlite_db=sqlite_db, monetdb=monetdb).execute(name, version, force)
    LOGGER.info("Data-model %s:%s deleted.", name, version)


@cli.command("delete-dataset")
@cl.argument("dataset")
@cl.option("-d", "--data-model", "data_model", required=True)
@cl.option("-v", "--version", required=True)
@handle_errors
@with_dbs
def delete_dataset(
    dataset: str,
    data_model: str,
    version: str,
    sqlite_db: SQLiteDB,
    monetdb: Optional[MonetDB],
):
    DeleteDataset(sqlite_db=sqlite_db, monetdb=monetdb).execute(
        dataset, data_model, version
    )
    LOGGER.info("Dataset %s deleted from %s:%s.", dataset, data_model, version)


@cli.command("disable-data-model")
@cl.argument("name")
@cl.option("-v", "--version", required=True)
@handle_errors
@with_dbs
def disable_data_model(
    name: str,
    version: str,
    sqlite_db: SQLiteDB,
    **_,
):
    DisableDataModel(db=sqlite_db).execute(name, version)
    LOGGER.info("Data-model %s:%s disabled.", name, version)


@cli.command("disable-dataset")
@cl.argument("dataset")
@cl.option("-d", "--data-model", "data_model", required=True)
@cl.option("-v", "--version", required=True)
@handle_errors
@with_dbs
def disable_dataset(
    dataset: str,
    data_model: str,
    version: str,
    sqlite_db: SQLiteDB,
    **_,
):
    DisableDataset(db=sqlite_db).execute(dataset, data_model, version)
    LOGGER.info("Dataset %s disabled in %s:%s.", dataset, data_model, version)


@cli.command("enable-data-model")
@cl.argument("name")
@cl.option("-v", "--version", required=True)
@handle_errors
@with_dbs
def enable_data_model(
    name: str,
    version: str,
    sqlite_db: SQLiteDB,
    **_,
):
    EnableDataModel(db=sqlite_db).execute(name, version)
    LOGGER.info("Data-model %s:%s enabled.", name, version)


@cli.command("enable-dataset")
@cl.argument("dataset")
@cl.option("-d", "--data-model", "data_model", required=True)
@cl.option("-v", "--version", required=True)
@handle_errors
@with_dbs
def enable_dataset(
    dataset: str,
    data_model: str,
    version: str,
    sqlite_db: SQLiteDB,
    **_,
):
    EnableDataset(db=sqlite_db).execute(dataset, data_model, version)
    LOGGER.info("Dataset %s enabled in %s:%s.", dataset, data_model, version)


def _parse_tag(tag: str, remove: bool, force: bool, target_name: str, version: str):
    if "=" in tag:
        key, value = tag.split("=", 1)
        if remove:
            return lambda db: (
                RemovePropertyFromDataModel(db=db).execute(
                    target_name, version, key, value
                )
            )
        return lambda db: (
            AddPropertyToDataModel(db=db).execute(
                target_name, version, key, value, force
            )
        )
    if remove:
        return lambda db: (UntagDataModel(db=db).execute(target_name, version, tag))
    return lambda db: (TagDataModel(db=db).execute(target_name, version, tag))


@cli.command("tag-data-model")
@cl.argument("name")
@cl.option("-v", "--version", required=True)
@cl.option("-t", "--tag", required=True, help="Bare tag or key=value property.")
@cl.option("-r", "--remove", is_flag=True, help="Remove tag / property instead.")
@cl.option("-f", "--force", is_flag=True, help="Overwrite existing property.")
@handle_errors
@with_dbs
def tag_data_model(
    name: str,
    version: str,
    tag: str,
    remove: bool,
    force: bool,
    sqlite_db: SQLiteDB,
    **_,
):
    action = _parse_tag(tag, remove, force, target_name=name, version=version)
    action(sqlite_db)
    LOGGER.info("Tag operation completed for data-model %s:%s.", name, version)


@cli.command("tag-dataset")
@cl.argument("dataset")
@cl.option("-d", "--data-model", "data_model", required=True)
@cl.option("-v", "--version", required=True)
@cl.option("-t", "--tag", required=True, help="Bare tag or key=value property.")
@cl.option("-r", "--remove", is_flag=True, help="Remove tag / property instead.")
@cl.option("-f", "--force", is_flag=True, help="Overwrite existing property.")
@handle_errors
@with_dbs
def tag_dataset(
    dataset: str,
    data_model: str,
    version: str,
    tag: str,
    remove: bool,
    force: bool,
    sqlite_db: SQLiteDB,
    **_,
):
    def _dataset_action(db):
        if "=" in tag:
            key, value = tag.split("=", 1)
            if remove:
                RemovePropertyFromDataset(db=db).execute(
                    dataset, data_model, version, key, value
                )
            else:
                AddPropertyToDataset(db=db).execute(
                    dataset, data_model, version, key, value, force
                )
        else:
            if remove:
                UntagDataset(db=db).execute(dataset, data_model, version, tag)
            else:
                TagDataset(db=db).execute(dataset, data_model, version, tag)

    _dataset_action(sqlite_db)
    LOGGER.info(
        "Tag operation completed for dataset %s (%s:%s).", dataset, data_model, version
    )


@cli.command("list-data-models")
@handle_errors
@with_dbs
def list_data_models(sqlite_db: SQLiteDB, **_):
    ListDataModels(db=sqlite_db).execute()


@cli.command("list-datasets")
@handle_errors
@with_dbs
def list_datasets(sqlite_db: SQLiteDB, **_):
    ListDatasets(sqlite_db=sqlite_db).execute()


entry = cli
