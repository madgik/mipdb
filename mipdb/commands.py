from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import click as cl

from mipdb.credentials import credentials_from_config
from mipdb.logger import LOGGER
from mipdb.duckdb import DuckDB
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


def _open_duckdb(path: str | Path) -> DuckDB:
    return DuckDB.from_config({"db_path": str(path)})


def with_db(func):

    @cl.pass_context
    def _wrapper(ctx: cl.Context, *args, **kwargs):
        kwargs.setdefault("duckdb", ctx.obj["duckdb"])
        return ctx.invoke(func, *args, **kwargs)

    return cl.decorators.update_wrapper(_wrapper, func)


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
    "--duckdb",
    "duckdb_path",
    default=None,
    help="DuckDB database file path.",
)
@cl.pass_context
def cli(ctx: cl.Context, duckdb_path: str | None):
    """
    Root command: resolves configuration and stores the DuckDB handle in ``ctx.obj``.
    """

    cfg = credentials_from_config()
    duckdb_path = _require("duckdb_path", duckdb_path, "DUCKDB_PATH", cfg, required=True)

    ctx.ensure_object(dict)
    ctx.obj["duckdb"] = _open_duckdb(duckdb_path)


@cli.command()
@handle_errors
@with_db
def init(duckdb: DuckDB, **_):
    InitDB(db=duckdb).execute()
    LOGGER.info("Database initialized")


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
@handle_errors
@with_db
def load_folder(folder: Path, duckdb: DuckDB):

    Cleanup(duckdb).execute()

    for meta_path in folder.rglob("CDEsMetadata.json"):
        subdir = meta_path.parent
        LOGGER.info("Processing data-model folder %s", subdir)

        meta = JsonFileReader(meta_path).read()
        code, ver = meta["code"], meta["version"]

        ValidateDataModel().execute(meta)
        AddDataModel(duckdb=duckdb).execute(meta)
        for csv in subdir.glob("*.csv"):
            ValidateDataset(duckdb=duckdb).execute(csv, code, ver)
            ImportCSV(duckdb=duckdb).execute(csv, code, ver)

    LOGGER.info("Folder import finished successfully.")


@cli.command("add-data-model")
@cl.argument("metadata", type=cl.Path(exists=True, dir_okay=False, path_type=Path))
@handle_errors
@with_db
def add_data_model(
    metadata: Path,
    duckdb: DuckDB,
):
    data_model_meta = JsonFileReader(metadata).read()
    ValidateDataModel().execute(data_model_meta)
    AddDataModel(duckdb=duckdb).execute(data_model_meta)

    LOGGER.info(
        "Data-model %s:%s registered.",
        data_model_meta["code"],
        data_model_meta["version"],
    )


@cli.command("validate-dataset")
@cl.argument("csv", type=cl.Path(exists=True, dir_okay=False, path_type=Path))
@cl.option("-d", "--data-model", "data_model", required=True)
@cl.option("-v", "--version", required=True)
@handle_errors
@with_db
def validate_dataset(
    csv: Path,
    data_model: str,
    version: str,
    duckdb: DuckDB,
):
    ValidateDataset(duckdb=duckdb).execute(csv, data_model, version)
    LOGGER.info("Dataset %s validated against %s:%s", csv.name, data_model, version)


@cli.command("add-dataset")
@cl.argument("csv", type=cl.Path(exists=True, dir_okay=False, path_type=Path))
@cl.option("-d", "--data-model", "data_model", required=True)
@cl.option("-v", "--version", required=True)
@handle_errors
@with_db
def add_dataset(
    csv: Path,
    data_model: str,
    version: str,
    duckdb: DuckDB,
):
    ValidateDataset(duckdb=duckdb).execute(csv, data_model, version)
    ImportCSV(duckdb=duckdb).execute(csv, data_model, version)

    LOGGER.info("Dataset %s registered under %s:%s", csv.name, data_model, version)


@cli.command("delete-data-model")
@cl.argument("name")
@cl.option("-v", "--version", required=True)
@cl.option(
    "-f", "--force", is_flag=True, help="Also drop datasets based on this model."
)
@handle_errors
@with_db
def delete_data_model(
    name: str,
    version: str,
    force: bool,
    duckdb: DuckDB,
):
    DeleteDataModel(duckdb=duckdb).execute(name, version, force)
    LOGGER.info("Data-model %s:%s deleted.", name, version)


@cli.command("delete-dataset")
@cl.argument("dataset")
@cl.option("-d", "--data-model", "data_model", required=True)
@cl.option("-v", "--version", required=True)
@handle_errors
@with_db
def delete_dataset(
    dataset: str,
    data_model: str,
    version: str,
    duckdb: DuckDB,
):
    DeleteDataset(duckdb=duckdb).execute(dataset, data_model, version)
    LOGGER.info("Dataset %s deleted from %s:%s.", dataset, data_model, version)


@cli.command("disable-data-model")
@cl.argument("name")
@cl.option("-v", "--version", required=True)
@handle_errors
@with_db
def disable_data_model(
    name: str,
    version: str,
    duckdb: DuckDB,
    **_,
):
    DisableDataModel(db=duckdb).execute(name, version)
    LOGGER.info("Data-model %s:%s disabled.", name, version)


@cli.command("disable-dataset")
@cl.argument("dataset")
@cl.option("-d", "--data-model", "data_model", required=True)
@cl.option("-v", "--version", required=True)
@handle_errors
@with_db
def disable_dataset(
    dataset: str,
    data_model: str,
    version: str,
    duckdb: DuckDB,
    **_,
):
    DisableDataset(db=duckdb).execute(dataset, data_model, version)
    LOGGER.info("Dataset %s disabled in %s:%s.", dataset, data_model, version)


@cli.command("enable-data-model")
@cl.argument("name")
@cl.option("-v", "--version", required=True)
@handle_errors
@with_db
def enable_data_model(
    name: str,
    version: str,
    duckdb: DuckDB,
    **_,
):
    EnableDataModel(db=duckdb).execute(name, version)
    LOGGER.info("Data-model %s:%s enabled.", name, version)


@cli.command("enable-dataset")
@cl.argument("dataset")
@cl.option("-d", "--data-model", "data_model", required=True)
@cl.option("-v", "--version", required=True)
@handle_errors
@with_db
def enable_dataset(
    dataset: str,
    data_model: str,
    version: str,
    duckdb: DuckDB,
    **_,
):
    EnableDataset(db=duckdb).execute(dataset, data_model, version)
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
@with_db
def tag_data_model(
    name: str,
    version: str,
    tag: str,
    remove: bool,
    force: bool,
    duckdb: DuckDB,
    **_,
):
    action = _parse_tag(tag, remove, force, target_name=name, version=version)
    action(duckdb)
    LOGGER.info("Tag operation completed for data-model %s:%s.", name, version)


@cli.command("tag-dataset")
@cl.argument("dataset")
@cl.option("-d", "--data-model", "data_model", required=True)
@cl.option("-v", "--version", required=True)
@cl.option("-t", "--tag", required=True, help="Bare tag or key=value property.")
@cl.option("-r", "--remove", is_flag=True, help="Remove tag / property instead.")
@cl.option("-f", "--force", is_flag=True, help="Overwrite existing property.")
@handle_errors
@with_db
def tag_dataset(
    dataset: str,
    data_model: str,
    version: str,
    tag: str,
    remove: bool,
    force: bool,
    duckdb: DuckDB,
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

    _dataset_action(duckdb)
    LOGGER.info(
        "Tag operation completed for dataset %s (%s:%s).", dataset, data_model, version
    )


@cli.command("list-data-models")
@handle_errors
@with_db
def list_data_models(duckdb: DuckDB, **_):
    ListDataModels(db=duckdb).execute()


@cli.command("list-datasets")
@handle_errors
@with_db
def list_datasets(duckdb: DuckDB, **_):
    ListDatasets(db=duckdb).execute()


entry = cli
