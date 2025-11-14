from __future__ import annotations

import copy

from mipdb.properties import Properties
from mipdb.dataelements import (
    flatten_cdes,
    validate_dataset_present_on_cdes_with_proper_format,
    validate_longitudinal_data_model,
)
from mipdb.exceptions import ForeignKeyError, InvalidDatasetError, UserInputError
from mipdb.duckdb import DuckDB
from mipdb.duckdb.data_tables import PrimaryDataTable
from mipdb.duckdb.metadata_tables import DataModelTable, MetadataTable, DatasetsTable
from mipdb.duckdb.schema import Schema
from mipdb.services.common import (
    LONGITUDINAL,
    ensure_initialized,
    get_data_model_fullname,
)


def init_tags_if_needed(meta: dict, db: DuckDB, code: str, version: str) -> None:
    if LONGITUDINAL not in meta:
        return

    longitudinal = meta[LONGITUDINAL]
    if not isinstance(longitudinal, bool):
        raise UserInputError(
            f"Longitudinal flag should be boolean, value given: {longitudinal}"
        )
    if longitudinal:
        tag_data_model(db, code=code, version=version, tag=LONGITUDINAL)


def add_data_model(db: DuckDB, data_model_metadata: dict) -> None:
    ensure_initialized(db)
    code, version = data_model_metadata["code"], data_model_metadata["version"]
    data_model = get_data_model_fullname(code, version)

    cdes = flatten_cdes(copy.deepcopy(data_model_metadata))
    _create_primary_data_table(db, data_model, cdes)
    _create_metadata_table(db, data_model, cdes)
    _insert_data_model_row(db, data_model_metadata)
    init_tags_if_needed(data_model_metadata, db, code, version)


def _create_primary_data_table(db: DuckDB, data_model: str, cdes: list) -> None:
    schema = Schema(data_model)
    primary_table = PrimaryDataTable.from_cdes(schema, cdes)
    if primary_table.exists(db):
        primary_table.drop(db)
    primary_table.create(db)


def _create_metadata_table(db: DuckDB, data_model: str, cdes: list) -> None:
    metadata_table = MetadataTable(data_model)
    if metadata_table.exists(db):
        metadata_table.drop(db)
    metadata_table.create(db)
    metadata_table.insert_values(
        metadata_table.get_values_from_cdes(cdes), db
    )


def _insert_data_model_row(db: DuckDB, data_model_metadata: dict) -> None:
    code, version = data_model_metadata["code"], data_model_metadata["version"]
    dm_table = DataModelTable()
    new_id = dm_table.get_next_data_model_id(db)

    props = Properties(dm_table.get_data_model_properties(new_id, db))
    props.add_property("cdes", data_model_metadata, force=True)
    dm_table.insert_values(
        dict(
            data_model_id=new_id,
            code=code,
            version=version,
            label=data_model_metadata["label"],
            status="ENABLED",
            properties=props.properties,
        ),
        db,
    )


def validate_data_model_metadata(data_model_metadata) -> None:
    if "version" not in data_model_metadata:
        raise UserInputError("You need to include a version on the CDEsMetadata.json")
    cdes = flatten_cdes(copy.deepcopy(data_model_metadata))
    validate_dataset_present_on_cdes_with_proper_format(cdes)
    if LONGITUDINAL in data_model_metadata:
        longitudinal = data_model_metadata[LONGITUDINAL]
        if not isinstance(longitudinal, bool):
            raise UserInputError(
                f"Longitudinal flag should be boolean, value given: {longitudinal}"
            )
        if longitudinal:
            validate_longitudinal_data_model(cdes)


def delete_data_model(db: DuckDB, code: str, version: str, force: bool) -> None:
    ensure_initialized(db)
    name = get_data_model_fullname(code, version)
    schema = Schema(name)
    data_model_table = DataModelTable()
    data_model_id = data_model_table.get_data_model_id(code, version, db)
    if not force:
        _validate_data_model_deletion(db, name, data_model_id)
    MetadataTable(data_model=name).drop(db)
    _delete_related_datasets(db, data_model_id, code, version)
    primary_table = PrimaryDataTable.from_db(schema, db)
    primary_table.drop(db)
    data_model_table.delete_data_model(code, version, db)


def _validate_data_model_deletion(db, data_model_name, data_model_id):
    datasets = DatasetsTable().get_dataset_codes(
        db=db, columns=["code"], data_model_id=data_model_id
    )
    if datasets:
        raise ForeignKeyError(
            f"The Data Model:{data_model_name} cannot be deleted because it contains "
            f"Datasets: {datasets}\nIf you want to force delete everything, please use the '--force' flag"
        )


def _delete_related_datasets(db, data_model_id, data_model_code, data_model_version):
    from mipdb.services.datasets import delete_dataset as delete_dataset_service

    datasets_table = DatasetsTable()
    dataset_codes = datasets_table.get_dataset_codes(
        data_model_id=data_model_id, columns=["code"], db=db
    )
    for dataset_code in dataset_codes:
        delete_dataset_service(db, dataset_code, data_model_code, data_model_version)


def enable_data_model(db: DuckDB, code: str, version: str) -> None:
    ensure_initialized(db)
    data_model_table = DataModelTable()
    data_model_id = data_model_table.get_data_model_id(code, version, db)
    current_status = data_model_table.get_data_model_status(data_model_id, db)
    if current_status != "ENABLED":
        data_model_table.set_data_model_status("ENABLED", data_model_id, db)
    else:
        raise UserInputError("The data model was already enabled")


def disable_data_model(db: DuckDB, code: str, version: str) -> None:
    ensure_initialized(db)
    data_model_table = DataModelTable()
    data_model_id = data_model_table.get_data_model_id(code, version, db)
    current_status = data_model_table.get_data_model_status(data_model_id, db)
    if current_status != "DISABLED":
        data_model_table.set_data_model_status("DISABLED", data_model_id, db)
    else:
        raise UserInputError("The data model was already disabled")


def tag_data_model(db: DuckDB, code: str, version: str, tag: str) -> None:
    ensure_initialized(db)
    data_model_table = DataModelTable()
    data_model_id = data_model_table.get_data_model_id(code, version, db)
    properties = Properties(data_model_table.get_data_model_properties(data_model_id, db))
    properties.add_tag(tag)
    data_model_table.set_data_model_properties(properties.properties, data_model_id, db)


def untag_data_model(db: DuckDB, code: str, version: str, tag: str) -> None:
    ensure_initialized(db)
    data_model_table = DataModelTable()
    data_model_id = data_model_table.get_data_model_id(code, version, db)
    properties = Properties(data_model_table.get_data_model_properties(data_model_id, db))
    properties.remove_tag(tag)
    data_model_table.set_data_model_properties(properties.properties, data_model_id, db)


def add_property_to_data_model(
    db: DuckDB, code: str, version: str, key: str, value, force: bool
) -> None:
    ensure_initialized(db)
    data_model_table = DataModelTable()
    data_model_id = data_model_table.get_data_model_id(code, version, db)
    properties = Properties(data_model_table.get_data_model_properties(data_model_id, db))
    properties.add_property(key, value, force)
    data_model_table.set_data_model_properties(properties.properties, data_model_id, db)


def remove_property_from_data_model(
    db: DuckDB, code: str, version: str, key: str, value
) -> None:
    ensure_initialized(db)
    data_model_table = DataModelTable()
    data_model_id = data_model_table.get_data_model_id(code, version, db)
    properties = Properties(data_model_table.get_data_model_properties(data_model_id, db))
    properties.remove_property(key, value)
    data_model_table.set_data_model_properties(properties.properties, data_model_id, db)
