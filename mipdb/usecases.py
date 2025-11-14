"""Compatibility wrappers that delegate to the service layer.

The CLI still instantiates these classes, but the actual logic now lives in
``mipdb.services`` modules so that the rest of the codebase can be pure DuckDB
without the old UseCase hierarchy.
"""

from __future__ import annotations

from mipdb.duckdb import DuckDB
from mipdb.services import (
    add_data_model,
    add_property_to_data_model,
    add_property_to_dataset,
    cleanup_database,
    delete_data_model,
    delete_dataset,
    disable_data_model,
    disable_dataset,
    enable_data_model,
    enable_dataset,
    import_dataset,
    init_database,
    list_data_models,
    list_datasets,
    remove_property_from_data_model,
    remove_property_from_dataset,
    tag_data_model,
    tag_dataset,
    untag_data_model,
    untag_dataset,
    validate_data_model_metadata,
    validate_dataset,
    validate_dataset_no_database,
)


class InitDB:
    def __init__(self, db: DuckDB) -> None:
        self.db = db

    def execute(self) -> None:
        init_database(self.db)


class Cleanup:
    def __init__(self, duckdb: DuckDB) -> None:
        self.db = duckdb

    def execute(self) -> None:
        cleanup_database(self.db)


class AddDataModel:
    def __init__(self, duckdb: DuckDB) -> None:
        self.db = duckdb

    def execute(self, data_model_metadata: dict) -> None:
        add_data_model(self.db, data_model_metadata)


class ValidateDataModel:
    def execute(self, data_model_metadata) -> None:
        validate_data_model_metadata(data_model_metadata)


class DeleteDataModel:
    def __init__(self, duckdb: DuckDB) -> None:
        self.db = duckdb

    def execute(self, code, version, force) -> None:
        delete_data_model(self.db, code, version, force)


class ImportCSV:
    def __init__(self, duckdb: DuckDB) -> None:
        self.db = duckdb

    def execute(self, csv_path, data_model_code, data_model_version):
        import_dataset(self.db, csv_path, data_model_code, data_model_version)


class ValidateDataset:
    def __init__(self, duckdb: DuckDB) -> None:
        self.db = duckdb

    def execute(self, csv_path, data_model_code, data_model_version):
        validate_dataset(self.db, csv_path, data_model_code, data_model_version)


class ValidateDatasetNoDatabase:
    def execute(self, csv_path, data_model_metadata) -> None:
        validate_dataset_no_database(csv_path, data_model_metadata)


class DeleteDataset:
    def __init__(self, duckdb: DuckDB) -> None:
        self.db = duckdb

    def execute(self, dataset, data_model_code, data_model_version) -> None:
        delete_dataset(self.db, dataset, data_model_code, data_model_version)


class EnableDataModel:
    def __init__(self, db: DuckDB) -> None:
        self.db = db

    def execute(self, code, version) -> None:
        enable_data_model(self.db, code, version)


class DisableDataModel:
    def __init__(self, db: DuckDB) -> None:
        self.db = db

    def execute(self, code, version) -> None:
        disable_data_model(self.db, code, version)


class EnableDataset:
    def __init__(self, db: DuckDB) -> None:
        self.db = db

    def execute(self, dataset_code, data_model_code, data_model_version) -> None:
        enable_dataset(self.db, dataset_code, data_model_code, data_model_version)


class DisableDataset:
    def __init__(self, db: DuckDB) -> None:
        self.db = db

    def execute(self, dataset_code, data_model_code, data_model_version) -> None:
        disable_dataset(self.db, dataset_code, data_model_code, data_model_version)


class TagDataModel:
    def __init__(self, db: DuckDB) -> None:
        self.db = db

    def execute(self, code, version, tag) -> None:
        tag_data_model(self.db, code, version, tag)


class UntagDataModel:
    def __init__(self, db: DuckDB) -> None:
        self.db = db

    def execute(self, code, version, tag) -> None:
        untag_data_model(self.db, code, version, tag)


class AddPropertyToDataModel:
    def __init__(self, db: DuckDB) -> None:
        self.db = db

    def execute(self, code, version, key, value, force) -> None:
        add_property_to_data_model(self.db, code, version, key, value, force)


class RemovePropertyFromDataModel:
    def __init__(self, db: DuckDB) -> None:
        self.db = db

    def execute(self, code, version, key, value) -> None:
        remove_property_from_data_model(self.db, code, version, key, value)


class TagDataset:
    def __init__(self, db: DuckDB) -> None:
        self.db = db

    def execute(self, dataset_code, data_model_code, data_model_version, tag) -> None:
        tag_dataset(self.db, dataset_code, data_model_code, data_model_version, tag)


class UntagDataset:
    def __init__(self, db: DuckDB) -> None:
        self.db = db

    def execute(self, dataset_code, data_model_code, version, tag) -> None:
        untag_dataset(self.db, dataset_code, data_model_code, version, tag)


class AddPropertyToDataset:
    def __init__(self, db: DuckDB) -> None:
        self.db = db

    def execute(self, dataset_code, data_model_code, version, key, value, force) -> None:
        add_property_to_dataset(
            self.db,
            dataset_code,
            data_model_code,
            version,
            key,
            value,
            force,
        )


class RemovePropertyFromDataset:
    def __init__(self, db: DuckDB) -> None:
        self.db = db

    def execute(self, dataset_code, data_model_code, version, key, value) -> None:
        remove_property_from_dataset(
            self.db,
            dataset_code,
            data_model_code,
            version,
            key,
            value,
        )


class ListDataModels:
    def __init__(self, db: DuckDB) -> None:
        self.db = db

    def execute(self) -> None:
        list_data_models(self.db)


class ListDatasets:
    def __init__(self, duckdb: DuckDB) -> None:
        self.db = duckdb

    def execute(self) -> None:
        list_datasets(self.db)
