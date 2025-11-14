from __future__ import annotations

import pandas as pd

from mipdb.duckdb import DuckDB
from mipdb.duckdb.metadata_tables import DataModelTable, DatasetsTable
from mipdb.services.common import ensure_initialized


def init_database(db: DuckDB) -> None:
    data_model_table, datasets_table = DataModelTable(), DatasetsTable()
    if not data_model_table.exists(db):
        data_model_table.create(db)
    if not datasets_table.exists(db):
        datasets_table.create(db)


def cleanup_database(db: DuckDB) -> None:
    ensure_initialized(db)
    data_model_table = DataModelTable()
    data_model_rows = data_model_table.get_data_models(db, columns=["code", "version"])
    if not data_model_rows:
        return

    from mipdb.services.datamodels import delete_data_model

    for code, version in data_model_rows:
        delete_data_model(db, code, version, force=True)


def list_data_models(db: DuckDB) -> None:
    ensure_initialized(db)
    data_model_table = DataModelTable()
    data_model_row_columns = ["data_model_id", "code", "version", "label", "status"]
    data_model_rows = data_model_table.get_data_models(db=db, columns=data_model_row_columns)
    dataset_count_by_data_model_id = {
        data_model_id: dataset_count
        for data_model_id, dataset_count in data_model_table.get_dataset_count_by_data_model_id(db)
    }
    data_models_info = [
        list(row) + [dataset_count_by_data_model_id.get(row[0], 0)]
        for row in data_model_rows
    ]

    if not data_models_info:
        print("There are no data models.")
        return

    df = pd.DataFrame(data_models_info, columns=data_model_row_columns + ["count"])
    print(df)


def list_datasets(db: DuckDB) -> None:
    ensure_initialized(db)
    dataset_table = DatasetsTable()
    dataset_row_columns = ["dataset_id", "data_model_id", "code", "label", "status"]
    dataset_rows = dataset_table.get_datasets(db, columns=dataset_row_columns)

    if not dataset_rows:
        print("There are no datasets.")
        return

    df = pd.DataFrame([list(row) for row in dataset_rows], columns=dataset_row_columns)
    print(df)
