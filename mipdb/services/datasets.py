from __future__ import annotations

import copy
from typing import List

import pandas as pd

from mipdb.data_frame import DataFrame, DATASET_COLUMN_NAME
from mipdb.data_frame_schema import DataFrameSchema
from mipdb.dataelements import (
    flatten_cdes,
    get_cdes_with_enumerations,
    get_cdes_with_min_max,
    get_dataset_enums,
    get_sql_type_per_column,
)
from mipdb.exceptions import InvalidDatasetError, UserInputError
from mipdb.properties import Properties
from mipdb.reader import CSVDataFrameReader
from mipdb.duckdb import DuckDB
from mipdb.duckdb.data_tables import PrimaryDataTable
from mipdb.duckdb.metadata_tables import DataModelTable, DatasetsTable, MetadataTable
from mipdb.duckdb.schema import Schema
from mipdb.services.common import (
    LONGITUDINAL,
    are_data_valid_longitudinal,
    ensure_initialized,
    get_data_model_fullname,
)


def import_dataset(
    db: DuckDB,
    csv_path,
    data_model_code,
    data_model_version,
) -> None:
    ensure_initialized(db)
    data_model_name = get_data_model_fullname(data_model_code, data_model_version)
    data_model = Schema(data_model_name)
    data_model_id = DataModelTable().get_data_model_id(
        data_model_code, data_model_version, db
    )
    metadata_table = MetadataTable.from_db(data_model_name, db)
    cdes = metadata_table.table
    dataset_enumerations = get_dataset_enums(cdes)

    csv_columns = pd.read_csv(csv_path, nrows=0).columns.tolist()
    imported_datasets = _import_datasets(db, csv_path, data_model)

    existing_datasets = DatasetsTable().get_dataset_codes(
        columns=["code"], data_model_id=data_model_id, db=db
    )
    dataset_id = DatasetsTable().get_next_dataset_id(db)
    for dataset in set(imported_datasets) - set(existing_datasets):
        values = dict(
            data_model_id=data_model_id,
            dataset_id=dataset_id,
            code=dataset,
            label=dataset_enumerations[dataset],
            csv_path=str(csv_path),
            status="ENABLED",
            properties={
                "tags": [],
                "properties": {"variables": csv_columns},
            },
        )
        DatasetsTable().insert_values(values, db)
        dataset_id += 1


def _import_datasets(db: DuckDB, csv_path, data_model):
    primary_data_table = PrimaryDataTable.from_db(data_model, db)
    table_columns = [col.name for col in primary_data_table.table.columns]

    imported_datasets: List[str] = []
    with CSVDataFrameReader(csv_path).get_reader() as reader:
        for dataset_data in reader:
            dataframe = DataFrame(dataset_data)
            records = dataframe.to_dict(table_columns)
            if records:
                primary_data_table.insert_values(records, db)
            imported_datasets = list(set(imported_datasets) | set(dataframe.datasets))

    return imported_datasets


def delete_dataset(db: DuckDB, dataset_code, data_model_code, data_model_version) -> None:
    ensure_initialized(db)
    data_model_fullname = get_data_model_fullname(data_model_code, data_model_version)
    _remove_dataset(db, data_model_fullname, dataset_code)
    data_model_id = DataModelTable().get_data_model_id(
        data_model_code, data_model_version, db
    )
    dataset_id = DatasetsTable().get_dataset_id(dataset_code, data_model_id, db)
    DatasetsTable().delete_dataset(dataset_id, data_model_id, db)


def _remove_dataset(db: DuckDB, data_model_fullname, dataset_code):
    schema = Schema(data_model_fullname)
    primary_data_table = PrimaryDataTable.from_db(schema, db)
    primary_data_table.remove_dataset(dataset_code, db)


def enable_dataset(db: DuckDB, dataset_code, data_model_code, data_model_version) -> None:
    ensure_initialized(db)
    datasets_table = DatasetsTable()
    data_model_table = DataModelTable()
    data_model_id = data_model_table.get_data_model_id(
        data_model_code, data_model_version, db
    )
    dataset_id = datasets_table.get_dataset_id(dataset_code, data_model_id, db)
    current_status = datasets_table.get_dataset_status(dataset_id, db)
    if current_status != "ENABLED":
        datasets_table.set_dataset_status("ENABLED", dataset_id, db)
    else:
        raise UserInputError("The dataset was already enabled")


def disable_dataset(db: DuckDB, dataset_code, data_model_code, data_model_version) -> None:
    ensure_initialized(db)
    datasets_table = DatasetsTable()
    data_model_table = DataModelTable()
    data_model_id = data_model_table.get_data_model_id(
        data_model_code, data_model_version, db
    )
    dataset_id = datasets_table.get_dataset_id(dataset_code, data_model_id, db)
    current_status = datasets_table.get_dataset_status(dataset_id, db)
    if current_status != "DISABLED":
        datasets_table.set_dataset_status("DISABLED", dataset_id, db)
    else:
        raise UserInputError("The dataset was already disabled")


def tag_dataset(db: DuckDB, dataset_code, data_model_code, data_model_version, tag):
    ensure_initialized(db)
    datasets_table = DatasetsTable()
    data_model_table = DataModelTable()
    data_model_id = data_model_table.get_data_model_id(
        data_model_code, data_model_version, db
    )
    dataset_id = datasets_table.get_dataset_id(dataset_code, data_model_id, db)
    properties = Properties(datasets_table.get_dataset_properties(data_model_id, db))
    properties.add_tag(tag)
    datasets_table.set_dataset_properties(properties.properties, dataset_id, db)


def untag_dataset(db: DuckDB, dataset_code, data_model_code, version, tag):
    ensure_initialized(db)
    datasets_table = DatasetsTable()
    data_model_table = DataModelTable()
    data_model_id = data_model_table.get_data_model_id(data_model_code, version, db)
    dataset_id = datasets_table.get_dataset_id(dataset_code, data_model_id, db)
    properties = Properties(datasets_table.get_dataset_properties(data_model_id, db))
    properties.remove_tag(tag)
    datasets_table.set_dataset_properties(properties.properties, dataset_id, db)


def add_property_to_dataset(
    db: DuckDB, dataset_code, data_model_code, version, key, value, force
) -> None:
    ensure_initialized(db)
    datasets_table = DatasetsTable()
    data_model_table = DataModelTable()
    data_model_id = data_model_table.get_data_model_id(data_model_code, version, db)
    dataset_id = datasets_table.get_dataset_id(dataset_code, data_model_id, db)
    properties = Properties(datasets_table.get_dataset_properties(data_model_id, db))
    properties.add_property(key, value, force)
    datasets_table.set_dataset_properties(properties.properties, dataset_id, db)


def remove_property_from_dataset(
    db: DuckDB, dataset_code, data_model_code, version, key, value
) -> None:
    ensure_initialized(db)
    datasets_table = DatasetsTable()
    data_model_table = DataModelTable()
    data_model_id = data_model_table.get_data_model_id(data_model_code, version, db)
    dataset_id = datasets_table.get_dataset_id(dataset_code, data_model_id, db)
    properties = Properties(datasets_table.get_dataset_properties(data_model_id, db))
    properties.remove_property(key, value)
    datasets_table.set_dataset_properties(properties.properties, dataset_id, db)


def validate_dataset(
    db: DuckDB,
    csv_path,
    data_model_code,
    data_model_version,
) -> List[str]:
    ensure_initialized(db)
    data_model = get_data_model_fullname(code=data_model_code, version=data_model_version)

    metadata_table = MetadataTable.from_db(data_model, db)
    cdes = metadata_table.table

    dataset_enumerations = get_dataset_enums(cdes)
    if _is_data_model_longitudinal(db, data_model_code, data_model_version):
        are_data_valid_longitudinal(csv_path)
    validated_datasets = _validate_datasets(csv_path, cdes)
    _verify_datasets_exist_in_enumerations(validated_datasets, dataset_enumerations)
    return validated_datasets


def _validate_datasets(csv_path, cdes):
    csv_columns = pd.read_csv(csv_path, nrows=0).columns.tolist()
    if DATASET_COLUMN_NAME not in csv_columns:
        raise InvalidDatasetError("The 'dataset' column is required to exist in the csv.")

    sql_type_per_column = get_sql_type_per_column(cdes)
    cdes_with_min_max = get_cdes_with_min_max(cdes, csv_columns)
    cdes_with_enumerations = get_cdes_with_enumerations(cdes, csv_columns)
    return _validate_csv(
        csv_path, sql_type_per_column, cdes_with_min_max, cdes_with_enumerations
    )


def _is_data_model_longitudinal(db: DuckDB, data_model_code, data_model_version):
    data_model_id = DataModelTable().get_data_model_id(data_model_code, data_model_version, db)
    properties = DataModelTable().get_data_model_properties(data_model_id, db)
    return LONGITUDINAL in properties.get("tags", [])


def _validate_csv(
    csv_path, sql_type_per_column, cdes_with_min_max, cdes_with_enumerations
):
    imported_datasets = []
    csv_columns = pd.read_csv(csv_path, nrows=0).columns.tolist()
    dataframe_schema = DataFrameSchema(
        sql_type_per_column, cdes_with_min_max, cdes_with_enumerations, csv_columns
    )
    with CSVDataFrameReader(csv_path).get_reader() as reader:
        for dataset_data in reader:
            dataframe = DataFrame(dataset_data)
            dataframe_schema.validate_dataframe(dataframe.data)
            imported_datasets = list(set(imported_datasets) | set(dataframe.datasets))
    return imported_datasets


def _verify_datasets_exist_in_enumerations(datasets, dataset_enumerations):
    non_existing_datasets = [dataset for dataset in datasets if dataset not in dataset_enumerations]
    if non_existing_datasets:
        raise InvalidDatasetError(
            "The values:'{non_existing_datasets}' are not present in the enumerations of the CDE 'dataset'."
        )


def validate_dataset_no_database(csv_path, data_model_metadata) -> None:
    csv_columns = pd.read_csv(csv_path, nrows=0).columns.tolist()
    if DATASET_COLUMN_NAME not in csv_columns:
        raise InvalidDatasetError("The 'dataset' column is required to exist in the csv.")
    cdes = flatten_cdes(copy.deepcopy(data_model_metadata))
    cdes = {cde.code: cde for cde in cdes}
    sql_type_per_column = get_sql_type_per_column(cdes)
    cdes_with_min_max = get_cdes_with_min_max(cdes, csv_columns)
    cdes_with_enumerations = get_cdes_with_enumerations(cdes, csv_columns)
    dataset_enumerations = get_dataset_enums(cdes)
    if LONGITUDINAL in data_model_metadata:
        longitudinal = data_model_metadata[LONGITUDINAL]
        if not isinstance(longitudinal, bool):
            raise UserInputError(
                f"Longitudinal flag should be boolean, value given: {longitudinal}"
            )
        if longitudinal:
            are_data_valid_longitudinal(csv_path)
    validated_datasets = _validate_csv(
        csv_path,
        sql_type_per_column,
        cdes_with_min_max,
        cdes_with_enumerations,
    )
    _verify_datasets_exist_in_enumerations(validated_datasets, dataset_enumerations)
