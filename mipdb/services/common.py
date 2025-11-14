from __future__ import annotations

import pandas as pd

from mipdb.exceptions import InvalidDatasetError, UserInputError
from mipdb.duckdb.metadata_tables import DataModelTable, DatasetsTable
from mipdb.duckdb import DuckDB

LONGITUDINAL = "longitudinal"


def get_data_model_fullname(code: str, version: str) -> str:
    return f"{code}:{version}"


def ensure_initialized(db: DuckDB) -> None:
    if not (DataModelTable().exists(db) and DatasetsTable().exists(db)):
        raise UserInputError("You need to initialize the database!\nTry mipdb init")


def check_unique_longitudinal_dataset_primary_keys(df: pd.DataFrame) -> None:
    duplicates = df[df.duplicated(subset=["visitid", "subjectid"], keep=False)]
    if not duplicates.empty:
        raise InvalidDatasetError(
            "Invalid csv: the following visitid and subjectid pairs are duplicated:\n"
            f"{duplicates}"
        )


def check_subjectid_is_full(df: pd.DataFrame) -> None:
    if df["subjectid"].isnull().any():
        raise InvalidDatasetError("Column 'subjectid' should never contain null values")


def check_visitid_is_full(df: pd.DataFrame) -> None:
    if df["visitid"].isnull().any():
        raise InvalidDatasetError("Column 'visitid' should never contain null values")


def are_data_valid_longitudinal(csv_path) -> None:
    df = pd.read_csv(csv_path, usecols=["subjectid", "visitid"])
    check_unique_longitudinal_dataset_primary_keys(df)
    check_subjectid_is_full(df)
    check_visitid_is_full(df)
