import json

import pandas as pd
import pandera as pa

from mipdb.exceptions import InvalidDatasetError

DATASET_COLUMN_NAME = "dataset"


class DataFrameSchema:
    _schema: pa.DataFrameSchema

    def __init__(self, metadata_table, columns) -> None:
        pa_columns = {}
        # Validating the dataset has proper values, according to the data model.

        # There is a need to construct a DataFrameSchema with all the constraints that the metadata is imposing
        # For each column a pandera Column is created that will contain the constraints for the specific column

        for column in columns:
            if column not in metadata_table:
                raise InvalidDatasetError(
                    f"The column: '{column}' does not exist in the metadata"
                )
            metadata_column = metadata_table[column].metadata
            metadata_column_dict = json.loads(metadata_column)
            checks = self._get_pa_checks(metadata_column_dict)
            cde_sql_type = metadata_column_dict["sql_type"]
            pa_type = self._pa_type_from_sql_type(cde_sql_type)
            pa_columns[column] = pa.Column(dtype=pa_type, checks=checks, nullable=True)

        self._schema = pa.DataFrameSchema(
            columns=pa_columns,
            coerce=True,
        )

    def validate_dataframe(self, dataframe):
        try:
            self._schema(dataframe)
        except pa.errors.SchemaError as exc:
            raise InvalidDatasetError(
                f"An error occurred while validating the csv on column: '{exc.schema.name}'\n{exc.failure_cases}"
            )

    @property
    def schema(self):
        return self._schema

    def _pa_type_from_sql_type(self, sql_type):
        return {"text": pa.String, "int": pd.Int64Dtype(), "real": pa.Float}.get(
            sql_type
        )

    def _get_pa_checks(self, _metadata):
        checks = []
        if "max" in _metadata:
            checks.append(pa.Check(lambda s: s <= _metadata["max"]))
        if "min" in _metadata:
            checks.append(pa.Check(lambda s: s >= _metadata["min"]))
        if "enumerations" in _metadata:
            checks.append(
                pa.Check(
                    lambda s: s.isin(
                        [key for key, value in _metadata["enumerations"].items()]
                        + ["None"]
                    )
                )
            )
        return checks
