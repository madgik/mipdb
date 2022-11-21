import json

import pandas as pd
import pandera as pa

from mipdb.exceptions import InvalidDatasetError


class Dataset:
    _data: pd.DataFrame
    _name: str

    def __init__(self, data: pd.DataFrame) -> None:
        # Pandas will insert nan values where there is an empty value in the csv.
        # In order to be able to insert the values through the sqlalchemy we need to replace nan with None.
        self._data = data
        self._data = self._data.astype(object).where(pd.notnull(self._data), None)
        self._verify_dataset_field()
        self._name = self._data["dataset"][0]

    @property
    def data(self):
        return self._data

    @property
    def name(self):
        return self._name

    def _verify_dataset_field(self):
        if "dataset" not in self.data.columns:
            raise InvalidDatasetError("There is no dataset field in the Dataset")
        if len(set(self.data["dataset"])) > 1:
            raise InvalidDatasetError("The dataset field contains multiple values.")

    def validate_dataset(self, metadata_table):
        pa_columns = {}
        # Validating the dataset has proper values, according to the data model.

        # There is a need to construct a DataFrameSchema with all the constraints that the metadata is imposing
        # For each column a pandera Column is created that will contain the constraints for the specific column
        for column in self._data.columns:
            if column not in metadata_table:
                raise InvalidDatasetError(
                    f"The column: '{column}' does not exist in the metadata"
                )
            metadata_column = metadata_table[column].metadata
            metadata_column_dict = json.loads(metadata_column)
            checks = self._get_pa_checks(metadata_column_dict)
            cde_sql_type = metadata_column_dict["sql_type"]
            pa_type = self._pa_type_from_sql_type(cde_sql_type)
            # A pandas dataframe it is not possible for a column of integers to contain Nan values.
            # It is automatically converted to float.
            # The workaround is to replace the column's Nan values with valid integers.
            # This change is only for the validation.
            # This will not affect the dataframe that will be imported in the database.
            if cde_sql_type == "int":
                valid_int = 0
                if "min" in metadata_column_dict:
                    valid_int = metadata_column_dict["min"]
                elif "max" in metadata_column_dict:
                    valid_int = metadata_column_dict["max"]
                self._data[column] = self._data[column].fillna(valid_int)
            pa_columns[column] = pa.Column(dtype=pa_type, checks=checks, nullable=True)

        schema = pa.DataFrameSchema(
            columns=pa_columns,
            coerce=True,
        )

        try:
            schema(self._data)
        except pa.errors.SchemaError as exc:
            raise InvalidDatasetError(
                f"On dataset {self._name} and column {exc.schema.name} has error\n{exc.failure_cases}"
            )

    def _pa_type_from_sql_type(self, sql_type):
        return {"text": pa.String, "int": pa.Int, "real": pa.Float}.get(sql_type)

    def to_dict(self):
        return self._data.to_dict("records")

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
