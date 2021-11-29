import json

import pandas as pd
import pandera as pa
from pandera.errors import SchemaError

from mipdb.exceptions import InvalidDatasetError


class Dataset:
    _data: pd.DataFrame
    _name: str

    def __init__(self, data: pd.DataFrame) -> None:
        # Pandas will insert nan values where there is a empty value in the csv.
        # In order to be able to insert the values through the sqlalchemy we need to replace nan with None.
        self._data = data.astype(object).where(pd.notnull(data), None)
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
        # Validating that the dataset always has subjectcode as the first column.
        # This is according to the data requirements
        # https://github.com/HBPMedical/mip-deployment/blob/master/documentation/NewDataRequirements.md
        if "subjectcode" != self.data.keys()[0]:
            raise InvalidDatasetError(
                f"""Error inserting dataset into the database,
                    subjectcode should be the first column.
                """
            )

        columns = [column for column in self.data if column != "subjectcode"]
        # There is a need to construct a DataFrameSchema with all the constrains that the metadata is imposing
        # For each column a pandera Column is created that will contain the constrains for the specific column
        for column in columns:
            if column not in metadata_table:
                raise InvalidDatasetError(f"The column: '{column}' does not exist in the metadata")
            metadata_column = metadata_table[column].metadata
            metadata_column_dict = json.loads(metadata_column)
            checks = self._get_pa_checks(metadata_column_dict)
            pa_type = self.pa_type_from_sql_type(metadata_column_dict["sql_type"])
            pa_columns[column] = pa.Column(dtype=pa_type, checks=checks, nullable=True)

        index = pa.Index(
            pa.String,
            checks=[
                # id is unique
                pa.Check(lambda s: s.duplicated().sum() == 0),
            ],
        )

        schema = pa.DataFrameSchema(
            columns=pa_columns,
            index=index,
            coerce=True,
        )

        try:
            schema.validate(self._data)
            print("This dataset has the proper format and data")
        except SchemaError as exc:
            raise InvalidDatasetError(f"The column: '{exc.schema.name}' in the dataset "
                                      f"violates the constraints imposed by the schema")
        except Exception as exc:
            raise exc

    def pa_type_from_sql_type(self, sql_type):
        return {"text": pa.String, "int": pa.Int, "real": pa.Float}.get(sql_type)

    def to_dict(self):
        return self._data.to_dict("records")

    def _get_pa_checks(self, _metadata):
        checks = []
        if "maxValue" in _metadata:
            checks.append(pa.Check(lambda s: s <= _metadata["maxValue"]))
        if "minValue" in _metadata:
            checks.append(pa.Check(lambda s: s >= _metadata["minValue"]))
        if "enumerations" in _metadata:
            checks.append(
                pa.Check(
                    lambda s: s.isin(
                        [
                            enumeration["code"]
                            for enumeration in _metadata["enumerations"]
                        ]
                    )
                )
            )
        return checks
