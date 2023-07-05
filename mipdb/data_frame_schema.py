import pandas as pd
import pandera as pa

from mipdb.exceptions import InvalidDatasetError


class DataFrameSchema:
    _schema: pa.DataFrameSchema

    def __init__(
        self, sql_type_per_column, cdes_with_min_max, cdes_with_enumerations, columns
    ) -> None:
        pa_columns = {}
        # Validating the dataset has proper values, according to the data model.

        # There is a need to construct a DataFrameSchema with all the constraints that the metadata is imposing
        # For each column a pandera Column is created that will contain the constraints for the specific column
        if not set(columns) <= set(sql_type_per_column.keys()):
            raise InvalidDatasetError(
                f"Columns:{set(columns) - set(sql_type_per_column.keys()) - {'row_id'} } are not present in the CDEs"
            )

        for column in columns:
            checks = self._get_pa_checks(
                cdes_with_min_max, cdes_with_enumerations, column
            )
            cde_sql_type = sql_type_per_column[column]
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

    def _get_pa_checks(self, cdes_with_min_max, cdes_with_enumerations, column):
        checks = []
        if column in cdes_with_min_max:
            min, max = cdes_with_min_max[column]
            if max:
                checks.append(pa.Check(lambda s: s <= max))
            if min:
                checks.append(pa.Check(lambda s: s >= min))

        if column in cdes_with_enumerations:
            checks.append(
                pa.Check(lambda s: s.isin(cdes_with_enumerations[column] + ["None"]))
            )
        return checks
