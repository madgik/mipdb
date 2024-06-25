from abc import ABC, abstractmethod
import json
from enum import Enum
from typing import Union, List

import sqlalchemy as sql
from sqlalchemy import MetaData

from mipdb.monetdb import credentials_from_config
from mipdb.data_frame import DATASET_COLUMN_NAME
from mipdb.dataelements import CommonDataElement
from mipdb.exceptions import UserInputError
from mipdb.schema import Schema

RECORDS_PER_COPY = 100000


class User(Enum):
    credentials = credentials_from_config()
    executor = (
        credentials["MONETDB_LOCAL_USERNAME"]
        if credentials["MONETDB_LOCAL_USERNAME"]
        else "executor"
    )
    admin = (
        credentials["MONETDB_ADMIN_USERNAME"]
        if credentials["MONETDB_ADMIN_USERNAME"]
        else "admin"
    )
    guest = (
        credentials["MONETDB_PUBLIC_USERNAME"]
        if credentials["MONETDB_PUBLIC_USERNAME"]
        else "guest"
    )


class SQLTYPES:
    INTEGER = sql.Integer
    STRING = sql.String(255)
    FLOAT = sql.Float


STR2SQLTYPE = {"int": SQLTYPES.INTEGER, "text": SQLTYPES.STRING, "real": SQLTYPES.FLOAT}
TEMPORARY_TABLE_NAME = "temp"


class Table(ABC):
    _table: sql.Table

    @abstractmethod
    def __init__(self, schema):
        pass

    @property
    def table(self):
        return self._table

    def create(self, db):
        db.create_table(self._table)
        db.grant_select_access_rights(self._table, User.executor.value)

    def exists(self, db):
        return db.table_exists(self._table)

    def insert_values(self, values, db):
        db.insert_values_to_table(self._table, values)

    def delete(self, db):
        db.delete_table_values(self._table)

    def get_row_count(self, db):
        return db.get_row_count(self.table.fullname)

    def get_column_distinct(self, column, db):
        return db.get_column_distinct(column, self.table.fullname)

    def drop(self, db):
        db.drop_table(self._table)


metadata = MetaData()


class PrimaryDataTable(Table):
    def __init__(self):
        self._table = None

    def set_table(self, table):
        self._table = table

    @classmethod
    def from_cdes(
        cls, schema: Schema, cdes: List[CommonDataElement]
    ) -> "PrimaryDataTable":
        columns = [
            sql.Column(
                cde.code, STR2SQLTYPE[json.loads(cde.metadata)["sql_type"]], quote=True
            )
            for cde in cdes
        ]
        columns.insert(
            0,
            sql.Column(
                "row_id",
                SQLTYPES.INTEGER,
                primary_key=True,
                quote=True,
            ),
        )
        table = sql.Table(
            "primary_data",
            schema.schema,
            *columns,
        )
        new_table = cls()
        new_table.set_table(table)
        return new_table

    def get_data_count_by_dataset(self, data_model_fullname, db):
        return db.get_data_count_by_dataset(data_model_fullname)

    @classmethod
    def from_db(cls, schema: Schema, db) -> "PrimaryDataTable":
        table = sql.Table(
            "primary_data", schema.schema, autoload_with=db.get_executor()
        )
        new_table = cls()
        table.columns = [
            sql.Column(column.name, quote=True) for column in list(table.columns)
        ]
        new_table.set_table(table)
        return new_table

    def remove_dataset(self, dataset_name, schema_full_name, db):
        delete = sql.text(
            f'DELETE FROM "{schema_full_name}".primary_data '
            "WHERE dataset = :dataset_name "
        )
        db.execute(delete, dataset_name=dataset_name)


class TemporaryTable(Table):
    def __init__(self, dataframe_sql_type_per_column, db):
        columns = [
            sql.Column(name, STR2SQLTYPE[sql_type], quote=True)
            for name, sql_type in dataframe_sql_type_per_column.items()
        ]

        self._table = sql.Table(
            TEMPORARY_TABLE_NAME,
            MetaData(bind=db.get_executor()),
            *columns,
            prefixes=["TEMPORARY"],
        )

    def validate_csv(self, csv_path, cdes_with_min_max, cdes_with_enumerations, db):
        validated_datasets = []
        offset = 2

        while True:
            self.load_csv(
                csv_path=csv_path, offset=offset, records=RECORDS_PER_COPY, db=db
            )
            offset += RECORDS_PER_COPY

            table_count = self.get_row_count(db=db)
            if not table_count:
                break

            validated_datasets = set(validated_datasets) | set(
                self.get_column_distinct(DATASET_COLUMN_NAME, db)
            )
            self._validate_enumerations_restriction(cdes_with_enumerations, db)
            self._validate_min_max_restriction(cdes_with_min_max, db)
            self.delete(db)

            # If the temp contains fewer rows than RECORDS_PER_COPY
            # that means we have read all the records in the csv and we need to stop the iteration.
            if table_count < RECORDS_PER_COPY:
                break

        return validated_datasets

    def _validate_min_max_restriction(self, cdes_with_min_max, db):
        for cde, min_max in cdes_with_min_max.items():
            min_value, max_value = min_max
            cde_invalid_values = db.execute(
                f"SELECT \"{cde}\" FROM {self.table.fullname} WHERE \"{cde}\" NOT BETWEEN '{min_value}' AND '{max_value}' "
            ).fetchone()
            if cde_invalid_values:
                raise Exception(
                    f"In the column: '{cde}' the following values are invalid: '{cde_invalid_values}'"
                )

    def load_csv(
        self,
        csv_path,
        db,
        records=None,
        offset=2,
    ):
        self._validate_csv_contains_eof(csv_path=csv_path)
        db.copy_csv_in_table(
            file_location=csv_path,
            records=records,
            offset=offset,
            table_name=self.table.name,
        )

    def _validate_csv_contains_eof(self, csv_path):
        with open(csv_path, "rb") as f:
            last_line = f.readlines()[-1]
        if not last_line.endswith(b"\n"):
            raise UserInputError(
                f"CSV:'{csv_path}' does not end with a valid EOF delimiter."
            )

    def _validate_enumerations_restriction(self, cdes_with_enumerations, db):
        for cde, enumerations in cdes_with_enumerations.items():
            cde_invalid_values = db.execute(
                f'SELECT "{cde}" from {self.table.fullname} where "{cde}" not in ({str(enumerations)[1:-1]})'
            ).fetchone()
            if cde_invalid_values:
                raise Exception(
                    f"In the column: '{cde}' the following values are invalid: '{cde_invalid_values}'"
                )

    def set_table(self, table):
        self._table = table
