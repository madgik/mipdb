import json
from typing import List

import sqlalchemy as sql

from mipdb.dataelements import CommonDataElement
from mipdb.exceptions import DataBaseError
from mipdb.duckdb.schema import Schema


class SQLTYPES:
    INTEGER = sql.Integer
    STRING = sql.String(255)
    FLOAT = sql.Float


STR2SQLTYPE = {
    "int": SQLTYPES.INTEGER,
    "text": SQLTYPES.STRING,
    "real": SQLTYPES.FLOAT,
}


class PrimaryDataTable:
    def __init__(self) -> None:
        self._table: sql.Table | None = None

    @property
    def table(self) -> sql.Table:
        if self._table is None:
            raise DataBaseError("Primary data table is not initialized")
        return self._table

    def set_table(self, table: sql.Table) -> None:
        self._table = table

    @classmethod
    def from_cdes(
        cls, schema: Schema, cdes: List[CommonDataElement]
    ) -> "PrimaryDataTable":
        columns = [
            sql.Column(
                cde.code,
                STR2SQLTYPE[json.loads(cde.metadata)["sql_type"]],
                quote=True,
            )
            for cde in cdes
        ]
        table = sql.Table(
            f"{schema.db_name}__primary_data",
            sql.MetaData(),
            *columns,
        )
        new_table = cls()
        new_table.set_table(table)
        return new_table

    @classmethod
    def from_db(cls, schema: Schema, db) -> "PrimaryDataTable":
        column_names = db.get_table_column_names(
            f"{schema.db_name}__primary_data"
        )
        if not column_names:
            raise DataBaseError(
                f"Table primary_data does not exist in schema '{schema.name}'"
            )

        table = sql.Table(
            f"{schema.db_name}__primary_data",
            sql.MetaData(),
            *[sql.Column(column_name, quote=True) for column_name in column_names],
        )
        new_table = cls()
        new_table.set_table(table)
        return new_table

    def create(self, db) -> None:
        db.create_table(self.table)

    def drop(self, db) -> None:
        db.drop_table(self.table)

    def exists(self, db) -> bool:
        return db.table_exists(self.table)

    def insert_values(self, values, db) -> None:
        db.insert_values_to_table(self.table, values)

    def remove_dataset(self, dataset_name: str, db) -> None:
        db.delete_from(self.table, {"dataset": dataset_name})
