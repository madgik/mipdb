from abc import ABC, abstractmethod
from typing import NamedTuple, Union, List

import sqlalchemy as sql
from sqlalchemy.ext.compiler import compiles

from mipdb.database import DataBase, Connection
from mipdb.dataelements import CommonDataElement, CategoricalCDE, NumericalCDE
from mipdb.exceptions import DataBaseError
from mipdb.schema import Schema


class SQLTYPES:
    INTEGER = sql.Integer
    STRING = sql.String(255)
    FLOAT = sql.Float
    JSON = sql.types.JSON


STR2SQLTYPE = {"int": SQLTYPES.INTEGER, "text": SQLTYPES.STRING, "real": SQLTYPES.FLOAT}


@compiles(sql.types.JSON, "monetdb")
def compile_binary_sqlite(type_, compiler, **kw):
    # The monetdb plugin for sqlalchemy doesn't seem to implement the JSON
    # datatype hence we need to teach sqlalchemy how to compile it
    return "JSON"


class Table(ABC):
    _table: sql.Table

    @abstractmethod
    def __init__(self, schema):
        pass

    @property
    def table(self):
        return self._table

    def create(self, db: Union[DataBase, Connection]):
        db.create_table(self._table)

    def insert_values(self, values, db: Union[DataBase, Connection]):
        db.insert_values_to_table(self._table, values)


class SchemasTable(Table):
    def __init__(self, schema):
        self.schema_id_seq = sql.Sequence("schema_id_seq", metadata=schema._schema)
        self._table = sql.Table(
            "schemas",
            schema._schema,
            sql.Column(
                "schema_id",
                SQLTYPES.INTEGER,
                self.schema_id_seq,
                primary_key=True,
            ),
            sql.Column("code", SQLTYPES.STRING, nullable=False),
            sql.Column("version", SQLTYPES.STRING, nullable=False),
            sql.Column("label", SQLTYPES.STRING),
            sql.Column("status", SQLTYPES.STRING, nullable=False),
            sql.Column("properties", SQLTYPES.JSON),
        )

    def get_schema_id(self, code, version, db):
        # I am forced to use textual SQL instead of SQLAlchemy objects because
        # of two bugs. The first one is in sqlalchemy_monetdb which translates
        # the 'not equal' operator as != instead of the correct <>. The second
        # bug is in Monet DB where column names of level >= 3 are not yet
        # implemented.
        select = sql.text(
            "SELECT schemas.schema_id "
            "FROM mipdb_metadata.schemas "
            "WHERE schemas.code = :code "
            "AND schemas.version = :version "
            "AND schemas.status <> 'DELETED'"
        )
        res = list(db._execute(select, code=code, version=version))
        if len(res) > 1:
            raise DataBaseError(
                f"Got more than one schema ids for {code=} and {version=}."
            )
        if len(res) == 0:
            raise DataBaseError(
                f"Schemas table doesn't have a record with {code=}, {version=}"
            )
        return res[0][0]

    def mark_schema_as_deleted(self, code, version, db):
        update = sql.text(
            "UPDATE mipdb_metadata.schemas "
            "SET status = 'DELETED' "
            "WHERE code = :code "
            "AND version = :version "
            "AND status <> 'DELETED'"
        )
        db._execute(update, code=code, version=version)

    def get_next_schema_id(self, db):
        return db._execute(self.schema_id_seq)


class DatasetsTable(Table):
    def __init__(self, schema):
        self._table = sql.Table(
            "datasets",
            schema._schema,
            sql.Column(
                "dataset_id",
                SQLTYPES.INTEGER,
                sql.Sequence("dataset_id_seq", metadata=schema._schema),
                primary_key=True,
            ),
            sql.Column(
                "schema_id",
                SQLTYPES.INTEGER,
                # sql.ForeignKey("schemas.schema_id"),
                nullable=False,
            ),
            sql.Column("version", SQLTYPES.STRING, nullable=False),
            sql.Column("label", SQLTYPES.STRING),
            sql.Column("status", SQLTYPES.STRING, nullable=False),
            sql.Column("properties", SQLTYPES.JSON),
        )


class ActionsTable(Table):
    def __init__(self, schema):
        self._table = sql.Table(
            "actions",
            schema._schema,
            sql.Column(
                "action_id",
                SQLTYPES.INTEGER,
                sql.Sequence("action_id_seq", metadata=schema._schema),
                primary_key=True,
            ),
            sql.Column("description", SQLTYPES.STRING, nullable=False),
            sql.Column("user", SQLTYPES.STRING, nullable=False),
            sql.Column("date", SQLTYPES.STRING, nullable=False),
        )


class VariablesTable(Table):
    def __init__(self, schema: Schema) -> None:
        self._table = sql.Table(
            "variables",
            schema._schema,
            sql.Column("code", sql.String(128), primary_key=True),
            sql.Column("label", sql.String(128)),
        )

    @staticmethod
    def get_values_from_cdes(cdes):
        return [{"code": cde.code, "label": cde.label} for cde in cdes]


class EnumerationsTable(Table):
    def __init__(self, schema: Schema):
        self._table = sql.Table(
            "enumerations",
            schema._schema,
            sql.Column("code", sql.String(128)),
            sql.Column("variable_code", sql.String(128)),
            sql.Column("label", sql.String(128)),
        )

    @staticmethod
    def get_values_from_cdes(cdes):
        nominal_cdes = [cde for cde in cdes if isinstance(cde, CategoricalCDE)]
        return [
            {
                "variable_code": cde.code,
                "code": enum["code"],
                "label": enum["label"],
            }
            for cde in nominal_cdes
            for enum in cde.enumerations
        ]


class DomainsTable(Table):
    def __init__(self, schema: Schema):
        self._table = sql.Table(
            "domains",
            schema._schema,
            sql.Column("variable_code", sql.String(128)),
            sql.Column("min", sql.Float),
            sql.Column("max", sql.Float),
        )

    @staticmethod
    def get_values_from_cdes(cdes):
        numerical_cdes = [cde for cde in cdes if isinstance(cde, NumericalCDE)]
        return [
            {
                "variable_code": cde.code,
                "min": cde.minValue,
                "max": cde.maxValue,
            }
            for cde in numerical_cdes
            if cde.minValue or cde.maxValue
        ]


class UnitsTable(Table):
    def __init__(self, schema: Schema) -> None:
        self._table = sql.Table(
            "units",
            schema._schema,
            sql.Column("variable_code", sql.String(128)),
            sql.Column("units", sql.String(128)),
        )

    @staticmethod
    def get_values_from_cdes(cdes):
        numerical_cdes = [cde for cde in cdes if isinstance(cde, NumericalCDE)]
        return [
            {
                "variable_code": cde.code,
                "units": cde.units,
            }
            for cde in numerical_cdes
            if cde.units
        ]


class PrimaryDataTable(Table):
    def __init__(self, schema: Schema, cdes: List[CommonDataElement]) -> None:
        columns = [sql.Column(cde.code, STR2SQLTYPE[cde.sql_type]) for cde in cdes]
        self._table = sql.Table(
            "primary_data",
            schema._schema,
            *columns,
        )
