from abc import ABC, abstractmethod
import json
from typing import NamedTuple, Union, List

import sqlalchemy as sql
from sqlalchemy.ext.compiler import compiles

from mipdb.constants import METADATA_SCHEMA, METADATA_TABLE
from mipdb.database import DataBase, Connection
from mipdb.dataelements import CommonDataElement
from mipdb.exceptions import DataBaseError
from mipdb.schema import Schema


@compiles(sql.types.JSON, "monetdb")
def compile_binary_monetdb(type_, compiler, **kw):
    # The monetdb plugin for sqlalchemy doesn't seem to implement the JSON
    # datatype hence we need to teach sqlalchemy how to compile it
    return "JSON"


class SQLTYPES:
    INTEGER = sql.Integer
    STRING = sql.String(255)
    FLOAT = sql.Float
    JSON = sql.types.JSON


STR2SQLTYPE = {"int": SQLTYPES.INTEGER, "text": SQLTYPES.STRING, "real": SQLTYPES.FLOAT}


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
            f"FROM {METADATA_SCHEMA}.schemas "
            "WHERE schemas.code = :code "
            "AND schemas.version = :version "
            "AND schemas.status <> 'DELETED'"
        )
        res = list(db.execute(select, code=code, version=version))
        if len(res) > 1:
            raise DataBaseError(
                f"Got more than one schema ids for {code=} and {version=}."
            )
        if len(res) == 0:
            raise DataBaseError(
                f"Schemas table doesn't have a record with {code=}, {version=}"
            )
        return res[0][0]

    # TODO delete instead of marking as deleted
    def mark_schema_as_deleted(self, code, version, db):
        update = sql.text(
            f"UPDATE {METADATA_SCHEMA}.schemas "
            "SET status = 'DELETED' "
            "WHERE code = :code "
            "AND version = :version "
            "AND status <> 'DELETED'"
        )
        db.execute(update, code=code, version=version)

    def get_next_schema_id(self, db):
        return db.execute(self.schema_id_seq)


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


class PrimaryDataTable(Table):
    def __init__(self, schema: Schema, cdes: List[CommonDataElement]) -> None:
        columns = [sql.Column(cde.code, STR2SQLTYPE[cde.sql_type]) for cde in cdes]
        self._table = sql.Table(
            "primary_data",
            schema._schema,
            *columns,
        )

    @staticmethod
    def insert_dataset(dataset, schema, db):
        # TODO specify dtype based on schema
        # NOTE The 'monetdb' dialect with current database version settings
        # does not support in-place multirow inserts
        dataset.data.to_sql(
            name="primary_data",
            con=db._executor,
            schema=schema.name,
            if_exists="append",
            index=False,
            # method="multi",
            chunksize=1000,
        )


class MetadataTable(Table):
    def __init__(self, schema: Schema) -> None:
        self._schema = schema.name
        self._table = sql.Table(
            METADATA_TABLE,
            schema._schema,
            sql.Column("code", SQLTYPES.STRING, primary_key=True),
            sql.Column("metadata", SQLTYPES.JSON),
        )

    @staticmethod
    def get_values_from_cdes(cdes):
        return [{"code": cde.code, "metadata": cde.metadata} for cde in cdes]

    def insert_values(self, values, db: Union[DataBase, Connection]):
        # Needs to be overridden because sqlalchemy and monetdb are not cooperating
        # well when inserting values to JSON columns
        query = sql.text(
            f'INSERT INTO "{self._schema}".{METADATA_TABLE} VALUES(:code, :metadata)'
        )
        db.execute(query, values)

    def load_from_db(self, db):
        res = db.execute(
            "SELECT code, json.filter(metadata, '$') "
            f'FROM "schema:1.0".{METADATA_TABLE}'
        )
        self.cdes = {
            name: CommonDataElement.from_cde_data(json.loads(val)[0])
            for name, val in res.fetchall()
        }
