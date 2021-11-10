from abc import ABC, abstractmethod
import json
from typing import Union, List

import sqlalchemy as sql
from sqlalchemy import ForeignKey
from sqlalchemy.ext.compiler import compiles

from mipdb.database import DataBase, Connection
from mipdb.database import METADATA_SCHEMA
from mipdb.database import METADATA_TABLE
from mipdb.dataelements import CommonDataElement
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

    def drop(self, db: Union[DataBase, Connection]):
        db.drop_table(self._table)


class SchemasTable(Table):
    def __init__(self, schema):
        self.schema_id_seq = sql.Sequence("schema_id_seq", metadata=schema.schema)
        self._table = sql.Table(
            "schemas",
            schema.schema,
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
        return db.get_schema_id(code, version)

    def get_schema_properties(self, schema_id, db):
        return db.get_schema_properties(schema_id)

    def set_schema_properties(self, properties, schema_id, db):
        db.set_schema_properties(properties, schema_id)

    def get_schema_status(self, schema_id, db):
        return db.get_schema_status(schema_id)

    def set_schema_status(self, status, schema_id, db):
        db.update_schema_status(status, schema_id)

    def delete_schema(self, code, version, db):
        delete = sql.text(
            f"DELETE FROM {METADATA_SCHEMA}.schemas "
            "WHERE code = :code "
            "AND version = :version "
        )
        db.execute(delete, code=code, version=version)

    def get_next_schema_id(self, db):
        return db.execute(self.schema_id_seq)


class DatasetsTable(Table):
    def __init__(self, schema):
        self.dataset_id_seq = sql.Sequence("dataset_id_seq", metadata=schema.schema)
        self._table = sql.Table(
            "datasets",
            schema.schema,
            sql.Column(
                "dataset_id",
                SQLTYPES.INTEGER,
                self.dataset_id_seq,
                primary_key=True,
            ),
            sql.Column(
                "schema_id",
                SQLTYPES.INTEGER,
                ForeignKey("schemas.schema_id"),
                nullable=False,
            ),
            sql.Column("code", SQLTYPES.STRING, nullable=False),
            sql.Column("label", SQLTYPES.STRING),
            sql.Column("status", SQLTYPES.STRING, nullable=False),
            sql.Column("properties", SQLTYPES.JSON),
        )

    def get_datasets(self, db, schema_id=None):
        return db.get_datasets(schema_id)

    def get_dataset_properties(self, dataset_id, db):
        return db.get_dataset_properties(dataset_id)

    def set_dataset_properties(self, properties, dataset_id, db):
        db.set_dataset_properties(properties, dataset_id)

    def delete_dataset(self, dataset_id, schema_id, db):
        delete = sql.text(
            f"DELETE FROM {METADATA_SCHEMA}.datasets "
            "WHERE dataset_id = :dataset_id "
            "AND schema_id = :schema_id "
        )
        db.execute(delete, dataset_id=dataset_id, schema_id=schema_id)

    def get_next_dataset_id(self, db):
        return db.execute(self.dataset_id_seq)

    def get_dataset_status(self, schema_id, db):
        return db.get_dataset_status(schema_id)

    def set_dataset_status(self, status, dataset_id, db):
        db.update_dataset_status(status, dataset_id)

    def get_dataset_id(self, code, schema_id, db):
        return db.get_dataset_id(code, schema_id)


class ActionsTable(Table):
    def __init__(self, schema):
        self.action_id_seq = sql.Sequence("action_id_seq", metadata=schema.schema)
        self._table = sql.Table(
            "actions",
            schema.schema,
            sql.Column(
                "action_id",
                SQLTYPES.INTEGER,
                self.action_id_seq,
                primary_key=True,
            ),
            sql.Column("action", SQLTYPES.JSON),
        )

    def insert_values(self, values, db: Union[DataBase, Connection]):
        # Needs to be overridden because sqlalchemy and monetdb are not cooperating
        # well when inserting values to JSON columns
        query = sql.text(
            f'INSERT INTO "{METADATA_SCHEMA}".actions VALUES(:action_id, :action)'
        )
        db.execute(query, values)

    def get_next_id(self, db):
        return db.execute(self.action_id_seq)


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
            sql.Column(cde.code, STR2SQLTYPE[json.loads(cde.metadata)["sql_type"]])
            for cde in cdes
        ]
        table = sql.Table(
            "primary_data",
            schema.schema,
            *columns,
        )
        new_table = cls()
        new_table.set_table(table)
        return new_table

    @classmethod
    def from_db(cls, schema: Schema, db: DataBase) -> "PrimaryDataTable":
        table = sql.Table(
            "primary_data", schema.schema, autoload_with=db.get_executor()
        )
        new_table = cls()
        new_table.set_table(table)
        return new_table

    def insert_dataset(self, dataset, db):
        values = dataset.to_dict()
        self.insert_values(values, db)

    def remove_dataset(self, dataset_name, schema_full_name, db):
        delete = sql.text(
            f'DELETE FROM "{schema_full_name}"."primary_data" '
            "WHERE dataset = :dataset_name "
        )
        db.execute(delete, dataset_name=dataset_name)


class MetadataTable(Table):
    def __init__(self, schema: Schema) -> None:
        self.schema = schema.name
        self._table = sql.Table(
            METADATA_TABLE,
            schema.schema,
            sql.Column("code", SQLTYPES.STRING, primary_key=True),
            sql.Column("metadata", SQLTYPES.JSON),
        )

    def set_table(self, table):
        self._table = table

    @classmethod
    def from_db(cls, schema, db):
        res = db.execute(
            "SELECT code, json.filter(metadata, '$') "
            f'FROM "{schema.name}".{METADATA_TABLE}'
        )
        new_table = cls(schema)
        new_table.set_table(
            {
                name: CommonDataElement.from_cde_data(json.loads(val)[0])
                for name, val in res.fetchall()
            }
        )
        return new_table

    @staticmethod
    def get_values_from_cdes(cdes):
        return [{"code": cde.code, "metadata": cde.metadata} for cde in cdes]

    def insert_values(self, values, db: Union[DataBase, Connection]):
        # Needs to be overridden because sqlalchemy and monetdb are not cooperating
        # well when inserting values to JSON columns
        query = sql.text(
            f'INSERT INTO "{self.schema}".{METADATA_TABLE} VALUES(:code, :metadata)'
        )
        db.execute(query, values)
