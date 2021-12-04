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


class DataModelTable(Table):
    def __init__(self, schema):
        self.data_model_id_seq = sql.Sequence(
            "data_model_id_seq", metadata=schema.schema
        )
        self._table = sql.Table(
            "data_models",
            schema.schema,
            sql.Column(
                "data_model_id",
                SQLTYPES.INTEGER,
                self.data_model_id_seq,
                primary_key=True,
            ),
            sql.Column("code", SQLTYPES.STRING, nullable=False),
            sql.Column("version", SQLTYPES.STRING, nullable=False),
            sql.Column("label", SQLTYPES.STRING),
            sql.Column("status", SQLTYPES.STRING, nullable=False),
            sql.Column("properties", SQLTYPES.JSON),
        )

    def get_data_models(self, db, columns: list = None):
        if columns and not set(columns).issubset(self.table.columns.keys()):
            non_existing_columns = list(set(columns) - set(self.table.columns.keys()))
            raise ValueError(
                f"The columns: {non_existing_columns} do not exist in the data models schema"
            )
        return db.get_data_models(columns)

    def get_dataset_count_by_data_model_id(self, db):
        return db.get_dataset_count_by_data_model_id()

    def get_data_model_id(self, code, version, db):
        return db.get_data_model_id(code, version)

    def get_data_model_properties(self, data_model_id, db):
        return db.get_data_model_properties(data_model_id)

    def set_data_model_properties(self, properties, data_model_id, db):
        db.set_data_model_properties(properties, data_model_id)

    def get_data_model_status(self, data_model_id, db):
        return db.get_data_model_status(data_model_id)

    def set_data_model_status(self, status, data_model_id, db):
        db.update_data_model_status(status, data_model_id)

    def delete_data_model(self, code, version, db):
        delete = sql.text(
            f"DELETE FROM {METADATA_SCHEMA}.data_models "
            "WHERE code = :code "
            "AND version = :version "
        )
        db.execute(delete, code=code, version=version)

    def get_next_data_model_id(self, db):
        return db.execute(self.data_model_id_seq)


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
                "data_model_id",
                SQLTYPES.INTEGER,
                ForeignKey("data_models.data_model_id"),
                nullable=False,
            ),
            sql.Column("code", SQLTYPES.STRING, nullable=False),
            sql.Column("label", SQLTYPES.STRING),
            sql.Column("status", SQLTYPES.STRING, nullable=False),
            sql.Column("properties", SQLTYPES.JSON),
        )

    def get_datasets(self, db, data_model_id=None, columns=None):
        if columns and not set(columns).issubset(self.table.columns.keys()):
            non_existing_columns = list(set(columns) - set(self.table.columns.keys()))
            raise ValueError(
                f"The columns: {non_existing_columns} do not exist in the datasets schema"
            )
        return db.get_datasets(data_model_id, columns)

    def get_data_count_by_dataset(self, data_model_fullname, db):
        return db.get_data_count_by_dataset(data_model_fullname)

    def get_dataset_properties(self, dataset_id, db):
        return db.get_dataset_properties(dataset_id)

    def set_dataset_properties(self, properties, dataset_id, db):
        db.set_dataset_properties(properties, dataset_id)

    def delete_dataset(self, dataset_id, data_model_id, db):
        delete = sql.text(
            f"DELETE FROM {METADATA_SCHEMA}.datasets "
            "WHERE dataset_id = :dataset_id "
            "AND data_model_id = :data_model_id "
        )
        db.execute(delete, dataset_id=dataset_id, data_model_id=data_model_id)

    def get_next_dataset_id(self, db):
        return db.execute(self.dataset_id_seq)

    def get_dataset_status(self, data_model_id, db):
        return db.get_dataset_status(data_model_id)

    def set_dataset_status(self, status, dataset_id, db):
        db.update_dataset_status(status, dataset_id)

    def get_dataset_id(self, code, data_model_id, db):
        return db.get_dataset_id(code, data_model_id)


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
