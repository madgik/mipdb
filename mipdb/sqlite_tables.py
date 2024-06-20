from abc import ABC, abstractmethod
import json

import sqlalchemy as sql
from sqlalchemy import Column, Integer, String, JSON, MetaData
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from mipdb.dataelements import CommonDataElement
from mipdb.exceptions import DataBaseError
from mipdb.sqlite import DataModel, Dataset, SQLiteDB

METADATA_TABLE = "variables_metadata"
PRIMARYDATA_TABLE = "primary_data"

Base = declarative_base()


class Status:
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"


class SQLTYPES:
    INTEGER = Integer
    STRING = String(255)
    FLOAT = sql.Float
    JSON = JSON


def get_metadata_table_name(data_model):
    return f"{data_model}_{METADATA_TABLE}"


def handle_errors(func):
    """Decorator for any function susceptible to raise a DB related exception."""

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exc:  # Use generic Exception to capture all errors
            raise DataBaseError(f"Database error: {exc}")

    return wrapper


class Table(ABC):
    _table: Base

    @abstractmethod
    def __init__(self):
        pass

    @property
    def table(self):
        return self._table

    def create(self, db):
        db.create_table(self._table)

    def exists(self, db):
        return db.table_exists(self._table)

    def insert_values(self, values, db):
        db.insert_values_to_table(self._table, values)

    def delete(self, db):
        db.delete_from(self._table, where_conditions={})

    def get_row_count(self, db):
        return db.get_row_count(self.table.fullname)

    def get_column_distinct(self, column, db):
        return db.get_column_distinct(column, self.table.fullname)

    def drop(self, db):
        db.drop_table(self._table)


class DataModelTable(Table):
    def __init__(self):
        self._table = DataModel.__table__

    def get_data_models(self, db, columns: list = None):
        return db.get_values(table=self._table, columns=columns)

    def get_data_model(self, data_model_id, db, columns: list = None):
        return db.get_data_models_values(
            columns=columns, where_conditions={"data_model_id": data_model_id}
        )

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
        db.delete_from(self._table, where_conditions={"code": code, "version": version})

    def get_next_data_model_id(self, db):
        result = db.get_max_data_model_id()
        return result + 1 if result else 1


class DatasetsTable(Table):
    def __init__(self):
        self._table = Dataset.__table__

    def get_datasets(self, db, columns: list = None):
        return db.get_values(table=self._table, columns=columns, where_conditions={})

    def get_dataset_codes(self, db, data_model_id=None, columns=None):
        result = db.get_values(
            table=self._table,
            columns=columns,
            where_conditions={"data_model_id": data_model_id},
        )
        return [dataset[0] for dataset in result]

    def get_dataset(self, db, dataset_id=None, columns=None):
        return db.get_values(
            table=self._table,
            columns=columns,
            where_conditions={"dataset_id": dataset_id},
        )

    def get_dataset_properties(self, dataset_id, db):
        return db.get_dataset_properties(dataset_id)

    def set_dataset_properties(self, properties, dataset_id, db):
        db.set_dataset_properties(properties, dataset_id)

    def delete_dataset(self, dataset_id, data_model_id, db):
        db.delete_from(
            self._table,
            where_conditions={"dataset_id": dataset_id, "data_model_id": data_model_id},
        )

    def get_next_dataset_id(self, db):
        result = db.get_max_dataset_id()
        return result + 1 if result else 1

    def get_dataset_status(self, data_model_id, db):
        return db.get_dataset_status(data_model_id)

    def set_dataset_status(self, status, dataset_id, db):
        db.update_dataset_status(status, dataset_id)

    def get_dataset_id(self, code, data_model_id, db):
        return db.get_dataset_id(code, data_model_id)


class MetadataTable(Table):
    def __init__(self, data_model):
        self.name = get_metadata_table_name(data_model)
        self._table = sql.Table(
            self.name,
            Base.metadata,
            sql.Column("code", SQLTYPES.STRING, primary_key=True),
            sql.Column("metadata", SQLTYPES.JSON),
            extend_existing=True,
        )

    def set_table(self, table):
        self._table = table

    @classmethod
    def from_db(cls, data_model, db):
        res = db.get_metadata(data_model)
        new_table = cls(data_model)
        new_table.set_table(
            {
                code: CommonDataElement.from_metadata(metadata)
                for code, metadata in res.items()
            }
        )
        return new_table

    @staticmethod
    def get_values_from_cdes(cdes):
        return [{"code": cde.code, "metadata": cde.metadata} for cde in cdes]

    def insert_values(self, values, db):
        # Needs to be overridden because sqlalchemy and monetdb are not cooperating
        # well when inserting values to JSON columns
        db.execute(f'INSERT INTO "{self.name}" VALUES(:code, :metadata)', values)
