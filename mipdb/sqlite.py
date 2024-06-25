from contextlib import contextmanager
from typing import List, Any, Dict
from enum import Enum
import json

import sqlalchemy as sql
from sqlalchemy import MetaData, ForeignKey, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import MultipleResultsFound

from mipdb.exceptions import DataBaseError

METADATA_TABLE = "variables_metadata"
PRIMARYDATA_TABLE = "primary_data"
metadata = MetaData()
Base = declarative_base(metadata=metadata)


class Status:
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"


class DBType(Enum):
    monetdb = "monetdb"
    sqlite = "sqlite"


class SQLTYPES:
    INTEGER = sql.Integer
    STRING = sql.String(255)
    FLOAT = sql.Float
    JSON = sql.types.JSON


def handle_errors(func):
    """Decorator for any function susceptible to raise a DB related exception."""

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exc:  # Use generic Exception to capture all errors
            raise DataBaseError(f"Database error: {exc}")

    return wrapper


class DataModel(Base):
    __tablename__ = "data_models"
    data_model_id = sql.Column(sql.Integer, primary_key=True, autoincrement=True)
    code = sql.Column(sql.String, nullable=False)
    version = sql.Column(sql.String, nullable=False)
    label = sql.Column(sql.String, nullable=False)
    status = sql.Column(sql.String, nullable=False)
    properties = sql.Column(sql.JSON, nullable=True)


class Dataset(Base):
    __tablename__ = "datasets"
    dataset_id = sql.Column(sql.Integer, primary_key=True, autoincrement=True)
    data_model_id = sql.Column(
        sql.Integer, ForeignKey("data_models.data_model_id"), nullable=False
    )
    code = sql.Column(sql.String, nullable=False)
    label = sql.Column(sql.String, nullable=False)
    status = sql.Column(sql.String, nullable=False)
    csv_path = sql.Column(sql.String, nullable=False)
    properties = sql.Column(sql.JSON, nullable=True)


class SQLiteDB:
    """Class representing a SQLite database interface."""

    def __init__(self, url: str, echo=False) -> None:
        self._executor = sql.create_engine(url, echo=echo)
        self.Session = sessionmaker(bind=self._executor)

    @classmethod
    def from_config(cls, dbconfig: dict) -> "SQLiteDB":
        db_path = dbconfig["db_path"]
        url = f"sqlite:///{db_path}"
        return SQLiteDB(url)

    @handle_errors
    def execute(self, query: str, *args, **kwargs) -> None:
        conn = self._executor.connect()
        conn.execute(sql.text(query), *args, **kwargs)
        conn.close()

    @handle_errors
    def execute_fetchall(self, query: str, *args, **kwargs) -> List[dict]:
        conn = self._executor.connect()
        result = conn.execute(sql.text(query), *args, **kwargs)
        result = result.fetchall() if result else []
        conn.close()
        return result

    def insert_values_to_table(self, table: sql.Table, values: List[dict]) -> None:
        session = self.Session()
        try:
            session.execute(table.insert(), values)
            session.commit()
        finally:
            session.close()

    def get_data_model_status(self, data_model_id: int) -> Any:
        session = self.Session()
        try:
            query = session.query(DataModel.status).filter(
                DataModel.data_model_id == data_model_id
            )
            result = query.one_or_none()
        finally:
            session.close()
        if result:
            return result[0]
        return None

    def update_data_model_status(self, status: str, data_model_id: int) -> None:
        session = self.Session()
        try:
            session.query(DataModel).filter(
                DataModel.data_model_id == data_model_id
            ).update({"status": status})
            session.commit()
        finally:
            session.close()

    def get_dataset_status(self, dataset_id: int) -> Any:
        session = self.Session()
        try:
            query = session.query(Dataset.status).filter(
                Dataset.dataset_id == dataset_id
            )
            result = query.one_or_none()
        finally:
            session.close()
        if result:
            return result[0]
        return None

    def get_metadata(self, data_model: str) -> dict:
        session = self.Session()
        try:
            table = sql.Table(
                f"{data_model}_{METADATA_TABLE}",
                Base.metadata,
                sql.Column("code", SQLTYPES.STRING, primary_key=True),
                sql.Column("metadata", SQLTYPES.JSON),
                extend_existing=True,
            )
            query = session.query(table.c.code, table.c.metadata)
            res = query.all()
        finally:
            session.close()
        return {row.code: row.metadata for row in res}

    def update_dataset_status(self, status: str, dataset_id: int) -> None:
        session = self.Session()
        try:
            session.query(Dataset).filter(Dataset.dataset_id == dataset_id).update(
                {"status": status}
            )
            session.commit()
        finally:
            session.close()

    def get_dataset(self, dataset_id: int, columns: List[str]) -> Any:
        session = self.Session()
        try:
            query = session.query(*[getattr(Dataset, col) for col in columns]).filter(
                Dataset.dataset_id == dataset_id
            )
            result = query.one_or_none()
        finally:
            session.close()
        return result

    def get_data_model(self, data_model_id: int, columns: List[str]) -> Any:
        session = self.Session()
        try:
            query = session.query(*[getattr(DataModel, col) for col in columns]).filter(
                DataModel.data_model_id == data_model_id
            )
            result = query.one_or_none()
        finally:
            session.close()
        return result

    def get_values(
        self, table, columns: List[str] = None, where_conditions: Dict[str, Any] = None
    ) -> List[dict]:
        session = self.Session()
        try:
            if columns is None:
                columns = [
                    col.name for col in table.columns
                ]  # Get all columns if none are specified
            query = session.query(*[getattr(table.c, col) for col in columns])

            if where_conditions:
                for col, value in where_conditions.items():
                    query = query.filter(getattr(table.c, col) == value)

            result = query.all()
        finally:
            session.close()
        return result

    def get_data_models(self, columns: List[str]) -> List[dict]:
        session = self.Session()
        try:
            query = session.query(*[getattr(DataModel, col) for col in columns])
            result = query.all()
        finally:
            session.close()
        return result

    def get_dataset_count_by_data_model_id(self) -> List[dict]:
        session = self.Session()
        try:
            query = session.query(
                Dataset.data_model_id,
                sql.func.count(Dataset.data_model_id).label("count"),
            ).group_by(Dataset.data_model_id)
            result = query.all()
        finally:
            session.close()
        return result

    def get_row_count(self, table: str) -> int:
        session = self.Session()
        try:
            query = session.query(sql.func.count()).select_from(sql.text(table))
            result = query.scalar()
        finally:
            session.close()
        return result

    def drop_table(self, table: str) -> None:
        session = self.Session()
        try:
            table = sql.Table(table, metadata, autoload_with=self._executor)
            table.drop(bind=self._executor)
            session.commit()
        finally:
            session.close()

    def delete_from(self, table, where_conditions: Dict[str, Any]) -> None:
        session = self.Session()
        try:
            query = session.query(table)
            if where_conditions:
                for col, value in where_conditions.items():
                    query = query.filter(getattr(table.c, col) == value)

            query.delete(synchronize_session=False)
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()

    def get_dataset_properties(self, dataset_id: int) -> Any:
        session = self.Session()
        try:
            query = session.query(Dataset.properties).filter(
                Dataset.dataset_id == dataset_id
            )
            result = query.one_or_none()
        finally:
            session.close()

        return result[0] if result else {}

    def get_data_model_properties(self, data_model_id: int) -> Any:
        session = self.Session()
        try:
            query = session.query(DataModel.properties).filter(
                DataModel.data_model_id == data_model_id
            )
            result = query.one_or_none()
        finally:
            session.close()

        return result[0] if result else {}

    def set_data_model_properties(self, properties: dict, data_model_id: int) -> None:
        session = self.Session()
        try:
            session.query(DataModel).filter(
                DataModel.data_model_id == data_model_id
            ).update({"properties": properties})
            session.commit()
        finally:
            session.close()

    def set_dataset_properties(self, properties: dict, dataset_id: int) -> None:
        session = self.Session()
        try:
            session.query(Dataset).filter(Dataset.dataset_id == dataset_id).update(
                {"properties": properties}
            )
            session.commit()
        finally:
            session.close()

    def get_data_model_id(self, code: str, version: str) -> int:
        session = self.Session()
        try:
            query = session.query(DataModel.data_model_id).filter(
                DataModel.code == code, DataModel.version == version
            )
            data_model_id = query.scalar()
        except MultipleResultsFound:
            raise DataBaseError(
                f"Got more than one data_model ids for {code=} and {version=}."
            )
        finally:
            session.close()

        if not data_model_id:
            raise DataBaseError(
                f"Data_models table doesn't have a record with {code=}, {version=}"
            )

        return data_model_id

    def get_max_data_model_id(self) -> int:
        session = self.Session()
        try:
            result = session.query(sql.func.max(DataModel.data_model_id)).scalar()
        finally:
            session.close()
        return result

    def get_max_dataset_id(self) -> int:
        session = self.Session()
        try:
            result = session.query(sql.func.max(Dataset.dataset_id)).scalar()
        finally:
            session.close()
        return result

    def get_dataset_id(self, code, data_model_id) -> int:
        session = self.Session()
        try:
            query = session.query(Dataset.dataset_id).filter(
                Dataset.code == code, Dataset.data_model_id == data_model_id
            )
            dataset_id = query.scalar()
        except MultipleResultsFound:
            raise DataBaseError(
                f"Got more than one dataset ids for {code=} and {data_model_id=}."
            )
        finally:
            session.close()

        if not dataset_id:
            raise DataBaseError(
                f"Datasets table doesn't have a record with {code=}, {data_model_id=}"
            )

        return dataset_id

    def table_exists(self, table) -> bool:
        return table.exists(bind=self._executor)

    def create_table(self, table: sql.Table) -> None:
        table.create(bind=self._executor)

    def get_all_tables(self) -> List[str]:
        inspector = inspect(self._executor)
        return inspector.get_table_names()
