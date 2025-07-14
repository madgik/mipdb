from typing import List, Any, Dict
from enum import Enum

import sqlalchemy as sql
from sqlalchemy import MetaData, ForeignKey, inspect, text, select, func
from sqlalchemy.ext.declarative import declarative_base
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
    JSON = sql.JSON


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
        sql.Integer,
        ForeignKey("data_models.data_model_id"),
        nullable=False,
    )
    code = sql.Column(sql.String, nullable=False)
    label = sql.Column(sql.String, nullable=False)
    status = sql.Column(sql.String, nullable=False)
    csv_path = sql.Column(sql.String, nullable=True)
    properties = sql.Column(sql.JSON, nullable=True)


class SQLiteDB:
    """Class representing a SQLite database interface."""

    def __init__(self, url: str, echo=False) -> None:
        self._executor = sql.create_engine(url, echo=echo)
        self.Session = sessionmaker(bind=self._executor, future=True)

    @classmethod
    def from_config(cls, dbconfig: Dict[str, Any]) -> "SQLiteDB":
        db_path = dbconfig["db_path"]
        url = f"sqlite:///{db_path}"
        return SQLiteDB(url)

    def execute(self, query: str, *args, **kwargs) -> List[Any]:
        """Execute a statement without returning rows."""
        with self._executor.connect() as conn:
            conn.execute(text(query), *args, **kwargs)
            conn.commit()
        return []

    def execute_fetchall(self, query: str, *args, **kwargs) -> List[Any]:
        """Execute a query and return all rows."""
        with self._executor.connect() as conn:
            result = conn.execute(text(query), *args, **kwargs)
            return result.fetchall()

    def insert_values_to_table(self, table: sql.Table, values: List[Dict[str, Any]]) -> None:
        session = self.Session()
        try:
            session.execute(table.insert(), values)
            session.commit()
        finally:
            session.close()

    def get_data_model_status(self, data_model_id: int) -> Any:
        session = self.Session()
        try:
            result = session.execute(
                select(Base.metadata.tables["data_models"].c.status)
                .where(Base.metadata.tables["data_models"].c.data_model_id == data_model_id)
            ).scalar_one_or_none()
        finally:
            session.close()
        return result

    def update_data_model_status(self, status: str, data_model_id: int) -> None:
        session = self.Session()
        try:
            session.execute(
                sql.update(Base.metadata.tables["data_models"])
                .where(Base.metadata.tables["data_models"].c.data_model_id == data_model_id)
                .values(status=status)
            )
            session.commit()
        finally:
            session.close()

    def get_dataset_status(self, dataset_id: int) -> Any:
        session = self.Session()
        try:
            result = session.execute(
                select(Base.metadata.tables["datasets"].c.status)
                .where(Base.metadata.tables["datasets"].c.dataset_id == dataset_id)
            ).scalar_one_or_none()
        finally:
            session.close()
        return result

    def get_metadata(self, data_model: str) -> Dict[str, Any]:
        table = sql.Table(
            f"{data_model}_{METADATA_TABLE}",
            Base.metadata,
            sql.Column("code", SQLTYPES.STRING, primary_key=True),
            sql.Column("metadata", SQLTYPES.JSON),
            extend_existing=True,
        )
        session = self.Session()
        try:
            result = session.execute(select(table.c.code, table.c.metadata)).all()
        finally:
            session.close()
        return {code: meta for code, meta in result}

    def update_dataset_status(self, status: str, dataset_id: int) -> None:
        session = self.Session()
        try:
            session.execute(
                sql.update(Base.metadata.tables["datasets"])
                .where(Base.metadata.tables["datasets"].c.dataset_id == dataset_id)
                .values(status=status)
            )
            session.commit()
        finally:
            session.close()

    def get_dataset(self, dataset_id: int, columns: List[str]) -> Any:
        session = self.Session()
        try:
            cols = [getattr(Base.metadata.tables["datasets"].c, col) for col in columns]
            result = session.execute(
                select(*cols).where(Base.metadata.tables["datasets"].c.dataset_id == dataset_id)
            ).one_or_none()
        finally:
            session.close()
        return result

    def get_data_model(self, data_model_id: int, columns: List[str]) -> Any:
        session = self.Session()
        try:
            cols = [getattr(Base.metadata.tables["data_models"].c, col) for col in columns]
            result = session.execute(
                select(*cols).where(Base.metadata.tables["data_models"].c.data_model_id == data_model_id)
            ).one_or_none()
        finally:
            session.close()
        return result

    from sqlalchemy import select

    # ...

    def get_values(
            self,
            table: sql.Table,
            columns: List[str] | None = None,
            where_conditions: Dict[str, Any] | None = None,
    ) -> List[sql.Row]:
        """Return rows (SQLAlchemy Row objects) respecting an optional WHERE."""
        stmt = select(
            *[table.c[col] for col in (columns or [c.name for c in table.columns])]
        )
        if where_conditions:
            for col, val in where_conditions.items():
                stmt = stmt.where(table.c[col] == val)

        with self.Session() as session:
            return session.execute(stmt).all()  # <-- rows, not dicts

    def get_data_models(self, columns: List[str]) -> List[Dict[str, Any]]:
        session = self.Session()
        try:
            cols = [getattr(Base.metadata.tables["data_models"].c, col) for col in columns]
            rows = session.execute(select(*cols)).all()
        finally:
            session.close()
        return [dict(zip(columns, row)) for row in rows]

    def get_dataset_count_by_data_model_id(self) -> List[Dict[str, Any]]:
        session = self.Session()
        try:
            stmt = (
                select(
                    Base.metadata.tables["datasets"].c.data_model_id,
                    func.count(Base.metadata.tables["datasets"].c.data_model_id).label("count"),
                )
                .group_by(Base.metadata.tables["datasets"].c.data_model_id)
            )
            rows = session.execute(stmt).all()
        finally:
            session.close()
        return [dict(data_model_id=row[0], count=row[1]) for row in rows]

    def get_row_count(self, table_name: str) -> int:
        session = self.Session()
        try:
            count = session.execute(select(func.count()).select_from(text(table_name))).scalar_one()
        finally:
            session.close()
        return count

    def drop_table(self, table_name: str) -> None:
        session = self.Session()
        try:
            table = sql.Table(table_name, metadata, autoload_with=self._executor)
            table.drop(bind=self._executor)
            session.commit()
        finally:
            session.close()

    def delete_from(self, table: sql.Table, where_conditions: Dict[str, Any]) -> None:
        session = self.Session()
        try:
            stmt = table.delete()
            if where_conditions:
                for col_name, val in where_conditions.items():
                    stmt = stmt.where(table.c[col_name] == val)
            session.execute(stmt)
            session.commit()
        finally:
            session.close()

    def get_dataset_properties(self, dataset_id: int) -> Any:
        session = self.Session()
        try:
            result = session.execute(
                select(Base.metadata.tables["datasets"].c.properties)
                .where(Base.metadata.tables["datasets"].c.dataset_id == dataset_id)
            ).scalar_one_or_none()
        finally:
            session.close()
        return result or {}

    def get_data_model_properties(self, data_model_id: int) -> Any:
        session = self.Session()
        try:
            result = session.execute(
                select(Base.metadata.tables["data_models"].c.properties)
                .where(Base.metadata.tables["data_models"].c.data_model_id == data_model_id)
            ).scalar_one_or_none()
        finally:
            session.close()
        return result or {}

    def set_data_model_properties(self, properties: Dict[str, Any], data_model_id: int) -> None:
        session = self.Session()
        try:
            session.execute(
                sql.update(Base.metadata.tables["data_models"])
                .where(Base.metadata.tables["data_models"].c.data_model_id == data_model_id)
                .values(properties=properties)
            )
            session.commit()
        finally:
            session.close()

    def set_dataset_properties(self, properties: Dict[str, Any], dataset_id: int) -> None:
        session = self.Session()
        try:
            session.execute(
                sql.update(Base.metadata.tables["datasets"])
                .where(Base.metadata.tables["datasets"].c.dataset_id == dataset_id)
                .values(properties=properties)
            )
            session.commit()
        finally:
            session.close()

    def get_data_model_id(self, code: str, version: str) -> int:
        session = self.Session()
        try:
            stmt = (
                select(Base.metadata.tables["data_models"].c.data_model_id)
                .where(
                    Base.metadata.tables["data_models"].c.code == code,
                    Base.metadata.tables["data_models"].c.version == version,
                )
            )
            data_model_id = session.execute(stmt).scalar_one_or_none()
        except MultipleResultsFound:
            raise DataBaseError(f"Got more than one data_model ids for code={code} and version={version}.")
        finally:
            session.close()
        if not data_model_id:
            raise DataBaseError(f"Data_models table doesn't have a record with code={code}, version={version}")
        return data_model_id

    def get_max_data_model_id(self) -> int:
        session = self.Session()
        try:
            max_id = session.execute(
                select(func.max(Base.metadata.tables["data_models"].c.data_model_id))
            ).scalar_one()
        finally:
            session.close()
        return max_id

    def get_max_dataset_id(self) -> int:
        session = self.Session()
        try:
            max_id = session.execute(
                select(func.max(Base.metadata.tables["datasets"].c.dataset_id))
            ).scalar_one()
        finally:
            session.close()
        return max_id

    def get_dataset_id(self, code: str, data_model_id: int) -> int:
        session = self.Session()
        try:
            stmt = (
                select(Base.metadata.tables["datasets"].c.dataset_id)
                .where(
                    Base.metadata.tables["datasets"].c.code == code,
                    Base.metadata.tables["datasets"].c.data_model_id == data_model_id,
                )
            )
            dataset_id = session.execute(stmt).scalar_one_or_none()
        except MultipleResultsFound:
            raise DataBaseError(f"Got more than one dataset ids for code={code} and data_model_id={data_model_id}.")
        finally:
            session.close()
        if not dataset_id:
            raise DataBaseError(f"Datasets table doesn't have a record with code={code}, data_model_id={data_model_id}")
        return dataset_id

    def table_exists(self, table: sql.Table) -> bool:
        inspector = inspect(self._executor)
        schema = table.schema or None
        return inspector.has_table(table.name, schema)

    def create_table(self, table: sql.Table) -> None:
        table.create(bind=self._executor)

    def get_all_tables(self) -> List[str]:
        inspector = inspect(self._executor)
        return inspector.get_table_names()
