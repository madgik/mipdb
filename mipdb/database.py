from abc import abstractmethod, ABC
from contextlib import contextmanager
from typing import Union

import sqlalchemy as sql

from mipdb.constants import METADATA_SCHEMA
from mipdb.exceptions import DataBaseError


def get_db_config():
    config = {
        "ip": "172.17.0.1",
        "port": 50123,
        "dbfarm": "db",
        "username": "monetdb",
        "password": "monetdb",
    }
    return config


class Connection(ABC):
    """Abstract class representing a database connection interface."""

    @abstractmethod
    def create_schema(self, schema_name):
        pass

    @abstractmethod
    def drop_schema(self, schema_name):
        pass

    @abstractmethod
    def get_schemas(self):
        pass

    @abstractmethod
    def get_schema_id(self, code, version):
        pass

    @abstractmethod
    def create_table(self, table):
        pass

    @abstractmethod
    def insert_values_to_table(self, values, table):
        pass

    @abstractmethod
    def get_dataset_id(self, code, schema_id):
        pass

    @abstractmethod
    def execute(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_current_user(self):
        pass


class DataBase(ABC):
    """Abstract class representing a database interface."""

    @abstractmethod
    def create_schema(self, schema_name):
        pass

    @abstractmethod
    def drop_schema(self, schema_name):
        pass

    @abstractmethod
    def get_dataset_id(self, code, schema_id):
        pass

    @abstractmethod
    def get_schemas(self):
        pass

    @abstractmethod
    def get_datasets(self):
        pass

    @abstractmethod
    def create_table(self, table):
        pass

    @abstractmethod
    def insert_values_to_table(self, values, table):
        pass

    @abstractmethod
    def begin(self):
        pass

    @abstractmethod
    def execute(self, *args, **kwargs):
        pass

    def select_table(self, table):
        pass

    def get_columns(self, table):
        pass

    @abstractmethod
    def get_current_user(self):
        pass

    @abstractmethod
    def get_executor(self):
        pass


def handle_errors(func):
    """Decorator for any function susceptible to raise a DB related exception.
    Wraps function with an exception handling contextmanager which catches DB
    exceptions and reraises them using a DataBaseError instance."""

    @contextmanager
    def _handle_errors():
        try:
            yield
        except sql.exc.OperationalError as exc:
            _, msg = exc.orig.args[0].split("!")
            raise DataBaseError(msg)
        except sql.exc.IntegrityError as exc:
            _, msg = exc.orig.args[0].split("!")
            raise DataBaseError(msg)

    def wrapper(*args, **kwargs):
        with _handle_errors():
            return func(*args, **kwargs)

    return wrapper


class DBExecutorMixin(ABC):
    """Since SQLAlchemy's Engine and Connection object interfaces have a
    significant overlap, we can avoid code duplication by defining the current
    mixin abstract class. Subclasses are required to implement the execute
    method. In practice this is done by delegating to the execute method of
    either an Engine or a Connection.

    Subclasses are required to have an _executor attribute of type Engine or
    Connection.

    Remark: creating tables using the execute method doesn't seem to work
    because tables need to be already bound to a connectable, which in our case
    they aren't. Hence a small hack is needed to implement create_table."""

    _executor: Union[sql.engine.Engine, sql.engine.Connection]

    @abstractmethod
    def execute(self, *args, **kwargs) -> list:
        pass

    def create_schema(self, schema_name):
        self.execute(sql.schema.CreateSchema(schema_name))

    def drop_schema(self, schema_name):
        self.execute(f'DROP SCHEMA "{schema_name}" CASCADE')

    def get_schema_id(self, code, version):
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
        res = list(self.execute(select, code=code, version=version))
        if len(res) > 1:
            raise DataBaseError(
                f"Got more than one schema ids for {code=} and {version=}."
            )
        if len(res) == 0:
            raise DataBaseError(
                f"Schemas table doesn't have a record with {code=}, {version=}"
            )
        return res[0][0]

    def get_dataset_id(self, code, schema_id):
        select = sql.text(
            "SELECT datasets.dataset_id "
            f"FROM {METADATA_SCHEMA}.datasets "
            "WHERE datasets.code = :code "
            "AND datasets.schema_id = :schema_id "
            "AND datasets.status <> 'DELETED'"
        )
        res = list(self.execute(select, code=code, schema_id=schema_id))
        if len(res) > 1:
            raise DataBaseError(
                f"Got more than one dataset ids for {code=} and {schema_id=}."
            )
        if len(res) == 0:
            raise DataBaseError(
                f"Datasets table doesn't have a record with {code=}, {schema_id=}"
            )
        return res[0][0]

    def get_schemas(self):
        res = self.execute("SELECT name FROM sys.schemas WHERE system=FALSE")
        return [schema for schema, *_ in res]

    def get_datasets(self):
        res = self.execute(
            "SELECT datasets.code "
            f"FROM {METADATA_SCHEMA}.datasets "
        )
        return [dataset for dataset, *_ in res]

    @handle_errors
    def create_table(self, table):
        table.create(bind=self._executor)

    def insert_values_to_table(self, table, values):
        self.execute(table.insert(), values)

    def select_table(self, table):
        res = self.execute(
            "SELECT * "
            f"FROM {METADATA_SCHEMA}.{table.name} "
        )
        return list(res)

    def get_columns(self, table):
        res = self.execute(
            "SELECT columns.name FROM tables "
            "INNER JOIN columns ON tables.id=columns.table_id "
            f"where tables.name= '{table.name}' and system = false;"
        )
        return [column for column, *_ in res]

    def get_executor(self):
        return self._executor

    def get_current_user(self):
        user, *_ = self.execute("SELECT CURRENT_USER").fetchone()
        return user


class MonetDBConnection(DBExecutorMixin, Connection):
    """Concrete connection object returned by MonetDB's begin contextmanager.
    This object offers the same interface as MonetDB, with respect to query
    execution, and is used to execute multiple queries within transaction
    boundaries. Gets all its query executing methods from DBExecutorMixin."""

    def __init__(self, conn: sql.engine.Connection) -> None:
        self._executor = conn

    @handle_errors
    def execute(self, query, *args, **kwargs) -> list:
        """Wrapper around SQLAlchemy's execute. Required because pymonetdb
        returns None when the result is empty, instead of [] which make more
        sense and agrees with sqlite behaviour."""
        return self._executor.execute(query, *args, **kwargs) or []


class MonetDB(DBExecutorMixin, DataBase):
    """Concrete DataBase object connecting to a MonetDB instance. Gets all its
    query executing methods from DBExecutorMixin."""

    def __init__(self, url: str, echo=False) -> None:
        self._executor = sql.create_engine(url, echo=echo)

    @classmethod
    def from_config(self, dbconfig) -> "MonetDB":
        username = dbconfig["username"]
        password = dbconfig["password"]
        ip = dbconfig["ip"]
        port = dbconfig["port"]
        dbfarm = dbconfig["dbfarm"]
        url = f"monetdb://{username}:{password}@localhost:{port}/{dbfarm}:"
        return MonetDB(url)

    @handle_errors
    def execute(self, query, *args, **kwargs) -> list:
        """Wrapper around SQLAlchemy's execute. Required because pymonetdb
        returns None when the result is empty, instead of [] which make more
        sense and agrees with sqlite behaviour."""
        return self._executor.execute(query, *args, **kwargs) or []

    @contextmanager
    def begin(self) -> MonetDBConnection:
        """Context manager returning a connection object. Used to execute
        multiple queries within transaction boundaries."""
        with self._executor.begin() as conn:
            yield MonetDBConnection(conn)
