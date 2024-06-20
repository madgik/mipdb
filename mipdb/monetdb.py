import ipaddress
from abc import abstractmethod, ABC
from contextlib import contextmanager
from typing import Union

import sqlalchemy as sql
import toml

from mipdb.exceptions import DataBaseError

PRIMARYDATA_TABLE = "primary_data"

CONFIG = "/home/config.toml"


class Connection(ABC):
    """Abstract class representing a database connection interface."""

    @abstractmethod
    def create_schema(self, schema_name):
        pass

    @abstractmethod
    def drop_schema(self, schema_name):
        pass

    @abstractmethod
    def get_row_count(self, table):
        pass

    @abstractmethod
    def drop_table(self, table):
        pass

    @abstractmethod
    def delete_table_values(self, table):
        pass

    @abstractmethod
    def get_schemas(self):
        pass

    @abstractmethod
    def create_table(self, table):
        pass

    @abstractmethod
    def grant_select_access_rights(self, table, user):
        pass

    @abstractmethod
    def table_exists(self, table):
        pass

    @abstractmethod
    def insert_values_to_table(self, values, table):
        pass

    @abstractmethod
    def execute(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_current_user(self):
        pass

    @abstractmethod
    def copy_csv_in_table(self, offset, table_name):
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
    def get_row_count(self, table):
        pass

    @abstractmethod
    def drop_table(self, table):
        pass

    @abstractmethod
    def delete_table_values(self, table):
        pass

    @abstractmethod
    def create_table(self, table):
        pass

    @abstractmethod
    def grant_select_access_rights(self, table, user):
        pass

    @abstractmethod
    def get_schemas(self):
        pass

    @abstractmethod
    def table_exists(self, table):
        pass

    @abstractmethod
    def insert_values_to_table(self, values, table):
        pass

    @abstractmethod
    def execute(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_current_user(self):
        pass

    @abstractmethod
    def copy_csv_in_table(self, offset, table_name):
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
    they aren't. Hence, a small hack is needed to implement create_table."""

    _executor: Union[sql.engine.Engine, sql.engine.Connection]

    @abstractmethod
    def execute(self, *args, **kwargs) -> list:
        pass

    def create_schema(self, schema_name):
        self.execute(sql.schema.CreateSchema(schema_name))

    def drop_schema(self, schema_name):
        self.execute(f'DROP SCHEMA "{schema_name}" CASCADE')

    def get_schemas(self):
        res = self.execute("SELECT name FROM sys.schemas WHERE system=FALSE")
        return [schema for schema, *_ in res]

    def get_data_count_by_dataset(self, schema_fullname):
        res = self.execute(
            f"""
            SELECT dataset, COUNT(dataset) as count
            FROM "{schema_fullname}"."primary_data"
            GROUP BY dataset
            """
        )
        return list(res)

    def get_row_count(self, table):
        res = self.execute(f"select COUNT(*) from {table}").fetchone()
        return res[0]

    def get_column_distinct(self, column, table):
        datasets = list(self.execute(f"SELECT DISTINCT({column}) FROM {table};"))
        datasets = [dataset[0] for dataset in datasets]
        return datasets

    def table_exists(self, table):
        return table.exists(bind=self._executor)

    @handle_errors
    def create_table(self, table):
        table.create(bind=self._executor)

    @handle_errors
    def grant_select_access_rights(self, table, user):
        fullname = (
            f'"{table.schema}"."{table.name}"' if table.schema else f'"{table.name}"'
        )
        query = f"GRANT SELECT ON TABLE {fullname} TO {user} WITH GRANT OPTION;"
        self.execute(query)

    def copy_csv_in_table(self, file_location, records, offset, table_name):
        records_query = ""
        if records:
            records_query = f"{records} RECORDS"

        copy_into_query = f"""
            COPY {records_query} OFFSET {offset} INTO {table_name}
            FROM '{file_location}'
            USING DELIMITERS ',', E'\n', '\"'
            NULL AS '';
            """
        self.execute(copy_into_query)

    def copy_data_table_to_another_table(self, copy_into_table, copy_from_table):
        # row_id is autoincrement, so we do not need to provide values.
        table_column_names_without_row_id = [
            column.name
            for column in list(copy_into_table.table.columns)
            if column.name != "row_id"
        ]
        csv_column_names = [
            column.name for column in list(copy_from_table.table.columns)
        ]
        select_query_columns = [
            f'"{column}"' if column in csv_column_names else "NULL"
            for column in table_column_names_without_row_id
        ]
        self.execute(
            f"""
            INSERT INTO "{copy_into_table.table.schema}".{copy_into_table.table.name} ({','.join([f'"{column}"' for column in table_column_names_without_row_id])})
            SELECT {', '.join(select_query_columns)}
            FROM {copy_from_table.table.name};
        """
        )

    @handle_errors
    def drop_table(self, table):
        table.drop(bind=self._executor)

    def delete_table_values(self, table):
        self.execute(table.delete())

    def insert_values_to_table(self, table, values):
        self.execute(table.insert(), values)

    def get_executor(self):
        return self._executor

    def get_current_user(self):
        (user, *_), *_ = self.execute("SELECT CURRENT_USER")
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


def credentials_from_config(conf_file_path=CONFIG):
    try:
        return toml.load(conf_file_path)
    except FileNotFoundError:
        return {
            "DB_IP": "",
            "DB_PORT": "",
            "MONETDB_ADMIN_USERNAME": "",
            "MONETDB_LOCAL_USERNAME": "",
            "MONETDB_LOCAL_PASSWORD": "",
            "MONETDB_PUBLIC_USERNAME": "",
            "MONETDB_PUBLIC_PASSWORD": "",
            "DB_NAME": "",
            "SQLITE_DB_PATH": "",
        }


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

        url = f"monetdb://{username}:{password}@{ip}:{port}/{dbfarm}"
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
