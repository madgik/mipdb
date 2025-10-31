from contextlib import contextmanager
from typing import Union

import sqlalchemy as sql
from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection as SAConnection

from mipdb.exceptions import DataBaseError

PRIMARYDATA_TABLE = "primary_data"


def handle_errors(func):
    """Decorator to handle DB exceptions and raise DataBaseError."""

    @contextmanager
    def _handle_errors():
        try:
            yield
        except (sql.exc.OperationalError, sql.exc.IntegrityError) as exc:
            _, msg = exc.orig.args[0].split("!")
            raise DataBaseError(msg)

    def wrapper(*args, **kwargs):
        with _handle_errors():
            return func(*args, **kwargs)

    return wrapper


class DBExecutor:
    """Class to handle SQL execution using SQLAlchemy's Engine and Connection."""

    def __init__(self, executor: Union[Engine, SAConnection]):
        self._executor = executor

    def _get_connection(self):
        # SQLAlchemy 2.x: if executor is Engine, open a new Connection; if already Connection, use it
        if isinstance(self._executor, Engine):
            return self._executor.connect()
        if isinstance(self._executor, SAConnection):
            return self._executor
        raise DataBaseError(f"Unsupported executor type: {type(self._executor)}")

    @handle_errors
    def execute(self, query, *args, **kwargs) -> sql.engine.CursorResult | None:
        conn = self._get_connection()
        own_conn = isinstance(self._executor, sql.engine.Engine)
        try:
            stmt = text(query) if isinstance(query, str) else query

            params = kwargs or None
            result = conn.execute(stmt, params) if params else conn.execute(stmt, *args)

            if own_conn:
                conn.commit()
            return result
        finally:
            if own_conn:
                conn.close()

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
            FROM "{schema_fullname}"."{PRIMARYDATA_TABLE}"
            GROUP BY dataset
        """
        )
        return list(res)

    def get_row_count(self, table):
        res = self.execute(f"SELECT COUNT(*) FROM {table}")
        return res.scalar_one() if res is not None else 0

    def get_column_distinct(self, column, table):
        datasets = self.execute(f"SELECT DISTINCT({column}) FROM {table};")
        return [dataset[0] for dataset in datasets]

    def table_exists(self, table) -> bool:
        table_name = table.name
        schema_name = str(table.schema) if table.schema else None

        if schema_name:
            result = self.execute(
                text(
                    """
                    SELECT 1
                    FROM sys.tables AS t
                    JOIN sys.schemas AS s ON s.id = t.schema_id
                    WHERE t.name = :table_name
                      AND s.name = :schema_name
                      AND t.type IN (0, 1, 30)
                    LIMIT 1
                    """
                ),
                table_name=table_name,
                schema_name=schema_name,
            )
        else:
            result = self.execute(
                text(
                    """
                    SELECT 1
                    FROM sys.tables AS t
                    WHERE t.name = :table_name
                      AND t.schema_id = (
                          SELECT id FROM sys.schemas WHERE name = CURRENT_SCHEMA
                      )
                      AND t.type IN (0, 1, 30)
                    LIMIT 1
                    """
                ),
                table_name=table_name,
            )

        return bool(result.scalar_one_or_none() if result is not None else None)

    def get_table_column_names(self, table_name: str, schema_name: str | None = None):
        if schema_name:
            result = self.execute(
                text(
                    """
                    SELECT c.name
                    FROM sys.columns AS c
                    JOIN sys.tables AS t ON c.table_id = t.id
                    JOIN sys.schemas AS s ON s.id = t.schema_id
                    WHERE t.name = :table_name
                      AND s.name = :schema_name
                    ORDER BY c.number
                    """
                ),
                table_name=table_name,
                schema_name=schema_name,
            )
        else:
            result = self.execute(
                text(
                    """
                    SELECT c.name
                    FROM sys.columns AS c
                    JOIN sys.tables AS t ON c.table_id = t.id
                    WHERE t.name = :table_name
                      AND t.schema_id = (
                          SELECT id FROM sys.schemas WHERE name = CURRENT_SCHEMA
                      )
                    ORDER BY c.number
                    """
                ),
                table_name=table_name,
            )

        return [row[0] for row in result] if result else []

    @handle_errors
    def create_table(self, table):
        table.create(bind=self._executor)

    @handle_errors
    def grant_select_access_rights(self, table, user):
        fullname = (
            f'"{table.schema}"."{table.name}"' if table.schema else f'"{table.name}"'
        )
        self.execute(f"GRANT SELECT ON TABLE {fullname} TO {user} WITH GRANT OPTION;")

    def copy_csv_in_table(self, file_location, records, offset, table_name):
        records_query = f"{records} RECORDS" if records else ""
        self.execute(
            f"""
            COPY {records_query} OFFSET {offset} INTO {table_name}
            FROM '{file_location}'
            USING DELIMITERS ',', E'\n', '"'
            NULL AS '';
        """
        )

    def copy_data_table_to_another_table(self, copy_into_table, copy_from_table):
        table_columns = [
            col.name for col in copy_into_table.table.columns if col.name != "row_id"
        ]
        csv_columns = [col.name for col in copy_from_table.table.columns]
        select_columns = [
            f'"{col}"' if col in csv_columns else "NULL" for col in table_columns
        ]
        self.execute(
            f"""
            INSERT INTO "{copy_into_table.table.schema}".{copy_into_table.table.name} ({','.join(f'"{col}"' for col in table_columns)})
            SELECT {', '.join(select_columns)}
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

    def get_current_user(self):
        res = self.execute("SELECT CURRENT_USER")
        return res[0][0] if res else None

    def get_executor(self):
        return self._executor


class MonetDBConnection(DBExecutor):
    """Concrete connection object for MonetDB within transaction boundaries."""

    def __init__(self, conn: SAConnection) -> None:
        super().__init__(conn)


class MonetDB(DBExecutor):
    """Concrete DataBase object for connecting to a MonetDB instance."""

    def __init__(self, url: str, echo=False) -> None:
        super().__init__(sql.create_engine(url, echo=echo))

    @classmethod
    def from_config(cls, dbconfig) -> "MonetDB":
        username, password, ip, port, dbfarm = (
            dbconfig["username"],
            dbconfig["password"],
            dbconfig["ip"],
            dbconfig["port"],
            dbconfig["dbfarm"],
        )
        url = f"monetdb://{username}:{password}@{ip}:{port}/{dbfarm}"
        return cls(url)

    @contextmanager
    def begin(self) -> MonetDBConnection:
        with self._executor.begin() as conn:
            yield MonetDBConnection(conn)
