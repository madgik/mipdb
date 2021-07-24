from abc import ABC, abstractmethod
import datetime

from mipdb.database import DataBase, Connection
from mipdb.schema import Schema
from mipdb.dataelements import CommonDataElement, make_cdes
from mipdb.tables import (
    SchemasTable,
    DatasetsTable,
    LogsTable,
    MetadataTable,
    PrimaryDataTable,
)
from mipdb.dataset import Dataset
from mipdb.event import EventEmitter
from mipdb.exceptions import DataBaseError
from mipdb.constants import Status, METADATA_SCHEMA


class UseCase(ABC):
    """Abstract use case class."""

    @abstractmethod
    def execute(self, *args, **kwargs) -> None:
        """Executes use case logic with arguments from cli command. Has side
        effects but no return values."""


emitter = EventEmitter()


class InitDB(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self) -> None:
        metadata = Schema(METADATA_SCHEMA)
        with self.db.begin() as conn:
            metadata.create(conn)
            SchemasTable(schema=metadata).create(conn)
            DatasetsTable(schema=metadata).create(conn)
            LogsTable(schema=metadata).create(conn)


class AddSchema(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, schema_data) -> None:
        schema_id = self._get_next_schema_id()
        code = schema_data["code"]
        version = schema_data["version"]
        name = get_schema_fullname(code, version)
        cdes = make_cdes(schema_data)

        with self.db.begin() as conn:
            schema = self._create_schema(name, conn)
            self._create_primary_data_table(schema, cdes, conn)
            self._create_metadata_table(schema, conn, cdes)

            record = dict(
                code=code,
                version=version,
                label=schema_data["label"],
                schema_id=schema_id,
            )
            emitter.emit("add_schema", record, conn)

    def _get_next_schema_id(self):
        metadata = Schema(METADATA_SCHEMA)
        schemas_table = SchemasTable(schema=metadata)
        schema_id = schemas_table.get_next_schema_id(self.db)
        return schema_id

    def _create_schema(self, name, conn):
        schema = Schema(name)
        schema.create(conn)
        return schema

    def _create_primary_data_table(self, schema, cdes, conn):
        primary_data_table = PrimaryDataTable.from_cdes(schema, cdes)
        primary_data_table.create(conn)

    def _create_metadata_table(self, schema, conn, cdes):
        metadata_table = MetadataTable(schema)
        metadata_table.create(conn)
        values = metadata_table.get_values_from_cdes(cdes)
        metadata_table.insert_values(values, conn)


@emitter.handle("add_schema")
def update_schemas_on_schema_addition(record: dict, conn: Connection):
    metadata = Schema(METADATA_SCHEMA)
    schemas_table = SchemasTable(schema=metadata)
    record = record.copy()
    record["type"] = "SCHEMA"
    record["status"] = Status.DISABLED
    schemas_table.insert_values(record, conn)


@emitter.handle("add_schema")
def update_logs_on_schema_addition(record: dict, conn: Connection):
    metadata = Schema(METADATA_SCHEMA)
    logs_table = LogsTable(schema=metadata)
    record = record.copy()
    schema_id = record["schema_id"]
    code = record["code"]
    version = record["version"]
    description = f"ADD SCHEMA WITH id={schema_id}, code={code}, version={version}"
    record["description"] = description
    record["user"] = conn.get_current_user()
    record["date"] = datetime.datetime.now().isoformat()
    logs_table.insert_values(record, conn)


def get_schema_fullname(code, version):
    return f"{code}:{version}"


class DeleteSchema(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, code, version) -> None:
        name = get_schema_fullname(code, version)

        with self.db.begin() as conn:
            schema = Schema(name)
            schema.drop(conn)
            schema_id = self._get_schema_id(code, version, conn)

            record = dict(
                code=code,
                version=version,
                schema_id=schema_id,
            )
            emitter.emit("delete_schema", record, conn)

    def _get_schema_id(self, code, version, conn):
        metadata = Schema(METADATA_SCHEMA)
        schemas_table = SchemasTable(schema=metadata)
        schema_id = schemas_table.get_schema_id(code, version, conn)
        return schema_id


@emitter.handle("delete_schema")
def update_schemas_on_schema_deletion(record, conn):
    code = record["code"]
    version = record["version"]
    metadata = Schema(METADATA_SCHEMA)
    schemas_table = SchemasTable(schema=metadata)
    schema_id = schemas_table.mark_schema_as_deleted(code, version, conn)


@emitter.handle("delete_schema")
def update_logs_on_schema_deletion(record, conn):
    metadata = Schema(METADATA_SCHEMA)
    logs_table = LogsTable(schema=metadata)
    record = record.copy()
    schema_id = record["schema_id"]
    code = record["code"]
    version = record["version"]
    description = f"DELETE SCHEMA WITH id={schema_id}, code={code}, version={version}"
    record["description"] = description
    record["user"] = conn.get_current_user()
    record["date"] = datetime.datetime.now().isoformat()
    logs_table.insert_values(record, conn)


# TODO update datasets
# TODO update logs
# TODO verify dataset not already in db
class AddDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, dataset_data, schema, version) -> None:
        dataset = Dataset(dataset_data)
        schema_name = get_schema_fullname(code=schema, version=version)
        schema = Schema(schema_name)
        primary_data_table = PrimaryDataTable.from_db(schema, self.db)
        primary_data_table.insert_dataset(dataset, self.db)
