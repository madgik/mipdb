from abc import ABC, abstractmethod
from mipdb.exceptions import DataBaseError

from mipdb.database import DataBase, MonetDB, Connection
from mipdb.schema import (
    Schema,
)
from mipdb.dataelements import (
    CommonDataElement,
    CategoricalCDE,
    NumericalCDE,
    make_cdes,
)
from mipdb.tables import (
    SchemasTable,
    DatasetsTable,
    PropertiesTable,
    ActionsTable,
    VariablesTable,
    EnumerationsTable,
    DomainsTable,
    UnitsTable,
    PrimaryDataTable,
)
from mipdb.event import EventEmitter
from mipdb.constants import Status


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
        metadata = Schema("mipdb_metadata")
        with self.db.begin() as conn:
            metadata.create(conn)
            SchemasTable(schema=metadata).create(conn)
            DatasetsTable(schema=metadata).create(conn)
            PropertiesTable(schema=metadata).create(conn)
            ActionsTable(schema=metadata).create(conn)


class AddSchema(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, schema_data) -> None:
        metadata = Schema("mipdb_metadata")
        schemas_table = SchemasTable(schema=metadata)
        schema_id = schemas_table.get_next_schema_id(self.db)

        code = schema_data["code"]
        version = schema_data["version"]
        name = get_schema_fullname(code, version)

        schema = Schema(name)
        vars_table = VariablesTable(schema)
        enums_table = EnumerationsTable(schema)
        domains_table = DomainsTable(schema)
        units_table = UnitsTable(schema)
        cdes = make_cdes(schema_data)
        primary_data_table = PrimaryDataTable(schema, cdes)
        with self.db.begin() as conn:
            schema.create(conn)
            vars_table.create(conn)
            vars_table.insert_values(vars_table.get_values_from_cdes(cdes), conn)
            enums_table.create(conn)
            enums_table.insert_values(enums_table.get_values_from_cdes(cdes), conn)
            domains_table.create(conn)
            domains_table.insert_values(domains_table.get_values_from_cdes(cdes), conn)
            units_table.create(conn)
            units_table.insert_values(units_table.get_values_from_cdes(cdes), conn)
            primary_data_table.create(conn)

            record = dict(
                code=code,
                version=version,
                label=schema_data["label"],
                schema_id=schema_id,
            )
            emitter.emit("add_schema", record, conn)


@emitter.handle("add_schema")
def update_schemas_on_schema_addition(record: dict, conn: Connection):
    metadata = Schema("mipdb_metadata")
    schemas_table = SchemasTable(schema=metadata)
    record = record.copy()
    record["type"] = "SCHEMA"
    record["status"] = Status.DISABLED
    schemas_table.insert_values(record, conn)


# TODO this is incomplete
@emitter.handle("add_schema")
def update_actions_on_schema_addition(record: dict, conn: Connection):
    metadata = Schema("mipdb_metadata")
    actions_table = ActionsTable(schema=metadata)
    record = record.copy()
    record["type"] = "ADD SCHEMA"
    record["user"] = "TO BE DETERMINED"
    record["date"] = "TO BE DETERMINED"
    actions_table.insert_values(record, conn)


def get_schema_fullname(code, version):
    return f"{code}:{version}"


class DeleteSchema(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, code, version) -> None:
        metadata = Schema("mipdb_metadata")
        schemas_table = SchemasTable(schema=metadata)

        name = get_schema_fullname(code, version)
        schema = Schema(name)
        with self.db.begin() as conn:
            schema.drop(conn)
            schema_id = schemas_table.get_schema_id(code, version, conn)

            record = dict(
                code=code,
                version=version,
                schema_id=schema_id,
            )
            emitter.emit("delete_schema", record, conn)


@emitter.handle("delete_schema")
def update_schemas_on_schema_deletion(record, conn):
    code = record["code"]
    version = record["version"]
    metadata = Schema("mipdb_metadata")
    schemas_table = SchemasTable(schema=metadata)
    schema_id = schemas_table.mark_schema_as_deleted(code, version, conn)


@emitter.handle("delete_schema")
def update_actions_on_schema_deletion(record, conn):
    metadata = Schema("mipdb_metadata")
    actions_table = ActionsTable(schema=metadata)
    record = record.copy()
    record["type"] = "DELETE SCHEMA"
    record["user"] = "TO BE DETERMINED"
    record["date"] = "TO BE DETERMINED"
    actions_table.insert_values(record, conn)
