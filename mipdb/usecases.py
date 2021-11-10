import datetime
import json
from abc import ABC, abstractmethod

from mipdb.database import DataBase, Connection
from mipdb.database import METADATA_SCHEMA
from mipdb.database import Status
from mipdb.exceptions import ForeignKeyError
from mipdb.exceptions import UserInputError
from mipdb.schema import Schema
from mipdb.dataelements import CommonDataElement, make_cdes
from mipdb.tables import (
    SchemasTable,
    DatasetsTable,
    ActionsTable,
    MetadataTable,
    PrimaryDataTable,
)
from mipdb.dataset import Dataset
from mipdb.event import EventEmitter


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
            ActionsTable(schema=metadata).create(conn)


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
    record["status"] = Status.DISABLED
    schemas_table.insert_values(record, conn)


@emitter.handle("add_schema")
def update_actions_on_schema_addition(record: dict, conn: Connection):
    metadata = Schema(METADATA_SCHEMA)
    actions_table = ActionsTable(schema=metadata)

    record = record.copy()
    action = f"ADD SCHEMA"
    record["action"] = action
    record["user"] = conn.get_current_user()
    record["date"] = datetime.datetime.now().isoformat()

    action_record = dict()
    action_record["action_id"] = actions_table.get_next_id(conn)
    action_record["action"] = json.dumps(record)
    actions_table.insert_values(action_record, conn)


def get_schema_fullname(code, version):
    return f"{code}:{version}"


class DeleteSchema(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, code, version, force) -> None:
        name = get_schema_fullname(code, version)
        schema = Schema(name)
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)

        with self.db.begin() as conn:
            schema.drop(conn)
            schema_id = self._get_schema_id(code, version, conn)
            if not force:
                self._validate_schema_deletion(name, schema_id, conn)
            datasets = datasets_table.get_datasets(conn)
            dataset_ids = [datasets_table.get_dataset_id(dataset, schema_id, conn) for dataset in datasets ]
            record = dict(
                code=code,
                version=version,
                schema_id=schema_id,
                dataset_ids=dataset_ids
            )
            emitter.emit("delete_schema", record, conn)

    def _get_schema_id(self, code, version, conn):
        metadata = Schema(METADATA_SCHEMA)
        schemas_table = SchemasTable(schema=metadata)
        schema_id = schemas_table.get_schema_id(code, version, conn)
        return schema_id

    def _validate_schema_deletion(self, schema_name, schema_id, conn):
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)
        datasets = datasets_table.get_datasets(conn, schema_id)
        if not len(datasets) == 0:
            raise ForeignKeyError(f"The Schema:{schema_name} cannot be deleted because it contains Datasets: {datasets}"
                              f"\nIf you want to force delete everything, please use the  '--force' flag")


@emitter.handle("delete_schema")
def update_datasets_on_schema_deletion(record, conn):
    schema_id = record["schema_id"]
    dataset_ids = record["dataset_ids"]
    metadata = Schema(METADATA_SCHEMA)
    datasets_table = DatasetsTable(schema=metadata)

    for dataset_id in dataset_ids:
        datasets_table.delete_dataset(dataset_id, schema_id, conn)


@emitter.handle("delete_schema")
def update_schemas_on_schema_deletion(record, conn):
    code = record["code"]
    version = record["version"]
    metadata = Schema(METADATA_SCHEMA)
    schemas_table = SchemasTable(schema=metadata)
    schemas_table.delete_schema(code, version, conn)


@emitter.handle("delete_schema")
def update_actions_on_schema_deletion(record, conn):
    metadata = Schema(METADATA_SCHEMA)
    actions_table = ActionsTable(schema=metadata)

    record = record.copy()
    action = f"DELETE SCHEMA"
    record["action"] = action
    record["user"] = conn.get_current_user()
    record["date"] = datetime.datetime.now().isoformat()

    action_record = dict()
    action_record["action_id"] = actions_table.get_next_id(conn)
    action_record["action"] = json.dumps(record)
    actions_table.insert_values(action_record, conn)

    if len(record["dataset_ids"]) > 0:
        action = f"DELETE DATASETS"
        record["action"] = action
        action_record = dict()
        action_record["action_id"] = actions_table.get_next_id(conn)
        action_record["action"] = json.dumps(record)
        actions_table.insert_values(action_record, conn)


class AddDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, dataset_data, code, version) -> None:
        dataset_id = self._get_next_dataset_id()
        dataset = Dataset(dataset_data)

        schema_name = get_schema_fullname(code=code, version=version)
        schema = Schema(schema_name)
        metadata = Schema(METADATA_SCHEMA)
        schemas_table = SchemasTable(schema=metadata)
        schemas_id = schemas_table.get_schema_id(code, version, self.db)

        with self.db.begin() as conn:
            primary_data_table = PrimaryDataTable.from_db(schema, conn)
            self._verify_dataset_does_not_exist(dataset, conn)
            primary_data_table.insert_dataset(dataset, conn)
            record = dict(
                schema_id=schemas_id,
                dataset_id=dataset_id,
                code=dataset.name,
            )
            emitter.emit("add_dataset", record, conn)

    def _get_next_dataset_id(self):
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)
        dataset_id = datasets_table.get_next_dataset_id(self.db)
        return dataset_id

    def _verify_dataset_does_not_exist(self,dataset, conn):
        metadata = Schema(METADATA_SCHEMA)
        dataset_table = DatasetsTable(schema=metadata)
        datasets = dataset_table.get_datasets(conn)
        if datasets is not None and dataset.name in datasets:
            raise UserInputError("Dataset already exists!")


@emitter.handle("add_dataset")
def update_datasets_on_dataset_addition(record: dict, conn: Connection):
    metadata = Schema(METADATA_SCHEMA)
    datasets_table = DatasetsTable(schema=metadata)
    record = record.copy()
    record["status"] = Status.DISABLED
    datasets_table.insert_values(record, conn)


@emitter.handle("add_dataset")
def update_actions_on_dataset_addition(record: dict, conn: Connection):
    metadata = Schema(METADATA_SCHEMA)
    actions_table = ActionsTable(schema=metadata)

    record = record.copy()
    action = f"ADD DATASET"
    record["action"] = action
    record["user"] = conn.get_current_user()
    record["date"] = datetime.datetime.now().isoformat()

    action_record = dict()
    action_record["action_id"] = actions_table.get_next_id(conn)
    action_record["action"] = json.dumps(record)
    actions_table.insert_values(action_record, conn)


class DeleteDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, dataset, schema_code, version) -> None:
        schema_name = get_schema_fullname(code=schema_code, version=version)
        schema = Schema(schema_name)
        with self.db.begin() as conn:
            primary_data_table = PrimaryDataTable.from_db(schema, conn)
            primary_data_table.remove_dataset(dataset, schema_name, conn)
            schema_id = self._get_schema_id(schema_code, version, conn)
            dataset_id = self._get_dataset_id(dataset, schema_id, conn)

            record = dict(
                dataset_id=dataset_id,
                schema_id=schema_id,
                version=version,
            )

            emitter.emit("delete_dataset", record, conn)

    def _get_schema_id(self, code, version, conn):
        metadata = Schema(METADATA_SCHEMA)
        schemas_table = SchemasTable(schema=metadata)
        schema_id = schemas_table.get_schema_id(code, version, conn)
        return schema_id

    def _get_dataset_id(self, code, schema_id, conn):
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)
        dataset_id = datasets_table.get_dataset_id(code, schema_id, conn)
        return dataset_id


@emitter.handle("delete_dataset")
def update_datasets_on_dataset_deletion(record, conn):
    dataset_id = record["dataset_id"]
    schema_id = record["schema_id"]
    metadata = Schema(METADATA_SCHEMA)
    dataset_table = DatasetsTable(schema=metadata)
    dataset_table.delete_dataset(dataset_id, schema_id, conn)


@emitter.handle("delete_dataset")
def update_actions_on_dataset_deletion(record, conn):
    metadata = Schema(METADATA_SCHEMA)
    actions_table = ActionsTable(schema=metadata)

    record = record.copy()
    action = f"DELETE DATASET"
    record["action"] = action
    record["user"] = conn.get_current_user()
    record["date"] = datetime.datetime.now().isoformat()

    action_record = dict()
    action_record["action_id"] = actions_table.get_next_id(conn)
    action_record["action"] = json.dumps(record)
    actions_table.insert_values(action_record, conn)