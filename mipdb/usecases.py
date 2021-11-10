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
    update_actions(record, "ADD SCHEMA", conn)


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
            dataset_ids = [
                datasets_table.get_dataset_id(dataset, schema_id, conn)
                for dataset in datasets
            ]
            record = dict(
                code=code, version=version, schema_id=schema_id, dataset_ids=dataset_ids
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
            raise ForeignKeyError(
                f"The Schema:{schema_name} cannot be deleted because it contains Datasets: {datasets}"
                f"\nIf you want to force delete everything, please use the  '-- force' flag"
            )


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
    update_actions(record, "DELETE SCHEMA", conn)

    if len(record["dataset_ids"]) > 0:
        update_actions(record, "DELETE DATASETS", conn)


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

    def _verify_dataset_does_not_exist(self, dataset, conn):
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
    update_actions(record, "ADD DATASET", conn)


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
    update_actions(record, "DELETE DATASET", conn)


class EnableSchema(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, name, version) -> None:
        metadata = Schema(METADATA_SCHEMA)
        schemas_table = SchemasTable(schema=metadata)

        with self.db.begin() as conn:
            schema_id = self._get_schema_id(name, version, conn)
            current_status = schemas_table.get_schema_status(schema_id, conn)
            if current_status != "ENABLED":
                schemas_table.set_schema_status("ENABLED", schema_id, conn)
                record = dict(
                    code=name,
                    version=version,
                    schema_id=schema_id,
                )
                emitter.emit("enable_schema", record, conn)
            else:
                raise UserInputError("The schema was already enabled")

    def _get_schema_id(self, code, version, conn):
        metadata = Schema(METADATA_SCHEMA)
        schemas_table = SchemasTable(schema=metadata)
        schema_id = schemas_table.get_schema_id(code, version, conn)
        return schema_id


@emitter.handle("enable_schema")
def update_actions_on_schema_enablement(record, conn):
    update_actions(record, "ENABLE SCHEMA", conn)


class DisableSchema(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, name, version) -> None:
        metadata = Schema(METADATA_SCHEMA)
        schemas_table = SchemasTable(schema=metadata)

        with self.db.begin() as conn:
            schema_id = self._get_schema_id(name, version, conn)
            current_status = schemas_table.get_schema_status(schema_id, conn)

            if current_status != "DISABLED":
                schemas_table.set_schema_status("DISABLED", schema_id, conn)
                record = dict(
                    code=name,
                    version=version,
                    schema_id=schema_id,
                )
                emitter.emit("disable_schema", record, conn)
            else:
                raise UserInputError("The schema was already disabled")

    def _get_schema_id(self, code, version, conn):
        metadata = Schema(METADATA_SCHEMA)
        schemas_table = SchemasTable(schema=metadata)
        schema_id = schemas_table.get_schema_id(code, version, conn)
        return schema_id


@emitter.handle("disable_schema")
def update_actions_on_schema_disablement(record, conn):
    update_actions(record, "DISABLE SCHEMA", conn)


class EnableDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, dataset, schema_code, version) -> None:
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)

        with self.db.begin() as conn:
            schema_id = self._get_schema_id(schema_code, version, conn)
            dataset_id = self._get_dataset_id(dataset, schema_id, conn)
            current_status = datasets_table.get_dataset_status(dataset_id, conn)
            if current_status != "ENABLED":
                datasets_table.set_dataset_status("ENABLED", dataset_id, conn)
                record = dict(
                    dataset_id=dataset_id,
                    code=dataset,
                    version=version,
                )

                emitter.emit("enable_dataset", record, conn)
            else:
                raise UserInputError("The dataset was already enabled")

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


@emitter.handle("enable_dataset")
def update_actions_on_dataset_enablement(record, conn):
    update_actions(record, "ENABLE DATASET", conn)


class DisableDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, dataset, schema_code, version) -> None:
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)

        with self.db.begin() as conn:
            schema_id = self._get_schema_id(schema_code, version, conn)
            dataset_id = self._get_dataset_id(dataset, schema_id, conn)
            current_status = datasets_table.get_dataset_status(dataset_id, conn)
            if current_status != "DISABLED":
                datasets_table.set_dataset_status("DISABLED", dataset_id, conn)
                record = dict(
                    dataset_id=dataset_id,
                    code=dataset,
                    version=version,
                )

                emitter.emit("disable_dataset", record, conn)
            else:
                raise UserInputError("The dataset was already disabled")

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


@emitter.handle("disable_dataset")
def update_actions_on_dataset_disablement(record, conn):
    update_actions(record, "DISABLE DATASET", conn)


class TagSchema(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, code, version, tag, key_value, remove_flag) -> None:
        metadata = Schema(METADATA_SCHEMA)
        schemas_table = SchemasTable(schema=metadata)

        with self.db.begin() as conn:
            schema_id = self._get_schema_id(code, version, conn)
            properties = Properties(schemas_table.get_schema_properties(schema_id, conn))
            properties.get_filtered_properties(tag, key_value, remove_flag)
            schemas_table.set_schema_properties(properties.properties, schema_id, conn)
            action = "REMOVE SCHEMA TAG" if remove_flag else "ADD SCHEMA TAG"

            record = dict(
                code=code,
                version=version,
                schema_id=schema_id,
                action=action,
            )
            emitter.emit("tag_schema", record, conn)

    def _get_schema_id(self, code, version, conn):
        metadata = Schema(METADATA_SCHEMA)
        schemas_table = SchemasTable(schema=metadata)
        schema_id = schemas_table.get_schema_id(code, version, conn)
        return schema_id


@emitter.handle("tag_schema")
def update_actions_on_schema_tagging(record, conn):
    update_actions(record, record["action"], conn)


class TagDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(
        self, dataset, schema_code, version, tag, key_value, remove_flag
    ) -> None:
        metadata = Schema(METADATA_SCHEMA)
        dataset_table = DatasetsTable(schema=metadata)
        with self.db.begin() as conn:
            schema_id = self._get_schema_id(schema_code, version, conn)
            dataset_id = self._get_dataset_id(dataset, schema_id, conn)
            properties = Properties(dataset_table.get_dataset_properties(dataset_id, conn))
            properties.get_filtered_properties(tag, key_value, remove_flag)
            dataset_table.set_dataset_properties(properties.properties, dataset_id, conn)
            action = "REMOVE DATASET TAG" if remove_flag else "ADD DATASET TAG"

            record = dict(
                dataset_id=dataset_id,
                schema_id=schema_id,
                version=version,
                action=action,
            )

            emitter.emit("tag_dataset", record, conn)

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


@emitter.handle("tag_dataset")
def update_actions_on_dataset_tagging(record, conn):
    update_actions(record, record["action"], conn)


def update_actions(record, action, conn):
    metadata = Schema(METADATA_SCHEMA)
    actions_table = ActionsTable(schema=metadata)

    record = record.copy()
    record["action"] = action
    record["user"] = conn.get_current_user()
    record["date"] = datetime.datetime.now().isoformat()

    action_record = dict()
    action_record["action_id"] = actions_table.get_next_id(conn)
    action_record["action"] = json.dumps(record)
    actions_table.insert_values(action_record, conn)


class Properties:
    def __init__(self, properties) -> None:
        self.properties = properties

    def get_filtered_properties(self, tag, key_value, remove_flag):
        if remove_flag:
            if tag:
                self.properties = self._get_properties_after_tag_deletition(
                    tag
                )
            if key_value:
                self.properties = self._get_properties_after_key_value_deletition(
                    key_value
                )
        else:
            if tag:
                self.properties = self._get_properties_after_tag_addition(
                    tag
                )
            if key_value:
                self.properties = self._get_properties_after_key_value_addition(
                    key_value
                )

    def _get_properties_after_tag_deletition(self, tag):
        if self.properties is not None:
            properties_dict = json.loads(self.properties)
            if tag in properties_dict["tags"]:
                properties_dict["tags"].remove(tag)
                return json.dumps(properties_dict)
        raise UserInputError("Tag does not exist")

    def _get_properties_after_key_value_deletition(self, key_value):
        if self.properties is not None:
            properties_dict = json.loads(self.properties)
            if key_value in properties_dict.items():
                key, _ = key_value
                properties_dict.pop(key)
                return json.dumps(properties_dict)
        raise UserInputError("Key value does not exist")

    def _get_properties_after_tag_addition(self, tag):
        if self.properties is None:
            return json.dumps({"tags": [tag]})
        else:
            properties_dict = json.loads(self.properties)
            if tag not in properties_dict["tags"]:
                properties_dict["tags"].append(tag)
                return json.dumps(properties_dict)
        raise UserInputError("Tag already exists")

    def _get_properties_after_key_value_addition(self, key_value):
        if self.properties is None:
            key, value = key_value
            return json.dumps({key: value})
        else:
            properties_dict = json.loads(self.properties)
            if key_value not in properties_dict.items():
                key, value = key_value
                properties_dict[key] = value
                return json.dumps(properties_dict)
        raise UserInputError("Key value already exists")
