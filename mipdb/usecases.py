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
    DataModelTable,
    DatasetsTable,
    ActionsTable,
    MetadataTable,
    PrimaryDataTable,
)
from mipdb.dataset import Dataset
from mipdb.event import EventEmitter


class Action:
    ADD = "ADD"
    REMOVE = "REMOVE"


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
            DataModelTable(schema=metadata).create(conn)
            DatasetsTable(schema=metadata).create(conn)
            ActionsTable(schema=metadata).create(conn)


class AddDataModel(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, data_model_data) -> None:
        data_model_id = self._get_next_data_model_id()
        code = data_model_data["code"]
        version = data_model_data["version"]
        name = get_data_model_fullname(code, version)
        cdes = make_cdes(data_model_data)

        with self.db.begin() as conn:
            schema = self._create_schema(name, conn)
            self._create_primary_data_table(schema, cdes, conn)
            self._create_metadata_table(schema, conn, cdes)

            record = dict(
                code=code,
                version=version,
                label=data_model_data["label"],
                data_model_id=data_model_id,
            )
            emitter.emit("add_data_model", record, conn)

    def _get_next_data_model_id(self):
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)
        data_model_id = data_model_table.get_next_data_model_id(self.db)
        return data_model_id

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


@emitter.handle("add_data_model")
def update_data_models_on_data_model_addition(record: dict, conn: Connection):
    metadata = Schema(METADATA_SCHEMA)
    data_model_table = DataModelTable(schema=metadata)
    record = record.copy()
    record["status"] = Status.DISABLED
    data_model_table.insert_values(record, conn)


@emitter.handle("add_data_model")
def update_actions_on_data_model_addition(record: dict, conn: Connection):
    update_actions(record, "ADD DATA MODEL", conn)


def get_data_model_fullname(code, version):
    return f"{code}:{version}"


class DeleteDataModel(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, code, version, force) -> None:
        name = get_data_model_fullname(code, version)
        schema = Schema(name)
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)

        with self.db.begin() as conn:
            schema.drop(conn)
            data_model_id = self._get_data_model_id(code, version, conn)
            if not force:
                self._validate_data_model_deletion(name, data_model_id, conn)
            datasets = datasets_table.get_datasets(conn)
            dataset_ids = [
                datasets_table.get_dataset_id(dataset, data_model_id, conn)
                for dataset in datasets
            ]
            record = dict(
                code=code, version=version, data_model_id=data_model_id, dataset_ids=dataset_ids
            )
            emitter.emit("delete_data_model", record, conn)

    def _get_data_model_id(self, code, version, conn):
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)
        data_model_id = data_model_table.get_data_model_id(code, version, conn)
        return data_model_id

    def _validate_data_model_deletion(self, schema_name, data_model_id, conn):
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)
        datasets = datasets_table.get_datasets(conn, data_model_id)
        if not len(datasets) == 0:
            raise ForeignKeyError(
                f"The Schema:{schema_name} cannot be deleted because it contains Datasets: {datasets}"
                f"\nIf you want to force delete everything, please use the  '-- force' flag"
            )


@emitter.handle("delete_data_model")
def update_datasets_on_data_model_deletion(record, conn):
    data_model_id = record["data_model_id"]
    dataset_ids = record["dataset_ids"]
    metadata = Schema(METADATA_SCHEMA)
    datasets_table = DatasetsTable(schema=metadata)

    for dataset_id in dataset_ids:
        datasets_table.delete_dataset(dataset_id, data_model_id, conn)


@emitter.handle("delete_data_model")
def update_data_models_on_data_model_deletion(record, conn):
    code = record["code"]
    version = record["version"]
    metadata = Schema(METADATA_SCHEMA)
    data_model_table = DataModelTable(schema=metadata)
    data_model_table.delete_data_model(code, version, conn)


@emitter.handle("delete_data_model")
def update_actions_on_data_model_deletion(record, conn):
    update_actions(record, "DELETE DATA MODEL", conn)

    if len(record["dataset_ids"]) > 0:
        update_actions(record, "DELETE DATASETS", conn)


class AddDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, dataset_data, code, version) -> None:
        dataset_id = self._get_next_dataset_id()
        dataset = Dataset(dataset_data)

        schema_name = get_data_model_fullname(code=code, version=version)
        schema = Schema(schema_name)
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)
        schemas_id = data_model_table.get_data_model_id(code, version, self.db)

        with self.db.begin() as conn:
            primary_data_table = PrimaryDataTable.from_db(schema, conn)
            self._verify_dataset_does_not_exist(dataset, conn)
            primary_data_table.insert_dataset(dataset, conn)
            record = dict(
                data_model_id=schemas_id,
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
        schema_name = get_data_model_fullname(code=schema_code, version=version)
        schema = Schema(schema_name)
        with self.db.begin() as conn:
            primary_data_table = PrimaryDataTable.from_db(schema, conn)
            primary_data_table.remove_dataset(dataset, schema_name, conn)
            data_model_id = self._get_data_model_id(schema_code, version, conn)
            dataset_id = self._get_dataset_id(dataset, data_model_id, conn)

            record = dict(
                dataset_id=dataset_id,
                data_model_id=data_model_id,
                version=version,
            )

            emitter.emit("delete_dataset", record, conn)

    def _get_data_model_id(self, code, version, conn):
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)
        data_model_id = data_model_table.get_data_model_id(code, version, conn)
        return data_model_id

    def _get_dataset_id(self, code, data_model_id, conn):
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)
        dataset_id = datasets_table.get_dataset_id(code, data_model_id, conn)
        return dataset_id


@emitter.handle("delete_dataset")
def update_datasets_on_dataset_deletion(record, conn):
    dataset_id = record["dataset_id"]
    data_model_id = record["data_model_id"]
    metadata = Schema(METADATA_SCHEMA)
    dataset_table = DatasetsTable(schema=metadata)
    dataset_table.delete_dataset(dataset_id, data_model_id, conn)


@emitter.handle("delete_dataset")
def update_actions_on_dataset_deletion(record, conn):
    update_actions(record, "DELETE DATASET", conn)


class EnableDataModel(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, name, version) -> None:
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_id = self._get_data_model_id(name, version, conn)
            current_status = data_model_table.get_data_model_status(data_model_id, conn)
            if current_status != "ENABLED":
                data_model_table.set_data_model_status("ENABLED", data_model_id, conn)
                record = dict(
                    code=name,
                    version=version,
                    data_model_id=data_model_id,
                )
                emitter.emit("enable_data_model", record, conn)
            else:
                raise UserInputError("The schema was already enabled")

    def _get_data_model_id(self, code, version, conn):
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)
        data_model_id = data_model_table.get_data_model_id(code, version, conn)
        return data_model_id


@emitter.handle("enable_data_model")
def update_actions_on_data_model_enablement(record, conn):
    update_actions(record, "ENABLE DATA MODEL", conn)


class DisableDataModel(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, name, version) -> None:
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_id = self._get_data_model_id(name, version, conn)
            current_status = data_model_table.get_data_model_status(data_model_id, conn)

            if current_status != "DISABLED":
                data_model_table.set_data_model_status("DISABLED", data_model_id, conn)
                record = dict(
                    code=name,
                    version=version,
                    data_model_id=data_model_id,
                )
                emitter.emit("disable_data_model", record, conn)
            else:
                raise UserInputError("The schema was already disabled")

    def _get_data_model_id(self, code, version, conn):
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)
        data_model_id = data_model_table.get_data_model_id(code, version, conn)
        return data_model_id


@emitter.handle("disable_data_model")
def update_actions_on_data_model_disablement(record, conn):
    update_actions(record, "DISABLE DATA MODEL", conn)


class EnableDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, dataset, schema_code, version) -> None:
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_id = self._get_data_model_id(schema_code, version, conn)
            dataset_id = self._get_dataset_id(dataset, data_model_id, conn)
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

    def _get_data_model_id(self, code, version, conn):
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)
        data_model_id = data_model_table.get_data_model_id(code, version, conn)
        return data_model_id

    def _get_dataset_id(self, code, data_model_id, conn):
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)
        dataset_id = datasets_table.get_dataset_id(code, data_model_id, conn)
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
            data_model_id = self._get_data_model_id(schema_code, version, conn)
            dataset_id = self._get_dataset_id(dataset, data_model_id, conn)
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

    def _get_data_model_id(self, code, version, conn):
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)
        data_model_id = data_model_table.get_data_model_id(code, version, conn)
        return data_model_id

    def _get_dataset_id(self, code, data_model_id, conn):
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)
        dataset_id = datasets_table.get_dataset_id(code, data_model_id, conn)
        return dataset_id


@emitter.handle("disable_dataset")
def update_actions_on_dataset_disablement(record, conn):
    update_actions(record, "DISABLE DATASET", conn)


class TagDataModel(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, code, version, tag, key_value, add, remove) -> None:
        if not tag and not key_value:
            raise UserInputError("You need to provide a tag or/and a key value pair")
        if add == remove:
            raise UserInputError("You need to add or remove")

        action = Action.ADD if add else Action.REMOVE
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_id = self._get_data_model_id(code, version, conn)
            properties = Properties(
                data_model_table.get_data_model_properties(data_model_id, conn)
            )
            properties.update_properties(tag, key_value, action)
            data_model_table.set_data_model_properties(properties.properties, data_model_id, conn)
            action = "REMOVE DATA MODEL TAG" if action == Action.REMOVE else "ADD DATA MODEL TAG"

            record = dict(
                code=code,
                version=version,
                data_model_id=data_model_id,
                action=action,
            )
            emitter.emit("tag_data_model", record, conn)

    def _get_data_model_id(self, code, version, conn):
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)
        data_model_id = data_model_table.get_data_model_id(code, version, conn)
        return data_model_id


@emitter.handle("tag_data_model")
def update_actions_on_data_model_tagging(record, conn):
    update_actions(record, record["action"], conn)


class TagDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(
        self, dataset, schema_code, version, tag, key_value, add, remove
    ) -> None:
        if not tag and not key_value:
            raise UserInputError("You need to provide a tag or/and a key value pair")
        if add == remove:
            raise UserInputError("You need to add or remove")

        action = Action.ADD if add else Action.REMOVE
        metadata = Schema(METADATA_SCHEMA)
        dataset_table = DatasetsTable(schema=metadata)
        with self.db.begin() as conn:
            data_model_id = self._get_data_model_id(schema_code, version, conn)
            dataset_id = self._get_dataset_id(dataset, data_model_id, conn)
            properties = Properties(
                dataset_table.get_dataset_properties(dataset_id, conn)
            )
            properties.update_properties(tag, key_value, action)
            dataset_table.set_dataset_properties(
                properties.properties, dataset_id, conn
            )
            action = "REMOVE DATASET TAG" if action == Action.REMOVE else "ADD DATASET TAG"

            record = dict(
                dataset_id=dataset_id,
                data_model_id=data_model_id,
                version=version,
                action=action,
            )

            emitter.emit("tag_dataset", record, conn)

    def _get_data_model_id(self, code, version, conn):
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)
        data_model_id = data_model_table.get_data_model_id(code, version, conn)
        return data_model_id

    def _get_dataset_id(self, code, data_model_id, conn):
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)
        dataset_id = datasets_table.get_dataset_id(code, data_model_id, conn)
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

    def update_properties(self, tag, key_value, action):
        if not self.properties:
            self.properties = json.dumps({"tags": []})

        if tag:
            self.update_properties_tag(tag, action)

        if key_value:
            self.update_properties_key_value(key_value, action)

    def update_properties_tag(self, tag, action):
        if action == Action.REMOVE:
            properties_dict = json.loads(self.properties)
            if tag in properties_dict["tags"]:
                properties_dict["tags"].remove(tag)
                self.properties = json.dumps(properties_dict)
            else:
                raise UserInputError("Tag does not exist")
        else:
            properties_dict = json.loads(self.properties)
            if tag not in properties_dict["tags"]:
                properties_dict["tags"].append(tag)
                self.properties = json.dumps(properties_dict)
            else:
                raise UserInputError("Tag already exists")

    def update_properties_key_value(self, key_value, action):
        if action == Action.REMOVE:
            properties_dict = json.loads(self.properties)
            if key_value in properties_dict.items():
                key, _ = key_value
                properties_dict.pop(key)
                self.properties = json.dumps(properties_dict)
            else:
                raise UserInputError("Key value does not exist")
        else:
            properties_dict = json.loads(self.properties)
            if key_value not in properties_dict.items():
                key, value = key_value
                properties_dict[key] = value
                self.properties = json.dumps(properties_dict)
            else:
                raise UserInputError("Key value already exists")
