import datetime
import json
from abc import ABC, abstractmethod

import pandas as pa

from mipdb.database import DataBase, Connection
from mipdb.database import METADATA_SCHEMA
from mipdb.database import Status
from mipdb.dataelements import get_system_columns_metadata
from mipdb.exceptions import ForeignKeyError
from mipdb.exceptions import UserInputError
from mipdb.properties import Properties
from mipdb.schema import Schema
from mipdb.dataelements import make_cdes
from mipdb.tables import (
    DataModelTable,
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
            DataModelTable(schema=metadata).create(conn)
            DatasetsTable(schema=metadata).create(conn)
            ActionsTable(schema=metadata).create(conn)


class AddDataModel(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, data_model_data) -> None:
        code = data_model_data["code"]
        version = data_model_data["version"]
        name = get_data_model_fullname(code, version)
        cdes = make_cdes(data_model_data)
        system_columns_metadata = get_system_columns_metadata()
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_id = data_model_table.get_next_data_model_id(conn)
            schema = self._create_schema(name, conn)
            self._create_primary_data_table(schema, cdes + system_columns_metadata, conn)
            self._create_metadata_table(schema, conn, cdes)
            record = dict(
                code=code,
                version=version,
                label=data_model_data["label"],
                data_model_id=data_model_id,
            )
            emitter.emit("add_data_model", record, conn)

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
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            schema.drop(conn)
            data_model_id = data_model_table.get_data_model_id(code, version, conn)
            if not force:
                self._validate_data_model_deletion(name, data_model_id, conn)
            datasets = datasets_table.get_datasets(
                data_model_id=data_model_id, columns=["dataset_id"], db=conn
            )
            dataset_ids = [dataset[0] for dataset in datasets]
            record = dict(
                code=code,
                version=version,
                data_model_id=data_model_id,
                dataset_ids=dataset_ids,
            )
            emitter.emit("delete_data_model", record, conn)

    def _validate_data_model_deletion(self, data_model_name, data_model_id, conn):
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)
        datasets = datasets_table.get_datasets(conn, data_model_id)
        if not len(datasets) == 0:
            raise ForeignKeyError(
                f"The Data Model:{data_model_name} cannot be deleted because it contains Datasets: {datasets}"
                f"\nIf you want to force delete everything, please use the  '--force' flag"
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

    if record["dataset_ids"]:
        update_actions(record, "DELETE DATASETS", conn)


class AddDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, dataset_data, code, version) -> None:
        dataset = Dataset(dataset_data)

        data_model_name = get_data_model_fullname(code=code, version=version)
        data_model = Schema(data_model_name)
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            dataset_id = self._get_next_dataset_id(conn)
            data_model_id = data_model_table.get_data_model_id(code, version, conn)

            primary_data_table = PrimaryDataTable.from_db(data_model, conn)
            self._verify_dataset_does_not_exist(data_model_id, dataset, conn)
            primary_data_table.insert_dataset(dataset, conn)
            record = dict(
                data_model_id=data_model_id,
                dataset_id=dataset_id,
                code=dataset.name,
            )
            emitter.emit("add_dataset", record, conn)

    def _get_next_dataset_id(self, conn):
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)
        dataset_id = datasets_table.get_next_dataset_id(conn)
        return dataset_id

    def _verify_dataset_does_not_exist(self, data_model_id, dataset, conn):
        metadata = Schema(METADATA_SCHEMA)
        dataset_table = DatasetsTable(schema=metadata)
        datasets = dataset_table.get_datasets(
            db=conn, data_model_id=data_model_id, columns=["code"]
        )
        if datasets is not None and (dataset.name,) in datasets:
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


class ValidateDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, dataset_data, code, version) -> None:
        dataset = Dataset(dataset_data)
        data_model_name = get_data_model_fullname(code=code, version=version)
        data_model = Schema(data_model_name)

        with self.db.begin() as conn:
            metadata_table = MetadataTable.from_db(data_model, conn)
            dataset.validate_dataset(metadata_table.table)


class DeleteDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, dataset, data_model_code, version) -> None:
        data_model_name = get_data_model_fullname(code=data_model_code, version=version)
        data_model = Schema(data_model_name)
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)
        datasets_table = DatasetsTable(schema=metadata)

        with self.db.begin() as conn:
            primary_data_table = PrimaryDataTable.from_db(data_model, conn)
            primary_data_table.remove_dataset(dataset, data_model_name, conn)
            data_model_id = data_model_table.get_data_model_id(
                data_model_code, version, conn
            )
            dataset_id = datasets_table.get_dataset_id(dataset, data_model_id, conn)

            record = dict(
                dataset_id=dataset_id,
                data_model_id=data_model_id,
                version=version,
            )

            emitter.emit("delete_dataset", record, conn)


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

    def execute(self, code, version) -> None:
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_id = data_model_table.get_data_model_id(code, version, conn)
            current_status = data_model_table.get_data_model_status(data_model_id, conn)
            if current_status != "ENABLED":
                data_model_table.set_data_model_status("ENABLED", data_model_id, conn)
                record = dict(
                    code=code,
                    version=version,
                    data_model_id=data_model_id,
                )
                emitter.emit("enable_data_model", record, conn)
            else:
                raise UserInputError("The data model was already enabled")


@emitter.handle("enable_data_model")
def update_actions_on_data_model_enablement(record, conn):
    update_actions(record, "ENABLE DATA MODEL", conn)


class DisableDataModel(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, code, version) -> None:
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_id = data_model_table.get_data_model_id(code, version, conn)
            current_status = data_model_table.get_data_model_status(data_model_id, conn)

            if current_status != "DISABLED":
                data_model_table.set_data_model_status("DISABLED", data_model_id, conn)
                record = dict(
                    code=code,
                    version=version,
                    data_model_id=data_model_id,
                )
                emitter.emit("disable_data_model", record, conn)
            else:
                raise UserInputError("The data model was already disabled")


@emitter.handle("disable_data_model")
def update_actions_on_data_model_disablement(record, conn):
    update_actions(record, "DISABLE DATA MODEL", conn)


class EnableDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, dataset, data_model_code, version) -> None:
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:

            data_model_id = data_model_table.get_data_model_id(
                data_model_code, version, conn
            )
            dataset_id = datasets_table.get_dataset_id(dataset, data_model_id, conn)
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


@emitter.handle("enable_dataset")
def update_actions_on_dataset_enablement(record, conn):
    update_actions(record, "ENABLE DATASET", conn)


class DisableDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, dataset, data_model_code, version) -> None:
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)
        data_model_table = DataModelTable(schema=metadata)
        with self.db.begin() as conn:

            data_model_id = data_model_table.get_data_model_id(
                data_model_code, version, conn
            )
            dataset_id = datasets_table.get_dataset_id(dataset, data_model_id, conn)
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


@emitter.handle("disable_dataset")
def update_actions_on_dataset_disablement(record, conn):
    update_actions(record, "DISABLE DATASET", conn)


class TagDataModel(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, code, version, tag) -> None:
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_id = data_model_table.get_data_model_id(code, version, conn)
            properties = Properties(
                data_model_table.get_data_model_properties(data_model_id, conn)
            )
            properties.add_tag(tag)
            data_model_table.set_data_model_properties(
                properties.properties, data_model_id, conn
            )
            action = "ADD DATA MODEL TAG"

            record = dict(
                code=code,
                version=version,
                data_model_id=data_model_id,
                action=action,
            )
            emitter.emit("tag_data_model", record, conn)


class UntagDataModel(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, code, version, tag) -> None:
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_id = data_model_table.get_data_model_id(code, version, conn)
            properties = Properties(
                data_model_table.get_data_model_properties(data_model_id, conn)
            )
            properties.remove_tag(tag)
            data_model_table.set_data_model_properties(
                properties.properties, data_model_id, conn
            )
            action = "REMOVE DATA MODEL TAG"

            record = dict(
                code=code,
                version=version,
                data_model_id=data_model_id,
                action=action,
            )
            emitter.emit("tag_data_model", record, conn)


class AddPropertyToDataModel(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, code, version, key, value, force) -> None:
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_id = data_model_table.get_data_model_id(code, version, conn)
            properties = Properties(
                data_model_table.get_data_model_properties(data_model_id, conn)
            )
            properties.add_property(key, value, force)
            data_model_table.set_data_model_properties(
                properties.properties, data_model_id, conn
            )
            action = "ADD DATA MODEL TAG"

            record = dict(
                code=code,
                version=version,
                data_model_id=data_model_id,
                action=action,
            )
            emitter.emit("tag_data_model", record, conn)


class RemovePropertyFromDataModel(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, code, version, key, value) -> None:
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_id = data_model_table.get_data_model_id(code, version, conn)

            properties = Properties(
                data_model_table.get_data_model_properties(data_model_id, conn)
            )
            properties.remove_property(key, value)
            data_model_table.set_data_model_properties(
                properties.properties, data_model_id, conn
            )
            action = "REMOVE DATA MODEL TAG"

            record = dict(
                code=code,
                version=version,
                data_model_id=data_model_id,
                action=action,
            )
            emitter.emit("tag_data_model", record, conn)


@emitter.handle("tag_data_model")
def update_actions_on_data_model_tagging(record, conn):
    update_actions(record, record["action"], conn)


class TagDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, dataset, data_model_code, version, tag) -> None:
        metadata = Schema(METADATA_SCHEMA)
        dataset_table = DatasetsTable(schema=metadata)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_id = data_model_table.get_data_model_id(
                data_model_code, version, conn
            )
            dataset_id = dataset_table.get_dataset_id(dataset, data_model_id, conn)
            properties = Properties(
                dataset_table.get_dataset_properties(data_model_id, conn)
            )
            properties.add_tag(tag)
            dataset_table.set_dataset_properties(
                properties.properties, dataset_id, conn
            )
            action = "ADD DATASET TAG"

            record = dict(
                dataset_id=dataset_id,
                data_model_id=data_model_id,
                version=version,
                action=action,
            )

            emitter.emit("tag_dataset", record, conn)


class UntagDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, dataset, data_model_code, version, tag) -> None:
        metadata = Schema(METADATA_SCHEMA)
        dataset_table = DatasetsTable(schema=metadata)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_id = data_model_table.get_data_model_id(
                data_model_code, version, conn
            )
            dataset_id = dataset_table.get_dataset_id(dataset, data_model_id, conn)
            properties = Properties(
                dataset_table.get_dataset_properties(data_model_id, conn)
            )
            properties.remove_tag(tag)
            dataset_table.set_dataset_properties(
                properties.properties, dataset_id, conn
            )
            action = "ADD DATASET TAG"

            record = dict(
                dataset_id=dataset_id,
                data_model_id=data_model_id,
                version=version,
                action=action,
            )

            emitter.emit("tag_dataset", record, conn)


class AddPropertyToDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, dataset, data_model_code, version, key, value, force) -> None:
        metadata = Schema(METADATA_SCHEMA)
        dataset_table = DatasetsTable(schema=metadata)
        data_model_table = DataModelTable(schema=metadata)
        with self.db.begin() as conn:
            data_model_id = data_model_table.get_data_model_id(
                data_model_code, version, conn
            )
            dataset_id = dataset_table.get_dataset_id(dataset, data_model_id, conn)
            properties = Properties(
                dataset_table.get_dataset_properties(data_model_id, conn)
            )
            properties.add_property(key, value, force)
            dataset_table.set_dataset_properties(
                properties.properties, dataset_id, conn
            )
            action = "ADD DATASET TAG"

            record = dict(
                dataset_id=dataset_id,
                data_model_id=data_model_id,
                version=version,
                action=action,
            )

            emitter.emit("tag_dataset", record, conn)


class RemovePropertyFromDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self, dataset, data_model_code, version, key, value) -> None:
        metadata = Schema(METADATA_SCHEMA)
        dataset_table = DatasetsTable(schema=metadata)
        data_model_table = DataModelTable(schema=metadata)
        with self.db.begin() as conn:
            data_model_id = data_model_table.get_data_model_id(
                data_model_code, version, conn
            )
            dataset_id = dataset_table.get_dataset_id(dataset, data_model_id, conn)
            properties = Properties(
                dataset_table.get_dataset_properties(data_model_id, conn)
            )
            properties.remove_property(key, value)
            dataset_table.set_dataset_properties(
                properties.properties, dataset_id, conn
            )
            action = "REMOVE DATASET TAG"

            record = dict(
                dataset_id=dataset_id,
                data_model_id=data_model_id,
                version=version,
                action=action,
            )

            emitter.emit("tag_dataset", record, conn)


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


class ListDataModels(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self) -> None:
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_row_columns = [
                "data_model_id",
                "code",
                "version",
                "label",
                "status",
            ]
            data_model_rows = data_model_table.get_data_models(
                db=conn, columns=data_model_row_columns
            )

            dataset_count_by_data_model_id = {
                data_model_id: dataset_count
                for data_model_id, dataset_count in data_model_table.get_dataset_count_by_data_model_id(
                    conn
                )
            }

            data_models_info = []

            for row in data_model_rows:
                data_model_id, *_ = row
                dataset_count = (
                    dataset_count_by_data_model_id[data_model_id]
                    if data_model_id in dataset_count_by_data_model_id
                    else 0
                )
                data_model_info = list(row) + [dataset_count]
                data_models_info.append(data_model_info)

            if not data_models_info:
                print("There are no data models.")
                return

            data_model_info_columns = data_model_row_columns + ["count"]
            df = pa.DataFrame(data_models_info, columns=data_model_info_columns)
            print(df)


class ListDatasets(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self) -> None:
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)
        dataset_table = DatasetsTable(schema=metadata)

        with self.db.begin() as conn:
            dataset_row_columns = [
                "dataset_id",
                "data_model_id",
                "code",
                "label",
                "status",
            ]
            dataset_rows = dataset_table.get_datasets(conn, columns=dataset_row_columns)

            data_model_fullname_by_data_model_id = {
                data_model_id: get_data_model_fullname(code, version)
                for data_model_id, code, version in data_model_table.get_data_models(
                    conn, ["data_model_id", "code", "version"]
                )
            }

            datasets_info = []
            for row in dataset_rows:
                _, data_model_id, dataset_code, *_ = row
                data_model_fullname = data_model_fullname_by_data_model_id[
                    data_model_id
                ]

                dataset_count = {
                    dataset: dataset_count
                    for dataset, dataset_count in dataset_table.get_data_count_by_dataset(
                        data_model_fullname, conn
                    )
                }[dataset_code]

                dataset_info = list(row) + [dataset_count]
                datasets_info.append(dataset_info)

            if not datasets_info:
                print("There are no datasets.")
                return

            dataset_info_columns = dataset_row_columns + ["count"]
            df = pa.DataFrame(datasets_info, columns=dataset_info_columns)
            print(df)
