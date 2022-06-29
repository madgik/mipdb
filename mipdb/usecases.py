import datetime
import json
from abc import ABC, abstractmethod

import pandas as pa

from mipdb.database import DataBase, Connection
from mipdb.database import METADATA_SCHEMA
from mipdb.dataelements import get_system_column_metadata
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


class UseCase(ABC):
    """Abstract use case class."""

    @abstractmethod
    def execute(self, *args, **kwargs) -> None:
        """Executes use case logic with arguments from cli command. Has side
        effects but no return values."""


def is_db_initialized(db):
    metadata = Schema(METADATA_SCHEMA)
    data_model_table = DataModelTable(schema=metadata)
    datasets_table = DatasetsTable(schema=metadata)
    actions_table = ActionsTable(schema=metadata)

    with db.begin() as conn:
        if (
            "mipdb_metadata" in db.get_schemas()
            and data_model_table.exists(conn)
            and datasets_table.exists(conn)
            and actions_table.exists(conn)
        ):
            return True
        else:
            raise UserInputError("You need to initialize the database!\n "
                                 "Try mipdb init --port <db_port>")


class InitDB(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db

    def execute(self) -> None:
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)
        datasets_table = DatasetsTable(schema=metadata)
        actions_table = ActionsTable(schema=metadata)

        with self.db.begin() as conn:
            if "mipdb_metadata" not in self.db.get_schemas():
                metadata.create(conn)
            if not data_model_table.exists(conn):
                data_model_table.drop_sequence(conn)
                data_model_table.create(conn)
            if not datasets_table.exists(conn):
                datasets_table.drop_sequence(conn)
                datasets_table.create(conn)
            if not actions_table.exists(conn):
                actions_table.drop_sequence(conn)
                actions_table.create(conn)


class AddDataModel(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

    def execute(self, data_model_metadata) -> None:
        code = data_model_metadata["code"]
        version = data_model_metadata["version"]
        name = get_data_model_fullname(code, version)
        cdes = make_cdes(data_model_metadata)
        cdes.append(get_system_column_metadata())
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_id = data_model_table.get_next_data_model_id(conn)
            schema = self._create_schema(name, conn)
            self._create_primary_data_table(schema, cdes, conn)
            self._create_metadata_table(schema, conn, cdes)
            values = dict(
                data_model_id=data_model_id,
                code=code,
                version=version,
                label=data_model_metadata["label"],
                status="ENABLED",
            )
            data_model_table.insert_values(values, conn)

            data_model_details = _get_data_model_details(data_model_id, conn)
            update_actions(
                conn=conn,
                action="ADD DATA MODEL",
                data_model_details=data_model_details,
            )

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


class DeleteDataModel(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

    def execute(self, code, version, force) -> None:
        name = get_data_model_fullname(code, version)
        schema = Schema(name)
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_id = data_model_table.get_data_model_id(code, version, conn)
            if not force:
                self._validate_data_model_deletion(name, data_model_id, conn)

            data_model_details = _get_data_model_details(data_model_id, conn)
            self._delete_datasets(data_model_id, code, version)
            schema.drop(conn)
            data_model_table.delete_data_model(code, version, conn)
            update_actions(
                conn=conn,
                action="DELETE DATA MODEL",
                data_model_details=data_model_details,
            )

    def _validate_data_model_deletion(self, data_model_name, data_model_id, conn):
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)
        datasets = datasets_table.get_datasets(conn, data_model_id)
        if not len(datasets) == 0:
            raise ForeignKeyError(
                f"The Data Model:{data_model_name} cannot be deleted because it contains Datasets: {datasets}"
                f"\nIf you want to force delete everything, please use the  '--force' flag"
            )

    def _delete_datasets(self, data_model_id, data_model_code, data_model_version):
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)
        with self.db.begin() as conn:
            dataset_rows = datasets_table.get_datasets(
                data_model_id=data_model_id, columns=["code"], db=conn
            )
        dataset_codes = [dataset_row[0] for dataset_row in dataset_rows]

        for dataset_code in dataset_codes:
            DeleteDataset(self.db).execute(
                dataset_code,
                data_model_code=data_model_code,
                data_model_version=data_model_version,
            )


class AddDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

    def execute(self, dataset_data, data_model_code, data_model_version) -> None:
        dataset = Dataset(dataset_data)

        data_model_name = get_data_model_fullname(
            code=data_model_code, version=data_model_version
        )
        data_model = Schema(data_model_name)
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)
        datasets_table = DatasetsTable(schema=metadata)

        with self.db.begin() as conn:
            metadata_table = MetadataTable.from_db(data_model, conn)
            dataset_enumerations = json.loads(metadata_table.table["dataset"].metadata)[
                "enumerations"
            ]
            dataset_id = self._get_next_dataset_id(conn)
            data_model_id = data_model_table.get_data_model_id(
                data_model_code, data_model_version, conn
            )

            primary_data_table = PrimaryDataTable.from_db(data_model, conn)
            self._verify_dataset_does_not_exist(data_model_id, dataset, conn)
            primary_data_table.insert_dataset(dataset, conn)
            label = dataset_enumerations[dataset.name]

            values = dict(
                data_model_id=data_model_id,
                dataset_id=dataset_id,
                code=dataset.name,
                label=label,
                status="ENABLED",
            )
            datasets_table.insert_values(values, conn)

            data_model_details = _get_data_model_details(data_model_id, conn)
            dataset_details = _get_dataset_details(dataset_id, conn)
            update_actions(
                conn=conn,
                action="ADD DATASET",
                data_model_details=data_model_details,
                dataset_details=dataset_details,
            )

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


class ValidateDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

    def execute(self, dataset_data, data_model_code, data_model_version) -> None:
        dataset = Dataset(dataset_data)
        data_model_name = get_data_model_fullname(
            code=data_model_code, version=data_model_version
        )
        data_model = Schema(data_model_name)

        with self.db.begin() as conn:
            metadata_table = MetadataTable.from_db(data_model, conn)
            dataset.validate_dataset(metadata_table.table)


class DeleteDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

    def execute(self, dataset_code, data_model_code, data_model_version) -> None:
        data_model_name = get_data_model_fullname(
            code=data_model_code, version=data_model_version
        )
        data_model = Schema(data_model_name)
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)
        datasets_table = DatasetsTable(schema=metadata)

        with self.db.begin() as conn:
            primary_data_table = PrimaryDataTable.from_db(data_model, conn)
            primary_data_table.remove_dataset(dataset_code, data_model_name, conn)
            data_model_id = data_model_table.get_data_model_id(
                data_model_code, data_model_version, conn
            )
            dataset_id = datasets_table.get_dataset_id(
                dataset_code, data_model_id, conn
            )

            data_model_details = _get_data_model_details(data_model_id, conn)
            dataset_details = _get_dataset_details(dataset_id, conn)
            datasets_table.delete_dataset(dataset_id, data_model_id, conn)
            update_actions(
                conn=conn,
                action="DELETE DATASET",
                data_model_details=data_model_details,
                dataset_details=dataset_details,
            )


class EnableDataModel(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

    def execute(self, code, version) -> None:
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_id = data_model_table.get_data_model_id(code, version, conn)
            current_status = data_model_table.get_data_model_status(data_model_id, conn)
            if current_status != "ENABLED":
                data_model_table.set_data_model_status("ENABLED", data_model_id, conn)
                data_model_details = _get_data_model_details(data_model_id, conn)
                update_actions(
                    conn=conn,
                    action="ENABLE DATA MODEL",
                    data_model_details=data_model_details,
                )

            else:
                raise UserInputError("The data model was already enabled")


class DisableDataModel(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

    def execute(self, code, version) -> None:
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_id = data_model_table.get_data_model_id(code, version, conn)
            current_status = data_model_table.get_data_model_status(data_model_id, conn)

            if current_status != "DISABLED":
                data_model_table.set_data_model_status("DISABLED", data_model_id, conn)
                data_model_details = _get_data_model_details(data_model_id, conn)
                update_actions(
                    conn=conn,
                    action="DISABLE DATA MODEL",
                    data_model_details=data_model_details,
                )
            else:
                raise UserInputError("The data model was already disabled")


class EnableDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

    def execute(self, dataset_code, data_model_code, data_model_version) -> None:
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:

            data_model_id = data_model_table.get_data_model_id(
                data_model_code, data_model_version, conn
            )
            dataset_id = datasets_table.get_dataset_id(
                dataset_code, data_model_id, conn
            )
            current_status = datasets_table.get_dataset_status(dataset_id, conn)
            if current_status != "ENABLED":
                datasets_table.set_dataset_status("ENABLED", dataset_id, conn)

                data_model_details = _get_data_model_details(data_model_id, conn)
                dataset_details = _get_dataset_details(dataset_id, conn)
                update_actions(
                    conn=conn,
                    action="ENABLE DATASET",
                    data_model_details=data_model_details,
                    dataset_details=dataset_details,
                )
            else:
                raise UserInputError("The dataset was already enabled")


class DisableDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

    def execute(self, dataset_code, data_model_code, data_model_version) -> None:
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)
        data_model_table = DataModelTable(schema=metadata)
        with self.db.begin() as conn:

            data_model_id = data_model_table.get_data_model_id(
                data_model_code, data_model_version, conn
            )
            dataset_id = datasets_table.get_dataset_id(
                dataset_code, data_model_id, conn
            )
            current_status = datasets_table.get_dataset_status(dataset_id, conn)
            if current_status != "DISABLED":
                datasets_table.set_dataset_status("DISABLED", dataset_id, conn)

                data_model_details = _get_data_model_details(data_model_id, conn)
                dataset_details = _get_dataset_details(dataset_id, conn)
                update_actions(
                    conn=conn,
                    action="DISABLE DATASET",
                    data_model_details=data_model_details,
                    dataset_details=dataset_details,
                )

            else:
                raise UserInputError("The dataset was already disabled")


class TagDataModel(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

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

            data_model_details = _get_data_model_details(data_model_id, conn)
            update_actions(
                conn=conn,
                action="ADD DATA MODEL TAG",
                data_model_details=data_model_details,
            )


class UntagDataModel(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

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

            data_model_details = _get_data_model_details(data_model_id, conn)
            update_actions(
                conn=conn,
                action="REMOVE DATA MODEL TAG",
                data_model_details=data_model_details,
            )


class AddPropertyToDataModel(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

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

            data_model_details = _get_data_model_details(data_model_id, conn)
            update_actions(
                conn=conn,
                action="ADD DATA MODEL TAG",
                data_model_details=data_model_details,
            )


class RemovePropertyFromDataModel(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

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

            data_model_details = _get_data_model_details(data_model_id, conn)
            update_actions(
                conn=conn,
                action="REMOVE DATA MODEL TAG",
                data_model_details=data_model_details,
            )


class TagDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

    def execute(self, dataset_code, data_model_code, data_model_version, tag) -> None:
        metadata = Schema(METADATA_SCHEMA)
        dataset_table = DatasetsTable(schema=metadata)
        data_model_table = DataModelTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_id = data_model_table.get_data_model_id(
                data_model_code, data_model_version, conn
            )
            dataset_id = dataset_table.get_dataset_id(dataset_code, data_model_id, conn)
            properties = Properties(
                dataset_table.get_dataset_properties(data_model_id, conn)
            )
            properties.add_tag(tag)
            dataset_table.set_dataset_properties(
                properties.properties, dataset_id, conn
            )

            data_model_details = _get_data_model_details(data_model_id, conn)
            dataset_details = _get_dataset_details(dataset_id, conn)
            update_actions(
                conn=conn,
                action="ADD DATASET TAG",
                data_model_details=data_model_details,
                dataset_details=dataset_details,
            )


class UntagDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

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

            data_model_details = _get_data_model_details(data_model_id, conn)
            dataset_details = _get_dataset_details(dataset_id, conn)
            update_actions(
                conn=conn,
                action="REMOVE DATASET TAG",
                data_model_details=data_model_details,
                dataset_details=dataset_details,
            )


class AddPropertyToDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

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
            data_model_details = _get_data_model_details(data_model_id, conn)
            dataset_details = _get_dataset_details(dataset_id, conn)
            update_actions(
                conn=conn,
                action="ADD DATASET TAG",
                data_model_details=data_model_details,
                dataset_details=dataset_details,
            )


class RemovePropertyFromDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

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
            data_model_details = _get_data_model_details(data_model_id, conn)
            dataset_details = _get_dataset_details(dataset_id, conn)
            update_actions(
                conn=conn,
                action="REMOVE DATASET TAG",
                data_model_details=data_model_details,
                dataset_details=dataset_details,
            )


class ListDataModels(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

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
        is_db_initialized(db)

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


class Cleanup(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

    def execute(self) -> None:
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)
        data_model_rows = []

        with self.db.begin() as conn:
            data_model_row_columns = [
                "code",
                "version",
            ]
            data_model_rows = data_model_table.get_data_models(
                conn, columns=data_model_row_columns
            )

        for data_model_row in data_model_rows:
            code, version = data_model_row
            DeleteDataModel(self.db).execute(code=code, version=version, force=True)


def get_data_model_fullname(code, version):
    return f"{code}:{version}"


class DatasetDetails:
    def __init__(self, dataset_id, code, label):
        self.dataset_id = dataset_id
        self.code = code
        self.label = label


class DataModelDetails:
    def __init__(self, data_model_id, code, version, label):
        self.data_model_id = data_model_id
        self.code = code
        self.version = version
        self.label = label


def update_actions(
    conn,
    action,
    data_model_details: DataModelDetails,
    dataset_details: DatasetDetails = None,
):
    metadata = Schema(METADATA_SCHEMA)
    actions_table = ActionsTable(schema=metadata)

    record = dict(
        data_model_id=data_model_details.data_model_id,
        data_model_code=data_model_details.code,
        data_model_label=data_model_details.label,
        data_model_version=data_model_details.version,
    )

    if dataset_details:
        record["dataset_code"] = dataset_details.code
        record["dataset_id"] = dataset_details.dataset_id
        record["dataset_label"] = dataset_details.label

    record["action"] = action
    record["user"] = conn.get_current_user()
    record["date"] = datetime.datetime.now().isoformat()

    action_record = dict()
    action_record["action_id"] = actions_table.get_next_id(conn)
    action_record["action"] = json.dumps(record)
    actions_table.insert_values(action_record, conn)


def _get_data_model_details(data_model_id, conn):
    metadata = Schema(METADATA_SCHEMA)
    data_model_table = DataModelTable(schema=metadata)
    code, version, label = data_model_table.get_data_model(
        data_model_id=data_model_id, db=conn, columns=["code", "version", "label"]
    )
    return DataModelDetails(data_model_id, code, version, label)


def _get_dataset_details(dataset_id, conn):
    metadata = Schema(METADATA_SCHEMA)
    dataset_table = DatasetsTable(schema=metadata)
    code, label = dataset_table.get_dataset(conn, dataset_id, ["code", "label"])
    return DatasetDetails(dataset_id, code, label)
