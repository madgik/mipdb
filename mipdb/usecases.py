import copy
import datetime
import json
from abc import ABC, abstractmethod

import pandas as pd

from mipdb.database import DataBase
from mipdb.database import METADATA_SCHEMA
from mipdb.data_frame_schema import DataFrameSchema
from mipdb.exceptions import ForeignKeyError, InvalidDatasetError
from mipdb.exceptions import UserInputError
from mipdb.properties import Properties
from mipdb.reader import CSVDataFrameReader
from mipdb.schema import Schema
from mipdb.dataelements import (
    flatten_cdes,
    validate_dataset_present_on_cdes_with_proper_format,
    validate_longitudinal_data_model,
    get_sql_type_per_column,
    get_cdes_with_min_max,
    get_cdes_with_enumerations,
    get_dataset_enums,
)
from mipdb.tables import (
    DataModelTable,
    DatasetsTable,
    ActionsTable,
    MetadataTable,
    PrimaryDataTable,
    TemporaryTable,
    RECORDS_PER_COPY,
)
from mipdb.data_frame import DataFrame, DATASET_COLUMN_NAME

LONGITUDINAL = "longitudinal"


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
            raise UserInputError(
                "You need to initialize the database!\n "
                "Try mipdb init --port <db_port>"
            )


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
        cdes = flatten_cdes(copy.deepcopy(data_model_metadata))
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
            AddPropertyToDataModel(self.db).execute(
                code=code,
                version=version,
                key="cdes",
                value=data_model_metadata,
                force=True,
            )
            if LONGITUDINAL in data_model_metadata:
                longitudinal = data_model_metadata[LONGITUDINAL]
                if not isinstance(longitudinal, bool):
                    raise UserInputError(f"Longitudinal flag should be boolean, value given: {longitudinal}")
                if longitudinal:
                    TagDataModel(self.db).execute(
                        code=code, version=version, tag=LONGITUDINAL
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


class ValidateDataModel(UseCase):
    def execute(self, data_model_metadata) -> None:
        if "version" not in data_model_metadata:
            raise UserInputError("You need to include a version on the CDEsMetadata.json")

        cdes = flatten_cdes(copy.deepcopy(data_model_metadata))
        validate_dataset_present_on_cdes_with_proper_format(cdes)
        if LONGITUDINAL in data_model_metadata:
            longitudinal = data_model_metadata[LONGITUDINAL]
            if not isinstance(longitudinal, bool):
                raise UserInputError(f"Longitudinal flag should be boolean, value given: {longitudinal}")
            if longitudinal:
                validate_longitudinal_data_model(cdes)


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
        datasets = datasets_table.get_values(conn, data_model_id)
        if not len(datasets) == 0:
            raise ForeignKeyError(
                f"The Data Model:{data_model_name} cannot be deleted because it contains Datasets: {datasets}"
                f"\nIf you want to force delete everything, please use the  '--force' flag"
            )

    def _delete_datasets(self, data_model_id, data_model_code, data_model_version):
        metadata = Schema(METADATA_SCHEMA)
        datasets_table = DatasetsTable(schema=metadata)
        with self.db.begin() as conn:
            dataset_codes = datasets_table.get_values(
                data_model_id=data_model_id, columns=["code"], db=conn
            )

        for dataset_code in dataset_codes:
            DeleteDataset(self.db).execute(
                dataset_code,
                data_model_code=data_model_code,
                data_model_version=data_model_version,
            )


class ImportCSV(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

    def execute(
        self, csv_path, copy_from_file, data_model_code, data_model_version
    ) -> None:
        data_model_name = get_data_model_fullname(
            code=data_model_code, version=data_model_version
        )
        data_model = Schema(data_model_name)
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)
        datasets_table = DatasetsTable(schema=metadata)

        with self.db.begin() as conn:
            data_model_id = data_model_table.get_data_model_id(
                data_model_code, data_model_version, conn
            )
            metadata_table = MetadataTable.from_db(data_model, conn)
            cdes = metadata_table.table
            dataset_enumerations = get_dataset_enums(cdes)
            sql_type_per_column = get_sql_type_per_column(cdes)

            if copy_from_file:
                imported_datasets = self.import_csv_with_volume(
                    csv_path=csv_path,
                    sql_type_per_column=sql_type_per_column,
                    data_model=data_model,
                    conn=conn,
                )
            else:
                imported_datasets = self._import_csv(
                    csv_path=csv_path, data_model=data_model, conn=conn
                )

            existing_datasets = datasets_table.get_values(
                columns=["code"], data_model_id=data_model_id, db=conn
            )
            for dataset in set(imported_datasets) - set(existing_datasets):
                dataset_id = self._get_next_dataset_id(conn)
                values = dict(
                    data_model_id=data_model_id,
                    dataset_id=dataset_id,
                    code=dataset,
                    label=dataset_enumerations[dataset],
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

    def _create_temporary_table(self, dataframe_sql_type_per_column, conn):
        temporary_table = TemporaryTable(dataframe_sql_type_per_column, conn)
        temporary_table.create(conn)
        return temporary_table

    def import_csv_with_volume(self, csv_path, sql_type_per_column, data_model, conn):
        csv_columns = pd.read_csv(csv_path, nrows=0).columns.tolist()
        dataframe_sql_type_per_column = {
            dataframe_column: sql_type_per_column[dataframe_column]
            for dataframe_column in csv_columns
        }
        temporary_table = self._create_temporary_table(
            dataframe_sql_type_per_column, conn
        )
        imported_datasets = self.insert_csv_to_db(
            csv_path, temporary_table, data_model, conn
        )
        temporary_table.drop(conn)
        return imported_datasets

    # The monetdb copy into prohibites the importion of a csv,
    # to a table if the csv contains fewer columns than the table.
    # In our case we need to load the csv a table named 'primary_data'.
    # Minimal example:
    # TABLE 'primary_data': col1 col2 | CSV: col1
    #                       1    2    |      5
    #                       2    4    |      6
    #                       3    6    |      7
    #                       4    8    |      8
    # We need to create a 'temp' table which will mirror the columns of the csv.
    # Thus making the copy into a valid choice again.
    # The workaround for that is that we first copy the value into the 'temp'.
    # Once the data is loaded in the 'temp',
    # we can now pass the values of the 'temp' to the 'primary_data' table using a query with the format:
    # INSERT INTO table1 (columns)
    # SELECT columns
    # FROM table2;
    # What about the missing columns at that the 'primary_data' contains but the 'temp' does not contain,
    # we will simply add null in place of the missing columns.
    # In our case the query will have the form:
    # INSERT INTO 'primary_data' (col1, col2)
    # SELECT col1, NULL
    # FROM 'temp'

    def insert_csv_to_db(self, csv_path, temporary_table, data_model, db):
        primary_data_table = PrimaryDataTable.from_db(data_model, db)
        offset = 2
        imported_datasets = []
        # If we load a csv to 'temp' and then insert them to the 'primary_data' in the case of a big file (3gb),
        # for a sort period of time will have a spike of memory usage because the data will be stored in both tables.
        # The workaround for that is to load the csv in batches.
        while True:
            temporary_table.load_csv(
                csv_path=csv_path, offset=offset, records=RECORDS_PER_COPY, db=db
            )
            offset += RECORDS_PER_COPY

            table_count = temporary_table.get_row_count(db=db)
            if not table_count:
                break

            imported_datasets = set(imported_datasets) | set(
                temporary_table.get_column_distinct(DATASET_COLUMN_NAME, db)
            )
            db.copy_data_table_to_another_table(primary_data_table, temporary_table)
            temporary_table.delete(db)

            # If the temp contains fewer rows than RECORDS_PER_COPY
            # that means we have read all the records in the csv and we need to stop the iteration.
            if table_count < RECORDS_PER_COPY:
                break

        return imported_datasets

    def _import_csv(self, csv_path, data_model, conn):
        imported_datasets = []
        primary_data_table = PrimaryDataTable.from_db(data_model, conn)
        with CSVDataFrameReader(csv_path).get_reader() as reader:
            for dataset_data in reader:
                dataframe = DataFrame(dataset_data)
                imported_datasets = set(imported_datasets) | set(dataframe.datasets)
                values = dataframe.to_dict()
                primary_data_table.insert_values(values, conn)
        return imported_datasets


def are_data_valid_longitudinal(csv_path):
    df = pd.read_csv(csv_path, usecols=["subjectid", "visitid"])
    check_unique_longitudinal_dataset_primary_keys(df)
    check_subjectid_is_full(df)
    check_visitid_is_full(df)


def check_unique_longitudinal_dataset_primary_keys(df):
    duplicates = df[df.duplicated(subset=["visitid", "subjectid"], keep=False)]
    if not duplicates.empty:
        raise InvalidDatasetError(
            f"Invalid csv: the following visitid and subjectid pairs are duplicated:\n{duplicates}"
        )


def check_subjectid_is_full(df):
    if df["subjectid"].isnull().any():
        raise InvalidDatasetError("Column 'subjectid' should never contain null values")


def check_visitid_is_full(df):
    if df["visitid"].isnull().any():
        raise InvalidDatasetError("Column 'visitid' should never contain null values")


class ValidateDataset(UseCase):
    """
    We separate the data validation from the importation to make sure that a csv is valid as a whole before committing it to the main table.
    In the data validation we use chunking in order to reduce the memory footprint of the process.
    Database constraints must NOT be used as part of the validation process since that could result in partially imported csvs.
    """

    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

    def execute(
        self, csv_path, copy_from_file, data_model_code, data_model_version
    ) -> None:
        data_model_name = get_data_model_fullname(
            code=data_model_code, version=data_model_version
        )
        data_model = Schema(data_model_name)

        with self.db.begin() as conn:
            csv_columns = pd.read_csv(csv_path, nrows=0).columns.tolist()
            if DATASET_COLUMN_NAME not in csv_columns:
                raise InvalidDatasetError(
                    "The 'dataset' column is required to exist in the csv."
                )
            metadata_table = MetadataTable.from_db(data_model, conn)
            cdes = metadata_table.table
            sql_type_per_column = get_sql_type_per_column(cdes)
            cdes_with_min_max = get_cdes_with_min_max(cdes, csv_columns)
            cdes_with_enumerations = get_cdes_with_enumerations(cdes, csv_columns)
            dataset_enumerations = get_dataset_enums(cdes)
            if self.is_data_model_longitudinal(
                data_model_code, data_model_version, conn
            ):
                are_data_valid_longitudinal(csv_path)

            if copy_from_file:
                validated_datasets = self.validate_csv_with_volume(
                    csv_path,
                    sql_type_per_column,
                    cdes_with_min_max,
                    cdes_with_enumerations,
                    conn,
                )
            else:
                validated_datasets = self.validate_csv(
                    csv_path,
                    sql_type_per_column,
                    cdes_with_min_max,
                    cdes_with_enumerations,
                )
            self.verify_datasets_exist_in_enumerations(
                datasets=validated_datasets,
                dataset_enumerations=dataset_enumerations,
            )

    def is_data_model_longitudinal(self, data_model_code, data_model_version, conn):
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)
        data_model_id = data_model_table.get_data_model_id(
            data_model_code, data_model_version, conn
        )
        properties = data_model_table.get_data_model_properties(data_model_id, conn)
        return "longitudinal" in json.loads(properties)["tags"]

    def validate_csv(
        self, csv_path, sql_type_per_column, cdes_with_min_max, cdes_with_enumerations
    ):
        imported_datasets = []

        csv_columns = pd.read_csv(csv_path, nrows=0).columns.tolist()
        dataframe_schema = DataFrameSchema(
            sql_type_per_column, cdes_with_min_max, cdes_with_enumerations, csv_columns
        )
        with CSVDataFrameReader(csv_path).get_reader() as reader:
            for dataset_data in reader:
                dataframe = DataFrame(dataset_data)
                dataframe_schema.validate_dataframe(dataframe.data)
                imported_datasets = set(imported_datasets) | set(dataframe.datasets)
        return imported_datasets

    def validate_csv_with_volume(
        self,
        csv_path,
        sql_type_per_column,
        cdes_with_min_max,
        cdes_with_enumerations,
        conn,
    ):
        csv_columns = pd.read_csv(csv_path, nrows=0).columns.tolist()
        dataframe_sql_type_per_column = self._get_dataframe_sql_type_per_column(
            csv_columns, sql_type_per_column
        )
        temporary_table = self._create_temporary_table(
            dataframe_sql_type_per_column, conn
        )
        validated_datasets = temporary_table.validate_csv(
            csv_path, cdes_with_min_max, cdes_with_enumerations, conn
        )
        temporary_table.drop(conn)
        return validated_datasets

    def _get_dataframe_sql_type_per_column(self, csv_columns, sql_type_per_column):
        if set(csv_columns) <= set(sql_type_per_column.keys()):
            return {
                dataframe_column: sql_type_per_column[dataframe_column]
                for dataframe_column in csv_columns
            }
        raise InvalidDatasetError(
            f"Columns:{set(csv_columns) - set(sql_type_per_column.keys()) - {'row_id'}} are not present in the CDEs"
        )

    def _create_temporary_table(self, dataframe_sql_type_per_column, conn):
        temporary_table = TemporaryTable(dataframe_sql_type_per_column, conn)
        temporary_table.create(conn)
        return temporary_table

    def verify_datasets_exist_in_enumerations(self, datasets, dataset_enumerations):
        non_existing_datasets = [
            dataset for dataset in datasets if dataset not in dataset_enumerations
        ]
        if non_existing_datasets:
            raise InvalidDatasetError(
                f"The values:'{non_existing_datasets}' are not present in the enumerations of the CDE 'dataset'."
            )


class ValidateDatasetNoDatabase(UseCase):
    """
    We separate the data validation from the importation to make sure that a csv is valid as a whole before committing it to the main table.
    In the data validation we use chunking in order to reduce the memory footprint of the process.
    Database constraints must NOT be used as part of the validation process since that could result in partially imported csvs.
    """

    def execute(self, csv_path, data_model_metadata) -> None:

        csv_columns = pd.read_csv(csv_path, nrows=0).columns.tolist()
        if DATASET_COLUMN_NAME not in csv_columns:
            raise InvalidDatasetError(
                "The 'dataset' column is required to exist in the csv."
            )
        cdes = flatten_cdes(copy.deepcopy(data_model_metadata))
        cdes = {cde.code: cde for cde in cdes}
        sql_type_per_column = get_sql_type_per_column(cdes)
        cdes_with_min_max = get_cdes_with_min_max(cdes, csv_columns)
        cdes_with_enumerations = get_cdes_with_enumerations(cdes, csv_columns)
        dataset_enumerations = get_dataset_enums(cdes)
        if LONGITUDINAL in data_model_metadata:
            longitudinal = data_model_metadata[LONGITUDINAL]
            if not isinstance(longitudinal, bool):
                raise UserInputError(f"Longitudinal flag should be boolean, value given: {longitudinal}")
            if longitudinal:
                are_data_valid_longitudinal(csv_path)
        validated_datasets = self.validate_csv(
            csv_path,
            sql_type_per_column,
            cdes_with_min_max,
            cdes_with_enumerations,
        )
        self.verify_datasets_exist_in_enumerations(
            datasets=validated_datasets,
            dataset_enumerations=dataset_enumerations,
        )

    def validate_csv(
        self, csv_path, sql_type_per_column, cdes_with_min_max, cdes_with_enumerations
    ):
        imported_datasets = []

        csv_columns = pd.read_csv(csv_path, nrows=0).columns.tolist()
        dataframe_schema = DataFrameSchema(
            sql_type_per_column, cdes_with_min_max, cdes_with_enumerations, csv_columns
        )
        with CSVDataFrameReader(csv_path).get_reader() as reader:
            for dataset_data in reader:
                dataframe = DataFrame(dataset_data)
                dataframe_schema.validate_dataframe(dataframe.data)
                imported_datasets = set(imported_datasets) | set(dataframe.datasets)
        return imported_datasets

    def verify_datasets_exist_in_enumerations(self, datasets, dataset_enumerations):
        non_existing_datasets = [
            dataset for dataset in datasets if dataset not in dataset_enumerations
        ]
        if non_existing_datasets:
            raise InvalidDatasetError(
                f"The values:'{non_existing_datasets}' are not present in the enumerations of the CDE 'dataset'."
            )


class DeleteDataset(UseCase):
    def __init__(self, db: DataBase) -> None:
        self.db = db
        is_db_initialized(db)

    def execute(self, dataset_code, data_model_code, data_model_version) -> None:
        data_model_fullname = get_data_model_fullname(
            code=data_model_code, version=data_model_version
        )
        data_model = Schema(data_model_fullname)
        metadata = Schema(METADATA_SCHEMA)
        data_model_table = DataModelTable(schema=metadata)
        datasets_table = DatasetsTable(schema=metadata)

        with self.db.begin() as conn:
            primary_data_table = PrimaryDataTable.from_db(data_model, conn)
            primary_data_table.remove_dataset(dataset_code, data_model_fullname, conn)
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
            df = pd.DataFrame(data_models_info, columns=data_model_info_columns)
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
            dataset_rows = dataset_table.get_values(conn, columns=dataset_row_columns)

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
            df = pd.DataFrame(datasets_info, columns=dataset_info_columns)
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
