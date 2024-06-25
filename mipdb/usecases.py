import copy
import os
from abc import ABC, abstractmethod

import pandas as pd

from mipdb.monetdb import MonetDB
from mipdb.data_frame_schema import DataFrameSchema
from mipdb.exceptions import ForeignKeyError, InvalidDatasetError, UserInputError
from mipdb.monetdb_tables import PrimaryDataTable, TemporaryTable, RECORDS_PER_COPY
from mipdb.properties import Properties
from mipdb.reader import CSVDataFrameReader
from mipdb.dataelements import (
    flatten_cdes,
    validate_dataset_present_on_cdes_with_proper_format,
    validate_longitudinal_data_model,
    get_sql_type_per_column,
    get_cdes_with_min_max,
    get_cdes_with_enumerations,
    get_dataset_enums,
)
from mipdb.schema import Schema
from mipdb.sqlite import SQLiteDB
from mipdb.sqlite_tables import DataModelTable, DatasetsTable, MetadataTable
from mipdb.data_frame import DataFrame, DATASET_COLUMN_NAME

LONGITUDINAL = "longitudinal"


class UseCase(ABC):
    """Abstract use case class."""

    @abstractmethod
    def execute(self, *args, **kwargs) -> None:
        """Executes use case logic with arguments from CLI command."""


def is_db_initialized(db: SQLiteDB):
    if not (DataModelTable().exists(db) and DatasetsTable().exists(db)):
        raise UserInputError("You need to initialize the database!\nTry mipdb init")


class InitDB(UseCase):
    def __init__(self, db: SQLiteDB) -> None:
        self.db = db

    def execute(self) -> None:
        data_model_table, datasets_table = DataModelTable(), DatasetsTable()
        if not data_model_table.exists(self.db):
            data_model_table.create(self.db)
        if not datasets_table.exists(self.db):
            datasets_table.create(self.db)


class AddDataModel(UseCase):
    def __init__(self, sqlite_db: SQLiteDB, monetdb: MonetDB) -> None:
        self.sqlite_db = sqlite_db
        self.monetdb = monetdb
        is_db_initialized(sqlite_db)

    def execute(self, data_model_metadata) -> None:
        code, version = data_model_metadata["code"], data_model_metadata["version"]
        data_model = get_data_model_fullname(code, version)
        cdes = flatten_cdes(copy.deepcopy(data_model_metadata))
        data_model_table = DataModelTable()
        data_model_id = data_model_table.get_next_data_model_id(self.sqlite_db)
        self._create_primary_data_table(data_model, cdes)
        self._create_metadata_table(data_model, cdes)
        properties = Properties(
            data_model_table.get_data_model_properties(data_model_id, self.sqlite_db)
        )
        properties.add_property("cdes", data_model_metadata, True)
        values = dict(
            data_model_id=data_model_id,
            code=code,
            version=version,
            label=data_model_metadata["label"],
            status="ENABLED",
            properties=properties.properties,
        )
        data_model_table.insert_values(values, self.sqlite_db)
        self._tag_longitudinal_if_needed(data_model_metadata, code, version)

    def _create_primary_data_table(self, data_model, cdes):
        with self.monetdb.begin() as conn:
            schema = Schema(data_model)
            schema.create(conn)
            PrimaryDataTable.from_cdes(schema, cdes).create(conn)

    def _create_metadata_table(self, data_model, cdes):
        metadata_table = MetadataTable(data_model)
        metadata_table.create(self.sqlite_db)
        values = metadata_table.get_values_from_cdes(cdes)
        metadata_table.insert_values(values, self.sqlite_db)

    def _tag_longitudinal_if_needed(self, data_model_metadata, code, version):
        if LONGITUDINAL in data_model_metadata:
            longitudinal = data_model_metadata[LONGITUDINAL]
            if not isinstance(longitudinal, bool):
                raise UserInputError(
                    f"Longitudinal flag should be boolean, value given: {longitudinal}"
                )
            if longitudinal:
                TagDataModel(self.sqlite_db).execute(
                    code=code, version=version, tag=LONGITUDINAL
                )


class ValidateDataModel(UseCase):
    def execute(self, data_model_metadata) -> None:
        if "version" not in data_model_metadata:
            raise UserInputError(
                "You need to include a version on the CDEsMetadata.json"
            )
        cdes = flatten_cdes(copy.deepcopy(data_model_metadata))
        validate_dataset_present_on_cdes_with_proper_format(cdes)
        if LONGITUDINAL in data_model_metadata:
            longitudinal = data_model_metadata[LONGITUDINAL]
            if not isinstance(longitudinal, bool):
                raise UserInputError(
                    f"Longitudinal flag should be boolean, value given: {longitudinal}"
                )
            if longitudinal:
                validate_longitudinal_data_model(cdes)


class DeleteDataModel(UseCase):
    def __init__(self, sqlite_db: SQLiteDB, monetdb: MonetDB) -> None:
        self.sqlite_db = sqlite_db
        self.monetdb = monetdb
        is_db_initialized(sqlite_db)

    def execute(self, code, version, force) -> None:
        name = get_data_model_fullname(code, version)
        schema = Schema(name)
        data_model_table = DataModelTable()
        data_model_id = data_model_table.get_data_model_id(
            code, version, self.sqlite_db
        )
        if not force:
            self._validate_data_model_deletion(name, data_model_id)
        MetadataTable(data_model=name).drop(self.sqlite_db)
        self._delete_datasets(data_model_id, code, version)
        with self.monetdb.begin() as conn:
            schema.drop(conn)
        data_model_table.delete_data_model(code, version, self.sqlite_db)

    def _validate_data_model_deletion(self, data_model_name, data_model_id):
        datasets = DatasetsTable().get_dataset_codes(
            db=self.sqlite_db, columns=["code"], data_model_id=data_model_id
        )
        if datasets:
            raise ForeignKeyError(
                f"The Data Model:{data_model_name} cannot be deleted because it contains Datasets: {datasets}\nIf you want to force delete everything, please use the '--force' flag"
            )

    def _delete_datasets(self, data_model_id, data_model_code, data_model_version):
        datasets_table = DatasetsTable()
        dataset_codes = datasets_table.get_dataset_codes(
            data_model_id=data_model_id, columns=["code"], db=self.sqlite_db
        )
        for dataset_code in dataset_codes:
            DeleteDataset(sqlite_db=self.sqlite_db, monetdb=self.monetdb).execute(
                dataset_code,
                data_model_code=data_model_code,
                data_model_version=data_model_version,
            )


class ImportCSV(UseCase):
    def __init__(self, sqlite_db: SQLiteDB, monetdb: MonetDB) -> None:
        self.sqlite_db = sqlite_db
        self.monetdb = monetdb
        is_db_initialized(sqlite_db)

    def execute(
        self, csv_path, copy_from_file, data_model_code, data_model_version
    ) -> None:
        data_model_name = get_data_model_fullname(
            code=data_model_code, version=data_model_version
        )
        data_model = Schema(data_model_name)
        data_model_id = DataModelTable().get_data_model_id(
            data_model_code, data_model_version, self.sqlite_db
        )
        metadata_table = MetadataTable.from_db(data_model_name, self.sqlite_db)
        cdes = metadata_table.table
        dataset_enumerations = get_dataset_enums(cdes)
        sql_type_per_column = get_sql_type_per_column(cdes)
        # In case the DATA_PATH is empty it will return the whole path.
        relative_csv_path = csv_path.split(os.getenv("DATA_PATH"))[-1]

        with self.monetdb.begin() as monetdb_conn:
            imported_datasets = (
                self.import_csv_with_volume(
                    csv_path, sql_type_per_column, data_model, monetdb_conn
                )
                if copy_from_file
                else self._import_csv(relative_csv_path, data_model, monetdb_conn)
            )

        existing_datasets = DatasetsTable().get_dataset_codes(
            columns=["code"], data_model_id=data_model_id, db=self.sqlite_db
        )
        dataset_id = self._get_next_dataset_id()
        for dataset in set(imported_datasets) - set(existing_datasets):
            values = dict(
                data_model_id=data_model_id,
                dataset_id=dataset_id,
                code=dataset,
                label=dataset_enumerations[dataset],
                csv_path=relative_csv_path,
                status="ENABLED",
                properties=None,
            )
            DatasetsTable().insert_values(values, self.sqlite_db)
            dataset_id += 1

    def _get_next_dataset_id(self):
        return DatasetsTable().get_next_dataset_id(self.sqlite_db)

    def _create_temporary_table(self, dataframe_sql_type_per_column, db):
        temporary_table = TemporaryTable(dataframe_sql_type_per_column, db)
        temporary_table.create(db)
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
        offset, imported_datasets = 2, []
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
            if table_count < RECORDS_PER_COPY:
                break
        return imported_datasets

    def _import_csv(self, csv_path, data_model, conn):
        imported_datasets, primary_data_table = [], PrimaryDataTable.from_db(
            data_model, conn
        )
        with CSVDataFrameReader(csv_path).get_reader() as reader:
            for dataset_data in reader:
                dataframe = DataFrame(dataset_data)
                imported_datasets = set(imported_datasets) | set(dataframe.datasets)
                primary_data_table.insert_values(dataframe.to_dict(), conn)
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

    def __init__(self, sqlite_db: SQLiteDB, monetdb: MonetDB) -> None:
        self.sqlite_db = sqlite_db
        self.monetdb = monetdb
        is_db_initialized(sqlite_db)

    def execute(
        self, csv_path, copy_from_file, data_model_code, data_model_version
    ) -> None:
        data_model = get_data_model_fullname(
            code=data_model_code, version=data_model_version
        )
        csv_columns = pd.read_csv(csv_path, nrows=0).columns.tolist()
        if DATASET_COLUMN_NAME not in csv_columns:
            raise InvalidDatasetError(
                "The 'dataset' column is required to exist in the csv."
            )
        metadata_table = MetadataTable.from_db(data_model, self.sqlite_db)
        cdes = metadata_table.table
        sql_type_per_column = get_sql_type_per_column(cdes)
        cdes_with_min_max = get_cdes_with_min_max(cdes, csv_columns)
        cdes_with_enumerations = get_cdes_with_enumerations(cdes, csv_columns)
        dataset_enumerations = get_dataset_enums(cdes)
        if self.is_data_model_longitudinal(data_model_code, data_model_version):
            are_data_valid_longitudinal(csv_path)

        if copy_from_file:
            with self.monetdb.begin() as monetdb_conn:
                validated_datasets = self.validate_csv_with_volume(
                    csv_path,
                    sql_type_per_column,
                    cdes_with_min_max,
                    cdes_with_enumerations,
                    monetdb_conn,
                )
        else:
            validated_datasets = self.validate_csv(
                csv_path, sql_type_per_column, cdes_with_min_max, cdes_with_enumerations
            )
        self.verify_datasets_exist_in_enumerations(
            validated_datasets, dataset_enumerations
        )

    def is_data_model_longitudinal(self, data_model_code, data_model_version):
        data_model_id = DataModelTable().get_data_model_id(
            data_model_code, data_model_version, self.sqlite_db
        )
        properties = DataModelTable().get_data_model_properties(
            data_model_id, self.sqlite_db
        )
        return LONGITUDINAL in properties["tags"]

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
                raise UserInputError(
                    f"Longitudinal flag should be boolean, value given: {longitudinal}"
                )
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
    def __init__(self, sqlite_db: SQLiteDB, monetdb: MonetDB) -> None:
        self.sqlite_db = sqlite_db
        self.monetdb = monetdb
        is_db_initialized(sqlite_db)

    def execute(self, dataset_code, data_model_code, data_model_version) -> None:
        data_model_fullname = get_data_model_fullname(
            code=data_model_code, version=data_model_version
        )
        with self.monetdb.begin() as conn:
            primary_data_table = PrimaryDataTable.from_db(
                Schema(data_model_fullname), conn
            )
            primary_data_table.remove_dataset(dataset_code, data_model_fullname, conn)
        data_model_id = DataModelTable().get_data_model_id(
            data_model_code, data_model_version, self.sqlite_db
        )
        dataset_id = DatasetsTable().get_dataset_id(
            dataset_code, data_model_id, self.sqlite_db
        )
        DatasetsTable().delete_dataset(dataset_id, data_model_id, self.sqlite_db)


class EnableDataModel(UseCase):
    def __init__(self, db: SQLiteDB) -> None:
        self.db = db

    def execute(self, code, version) -> None:
        data_model_table = DataModelTable()
        data_model_id = data_model_table.get_data_model_id(code, version, self.db)
        current_status = data_model_table.get_data_model_status(data_model_id, self.db)
        if current_status != "ENABLED":
            data_model_table.set_data_model_status("ENABLED", data_model_id, self.db)
        else:
            raise UserInputError("The data model was already enabled")


class DisableDataModel(UseCase):
    def __init__(self, db: SQLiteDB) -> None:
        self.db = db

    def execute(self, code, version) -> None:
        data_model_table = DataModelTable()
        data_model_id = data_model_table.get_data_model_id(code, version, self.db)
        current_status = data_model_table.get_data_model_status(data_model_id, self.db)
        if current_status != "DISABLED":
            data_model_table.set_data_model_status("DISABLED", data_model_id, self.db)
        else:
            raise UserInputError("The data model was already disabled")


class EnableDataset(UseCase):
    def __init__(self, db: SQLiteDB) -> None:
        self.db = db

    def execute(self, dataset_code, data_model_code, data_model_version) -> None:
        datasets_table = DatasetsTable()
        data_model_table = DataModelTable()
        data_model_id = data_model_table.get_data_model_id(
            data_model_code, data_model_version, self.db
        )
        dataset_id = datasets_table.get_dataset_id(dataset_code, data_model_id, self.db)
        current_status = datasets_table.get_dataset_status(dataset_id, self.db)
        if current_status != "ENABLED":
            datasets_table.set_dataset_status("ENABLED", dataset_id, self.db)
        else:
            raise UserInputError("The dataset was already enabled")


class DisableDataset(UseCase):
    def __init__(self, db: SQLiteDB) -> None:
        self.db = db

    def execute(self, dataset_code, data_model_code, data_model_version) -> None:
        datasets_table = DatasetsTable()
        data_model_table = DataModelTable()
        data_model_id = data_model_table.get_data_model_id(
            data_model_code, data_model_version, self.db
        )
        dataset_id = datasets_table.get_dataset_id(dataset_code, data_model_id, self.db)
        current_status = datasets_table.get_dataset_status(dataset_id, self.db)
        if current_status != "DISABLED":
            datasets_table.set_dataset_status("DISABLED", dataset_id, self.db)
        else:
            raise UserInputError("The dataset was already disabled")


class TagDataModel(UseCase):
    def __init__(self, db: SQLiteDB) -> None:
        self.db = db

    def execute(self, code, version, tag) -> None:
        data_model_table = DataModelTable()
        data_model_id = data_model_table.get_data_model_id(code, version, self.db)
        properties = Properties(
            data_model_table.get_data_model_properties(data_model_id, self.db)
        )
        properties.add_tag(tag)
        data_model_table.set_data_model_properties(
            properties.properties, data_model_id, self.db
        )


class UntagDataModel(UseCase):
    def __init__(self, db: SQLiteDB) -> None:
        self.db = db

    def execute(self, code, version, tag) -> None:
        data_model_table = DataModelTable()
        data_model_id = data_model_table.get_data_model_id(code, version, self.db)
        properties = Properties(
            data_model_table.get_data_model_properties(data_model_id, self.db)
        )
        properties.remove_tag(tag)
        data_model_table.set_data_model_properties(
            properties.properties, data_model_id, self.db
        )


class AddPropertyToDataModel(UseCase):
    def __init__(self, db: SQLiteDB) -> None:
        self.db = db

    def execute(self, code, version, key, value, force) -> None:
        data_model_table = DataModelTable()
        data_model_id = data_model_table.get_data_model_id(code, version, self.db)
        properties = Properties(
            data_model_table.get_data_model_properties(data_model_id, self.db)
        )
        properties.add_property(key, value, force)
        data_model_table.set_data_model_properties(
            properties.properties, data_model_id, self.db
        )


class RemovePropertyFromDataModel(UseCase):
    def __init__(self, db: SQLiteDB) -> None:
        self.db = db

    def execute(self, code, version, key, value) -> None:
        data_model_table = DataModelTable()
        data_model_id = data_model_table.get_data_model_id(code, version, self.db)
        properties = Properties(
            data_model_table.get_data_model_properties(data_model_id, self.db)
        )
        properties.remove_property(key, value)
        data_model_table.set_data_model_properties(
            properties.properties, data_model_id, self.db
        )


class TagDataset(UseCase):
    def __init__(self, db: SQLiteDB) -> None:
        self.db = db

    def execute(self, dataset_code, data_model_code, data_model_version, tag) -> None:
        datasets_table = DatasetsTable()
        data_model_table = DataModelTable()
        data_model_id = data_model_table.get_data_model_id(
            data_model_code, data_model_version, self.db
        )
        dataset_id = datasets_table.get_dataset_id(dataset_code, data_model_id, self.db)
        properties = Properties(
            datasets_table.get_dataset_properties(data_model_id, self.db)
        )
        properties.add_tag(tag)
        datasets_table.set_dataset_properties(
            properties.properties, dataset_id, self.db
        )


class UntagDataset(UseCase):
    def __init__(self, db: SQLiteDB) -> None:
        self.db = db

    def execute(self, dataset, data_model_code, version, tag) -> None:
        datasets_table = DatasetsTable()
        data_model_table = DataModelTable()
        data_model_id = data_model_table.get_data_model_id(
            data_model_code, version, self.db
        )
        dataset_id = datasets_table.get_dataset_id(dataset, data_model_id, self.db)
        properties = Properties(
            datasets_table.get_dataset_properties(data_model_id, self.db)
        )
        properties.remove_tag(tag)
        datasets_table.set_dataset_properties(
            properties.properties, dataset_id, self.db
        )


class AddPropertyToDataset(UseCase):
    def __init__(self, db: SQLiteDB) -> None:
        self.db = db

    def execute(self, dataset, data_model_code, version, key, value, force) -> None:
        datasets_table = DatasetsTable()
        data_model_table = DataModelTable()
        data_model_id = data_model_table.get_data_model_id(
            data_model_code, version, self.db
        )
        dataset_id = datasets_table.get_dataset_id(dataset, data_model_id, self.db)
        properties = Properties(
            datasets_table.get_dataset_properties(data_model_id, self.db)
        )
        properties.add_property(key, value, force)
        datasets_table.set_dataset_properties(
            properties.properties, dataset_id, self.db
        )


class RemovePropertyFromDataset(UseCase):
    def __init__(self, db: SQLiteDB) -> None:
        self.db = db

    def execute(self, dataset, data_model_code, version, key, value) -> None:
        datasets_table = DatasetsTable()
        data_model_table = DataModelTable()
        data_model_id = data_model_table.get_data_model_id(
            data_model_code, version, self.db
        )
        dataset_id = datasets_table.get_dataset_id(dataset, data_model_id, self.db)
        properties = Properties(
            datasets_table.get_dataset_properties(data_model_id, self.db)
        )
        properties.remove_property(key, value)
        datasets_table.set_dataset_properties(
            properties.properties, dataset_id, self.db
        )


class ListDataModels(UseCase):
    def __init__(self, db: SQLiteDB) -> None:
        self.db = db

    def execute(self) -> None:
        data_model_table = DataModelTable()
        data_model_row_columns = ["data_model_id", "code", "version", "label", "status"]
        data_model_rows = data_model_table.get_data_models(
            db=self.db, columns=data_model_row_columns
        )
        dataset_count_by_data_model_id = {
            data_model_id: dataset_count
            for data_model_id, dataset_count in data_model_table.get_dataset_count_by_data_model_id(
                self.db
            )
        }
        data_models_info = [
            list(row) + [dataset_count_by_data_model_id.get(row[0], 0)]
            for row in data_model_rows
        ]

        if not data_models_info:
            print("There are no data models.")
            return

        df = pd.DataFrame(data_models_info, columns=data_model_row_columns + ["count"])
        print(df)


class ListDatasets(UseCase):
    def __init__(self, sqlite_db: SQLiteDB, monetdb: MonetDB) -> None:
        self.sqlite_db = sqlite_db
        self.monetdb = monetdb

    def execute(self) -> None:
        data_model_table = DataModelTable()
        dataset_table = DatasetsTable()
        dataset_row_columns = ["dataset_id", "data_model_id", "code", "label", "status"]
        dataset_rows = dataset_table.get_datasets(
            self.sqlite_db, columns=dataset_row_columns
        )
        data_model_fullname_by_data_model_id = {
            data_model_id: get_data_model_fullname(code, version)
            for data_model_id, code, version in data_model_table.get_data_models(
                self.sqlite_db, ["data_model_id", "code", "version"]
            )
        }
        datasets_info = []

        for row in dataset_rows:
            data_model_fullname = data_model_fullname_by_data_model_id[row[1]]
            with self.monetdb.begin() as conn:
                primary_data_table = PrimaryDataTable.from_db(
                    Schema(data_model_fullname), conn
                )
                dataset_count = {
                    dataset: count
                    for dataset, count in primary_data_table.get_data_count_by_dataset(
                        data_model_fullname, conn
                    )
                }.get(row[2], 0)
                datasets_info.append(list(row) + [dataset_count])

        if not datasets_info:
            print("There are no datasets.")
            return

        df = pd.DataFrame(datasets_info, columns=dataset_row_columns + ["count"])
        print(df)


class Cleanup(UseCase):
    def __init__(self, sqlite_db: SQLiteDB, monetdb: MonetDB) -> None:
        self.sqlite_db = sqlite_db
        self.monetdb = monetdb

    def execute(self) -> None:
        data_model_table = DataModelTable()
        data_model_rows = data_model_table.get_data_models(
            self.sqlite_db, columns=["code", "version"]
        )

        for code, version in data_model_rows:
            DeleteDataModel(sqlite_db=self.sqlite_db, monetdb=self.monetdb).execute(
                code=code, version=version, force=True
            )


def get_data_model_fullname(code, version):
    return f"{code}:{version}"
