import copy
from abc import ABC, abstractmethod
import pandas as pd

from mipdb.logger import LOGGER
from mipdb.data_frame_schema import DataFrameSchema
from mipdb.exceptions import (
    DataBaseError,
    ForeignKeyError,
    InvalidDatasetError,
    UserInputError,
)
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
from mipdb.data_frame import DataFrame, DATASET_COLUMN_NAME
from mipdb.duckdb import (
    DuckDB,
    DataModelTable,
    DatasetsTable,
    MetadataTable,
    PrimaryDataTable,
    Schema,
)

LONGITUDINAL = "longitudinal"


class UseCase(ABC):
    """Abstract use case class."""

    @abstractmethod
    def execute(self, *args, **kwargs) -> None:
        """Executes use case logic with arguments from CLI command."""


def is_db_initialized(db: DuckDB):
    if not (DataModelTable().exists(db) and DatasetsTable().exists(db)):
        raise UserInputError("You need to initialize the database!\nTry mipdb init")


class InitDB(UseCase):
    def __init__(self, db: DuckDB) -> None:
        self.db = db

    def execute(self) -> None:
        data_model_table, datasets_table = DataModelTable(), DatasetsTable()
        if not data_model_table.exists(self.db):
            data_model_table.create(self.db)
        if not datasets_table.exists(self.db):
            datasets_table.create(self.db)


class AddDataModel(UseCase):
    def __init__(self, duckdb: DuckDB) -> None:
        self.db = duckdb
        is_db_initialized(duckdb)

    def execute(self, data_model_metadata: dict) -> None:
        code, version = data_model_metadata["code"], data_model_metadata["version"]
        data_model = get_data_model_fullname(code, version)

        cdes = flatten_cdes(copy.deepcopy(data_model_metadata))
        self._create_primary_data_table(data_model, cdes)

        self._create_metadata_table(data_model, cdes)

        self._insert_data_model_row(data_model_metadata)

        self._tag_longitudinal_if_needed(data_model_metadata, code, version)

    def _create_primary_data_table(self, data_model: str, cdes: list) -> None:
        schema = Schema(data_model)
        primary_table = PrimaryDataTable.from_cdes(schema, cdes)
        if primary_table.exists(self.db):
            primary_table.drop(self.db)
        primary_table.create(self.db)

    def _create_metadata_table(self, data_model: str, cdes: list) -> None:
        metadata_table = MetadataTable(data_model)
        if metadata_table.exists(self.db):
            metadata_table.drop(self.db)
        metadata_table.create(self.db)
        metadata_table.insert_values(
            metadata_table.get_values_from_cdes(cdes), self.db
        )

    def _insert_data_model_row(self, data_model_metadata: dict) -> None:
        code, version = data_model_metadata["code"], data_model_metadata["version"]
        dm_table = DataModelTable()
        new_id = dm_table.get_next_data_model_id(self.db)

        props = Properties(dm_table.get_data_model_properties(new_id, self.db))
        props.add_property("cdes", data_model_metadata, force=True)
        dm_table.insert_values(
            dict(
                data_model_id=new_id,
                code=code,
                version=version,
                label=data_model_metadata["label"],
                status="ENABLED",
                properties=props.properties,
            ),
            self.db,
        )

    def _tag_longitudinal_if_needed(self, meta: dict, code: str, version: str) -> None:
        if LONGITUDINAL not in meta:
            return

        longitudinal = meta[LONGITUDINAL]
        if not isinstance(longitudinal, bool):
            raise UserInputError(
                f"Longitudinal flag should be boolean, value given: {longitudinal}"
            )

        if longitudinal:
            TagDataModel(self.db).execute(
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
    def __init__(self, duckdb: DuckDB) -> None:
        self.db = duckdb
        is_db_initialized(duckdb)

    def execute(self, code, version, force) -> None:

        name = get_data_model_fullname(code, version)
        schema = Schema(name)
        data_model_table = DataModelTable()
        data_model_id = data_model_table.get_data_model_id(code, version, self.db)
        if not force:
            self._validate_data_model_deletion(name, data_model_id)
        MetadataTable(data_model=name).drop(self.db)
        self._delete_datasets(data_model_id, code, version)
        self._drop_primary_data_table(schema)
        data_model_table.delete_data_model(code, version, self.db)

    def _drop_primary_data_table(self, schema):
        try:
            primary_table = PrimaryDataTable.from_db(schema, self.db)
        except DataBaseError:
            return
        primary_table.drop(self.db)

    def _validate_data_model_deletion(self, data_model_name, data_model_id):
        datasets = DatasetsTable().get_dataset_codes(
            db=self.db, columns=["code"], data_model_id=data_model_id
        )
        if datasets:
            raise ForeignKeyError(
                f"The Data Model:{data_model_name} cannot be deleted because it contains Datasets: {datasets}\nIf you want to force delete everything, please use the '--force' flag"
            )

    def _delete_datasets(self, data_model_id, data_model_code, data_model_version):
        datasets_table = DatasetsTable()
        dataset_codes = datasets_table.get_dataset_codes(
            data_model_id=data_model_id, columns=["code"], db=self.db
        )
        for dataset_code in dataset_codes:
            DeleteDataset(duckdb=self.db).execute(
                dataset_code,
                data_model_code=data_model_code,
                data_model_version=data_model_version,
            )


class ImportCSV(UseCase):
    def __init__(self, duckdb: DuckDB) -> None:
        self.db = duckdb
        is_db_initialized(duckdb)

    def execute(self, csv_path, data_model_code, data_model_version) -> None:

        data_model_name = get_data_model_fullname(
            code=data_model_code, version=data_model_version
        )
        data_model = Schema(data_model_name)
        data_model_id = DataModelTable().get_data_model_id(
            data_model_code, data_model_version, self.db
        )
        metadata_table = MetadataTable.from_db(data_model_name, self.db)
        cdes = metadata_table.table
        dataset_enumerations = get_dataset_enums(cdes)

        csv_columns = pd.read_csv(csv_path, nrows=0).columns.tolist()
        imported_datasets = self._import_datasets(csv_path, data_model)

        existing_datasets = DatasetsTable().get_dataset_codes(
            columns=["code"], data_model_id=data_model_id, db=self.db
        )
        dataset_id = self._get_next_dataset_id()
        for dataset in set(imported_datasets) - set(existing_datasets):
            values = dict(
                data_model_id=data_model_id,
                dataset_id=dataset_id,
                code=dataset,
                label=dataset_enumerations[dataset],
                csv_path=str(csv_path),
                status="ENABLED",
                properties={
                    "tags": [],
                    "properties": {"variables": csv_columns},
                },
            )
            DatasetsTable().insert_values(values, self.db)
            dataset_id += 1

    def _import_datasets(self, csv_path, data_model):
        primary_data_table = PrimaryDataTable.from_db(data_model, self.db)
        table_columns = [col.name for col in primary_data_table.table.columns]

        imported_datasets = []
        with CSVDataFrameReader(csv_path).get_reader() as reader:
            for dataset_data in reader:
                dataframe = DataFrame(dataset_data)
                records = dataframe.to_dict(table_columns)
                if records:
                    primary_data_table.insert_values(records, self.db)
                imported_datasets = list(set(imported_datasets) | set(dataframe.datasets))

        return imported_datasets

    def _get_next_dataset_id(self):
        return DatasetsTable().get_next_dataset_id(self.db)


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
    """Validate CSV files prior to importing them."""

    def __init__(self, duckdb: DuckDB) -> None:
        self.db = duckdb
        is_db_initialized(duckdb)

    def execute(self, csv_path, data_model_code, data_model_version) -> None:
        data_model = get_data_model_fullname(
            code=data_model_code, version=data_model_version
        )

        metadata_table = MetadataTable.from_db(data_model, self.db)
        cdes = metadata_table.table

        dataset_enumerations = get_dataset_enums(cdes)
        if self.is_data_model_longitudinal(data_model_code, data_model_version):
            are_data_valid_longitudinal(csv_path)
        validated_datasets = self._validate_datasets(csv_path, cdes)
        self.verify_datasets_exist_in_enumerations(
            validated_datasets, dataset_enumerations
        )

    def _validate_datasets(self, csv_path, cdes):
        csv_columns = pd.read_csv(csv_path, nrows=0).columns.tolist()
        if DATASET_COLUMN_NAME not in csv_columns:
            raise InvalidDatasetError(
                "The 'dataset' column is required to exist in the csv."
            )

        sql_type_per_column = get_sql_type_per_column(cdes)
        cdes_with_min_max = get_cdes_with_min_max(cdes, csv_columns)
        cdes_with_enumerations = get_cdes_with_enumerations(cdes, csv_columns)
        return self.validate_csv(
            csv_path, sql_type_per_column, cdes_with_min_max, cdes_with_enumerations
        )

    def is_data_model_longitudinal(self, data_model_code, data_model_version):
        data_model_id = DataModelTable().get_data_model_id(
            data_model_code, data_model_version, self.db
        )
        properties = DataModelTable().get_data_model_properties(
            data_model_id, self.db
        )
        return LONGITUDINAL in properties.get("tags", [])

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
                imported_datasets = list(
                    set(imported_datasets) | set(dataframe.datasets)
                )
        return imported_datasets

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
    def __init__(self, duckdb: DuckDB) -> None:
        self.db = duckdb
        is_db_initialized(duckdb)

    def execute(self, dataset_code, data_model_code, data_model_version) -> None:
        data_model_fullname = get_data_model_fullname(
            code=data_model_code, version=data_model_version
        )
        self._remove_dataset(data_model_fullname, dataset_code)
        data_model_id = DataModelTable().get_data_model_id(
            data_model_code, data_model_version, self.db
        )
        dataset_id = DatasetsTable().get_dataset_id(
            dataset_code, data_model_id, self.db
        )
        DatasetsTable().delete_dataset(dataset_id, data_model_id, self.db)

    def _remove_dataset(self, data_model_fullname, dataset_code):
        schema = Schema(data_model_fullname)
        primary_data_table = PrimaryDataTable.from_db(schema, self.db)
        primary_data_table.remove_dataset(dataset_code, self.db)


class EnableDataModel(UseCase):
    def __init__(self, db: DuckDB) -> None:
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
    def __init__(self, db: DuckDB) -> None:
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
    def __init__(self, db: DuckDB) -> None:
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
    def __init__(self, db: DuckDB) -> None:
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
    def __init__(self, db: DuckDB) -> None:
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
    def __init__(self, db: DuckDB) -> None:
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
    def __init__(self, db: DuckDB) -> None:
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
    def __init__(self, db: DuckDB) -> None:
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
    def __init__(self, db: DuckDB) -> None:
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
    def __init__(self, db: DuckDB) -> None:
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
    def __init__(self, db: DuckDB) -> None:
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
    def __init__(self, db: DuckDB) -> None:
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
    def __init__(self, db: DuckDB) -> None:
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
    def __init__(self, db: DuckDB) -> None:
        self.db = db

    def execute(self) -> None:
        dataset_table = DatasetsTable()
        dataset_row_columns = ["dataset_id", "data_model_id", "code", "label", "status"]
        dataset_rows = dataset_table.get_datasets(self.db, columns=dataset_row_columns)
        datasets_info = []

        for row in dataset_rows:
            datasets_info.append(list(row))

        if not datasets_info:
            print("There are no datasets.")
            return

        df = pd.DataFrame(datasets_info, columns=dataset_row_columns)
        print(df)


class Cleanup(UseCase):
    def __init__(self, duckdb: DuckDB) -> None:
        self.db = duckdb

    def execute(self) -> None:
        data_model_table = DataModelTable()
        data_model_rows = data_model_table.get_data_models(
            self.db, columns=["code", "version"]
        )

        for code, version in data_model_rows:
            DeleteDataModel(self.db).execute(
                code=code, version=version, force=True
            )


def get_data_model_fullname(code, version):
    return f"{code}:{version}"
