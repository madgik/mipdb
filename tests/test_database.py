from click.testing import CliRunner

from mipdb import add_dataset
from mipdb import add_data_model
from mipdb import init
import pytest

from mipdb.exceptions import DataBaseError
from mipdb.sqlite import DataModel, Dataset
from tests.conftest import DATASET_FILE, MONETDB_OPTIONS, SQLiteDB_OPTION
from tests.conftest import DATA_MODEL_FILE


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_update_data_model_status(sqlite_db):
    # Setup
    runner = CliRunner()
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    # Check the status of data model is disabled
    result = sqlite_db.get_values(
        table=DataModel.__table__,
        columns=["status"],
        where_conditions={"data_model_id": 1},
    )
    assert result[0][0] == "ENABLED"

    # Test
    sqlite_db.update_data_model_status("DISABLED", 1)
    result = sqlite_db.get_values(
        table=DataModel.__table__,
        columns=["status"],
        where_conditions={"data_model_id": 1},
    )
    assert result[0][0] == "DISABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def update_dataset_status(sqlite_db):
    # Setup
    runner = CliRunner()
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    runner.invoke(
        add_dataset,
        [
            DATASET_FILE,
            "-d",
            "data_model",
            "-v",
            "1.0",
            "--copy_from_file",
            False,
        ]
        + SQLiteDB_OPTION
        + MONETDB_OPTIONS,
    )

    # Check the status of dataset is disabled

    result = sqlite_db.get_values(
        table=Dataset.__table__,
        columns=["status"],
        where_conditions={"data_model_id": 1},
    )
    assert result[0][0] == "DISABLED"

    # Test
    sqlite_db.update_dataset_status("ENABLED", 1)
    result = sqlite_db.get_values(
        table=Dataset.__table__,
        columns=["status"],
        where_conditions={"data_model_id": 1},
    )
    assert result[0][0] == "ENABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_datasets_with_db(sqlite_db):
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    runner.invoke(
        add_dataset,
        [
            DATASET_FILE,
            "-d",
            "data_model",
            "-v",
            "1.0",
            "--copy_from_file",
            False,
        ]
        + SQLiteDB_OPTION
        + MONETDB_OPTIONS,
    )

    # Check dataset present
    datasets = sqlite_db.get_values(Dataset.__table__, columns=["code"])
    assert ("dataset",) == datasets[0]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_data_model_id_with_db(sqlite_db):
    # Setup
    runner = CliRunner()
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)

    # Test success
    data_model_id = sqlite_db.get_data_model_id("data_model", "1.0")
    assert data_model_id == 1


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_data_model_id_not_found_error(sqlite_db):
    # Setup
    runner = CliRunner()
    runner.invoke(init, SQLiteDB_OPTION)

    # Test when there is no schema in the database with the specific code and version
    with pytest.raises(DataBaseError):
        data_model_id = sqlite_db.get_data_model_id("schema", "1.0")


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_data_model_id_duplication_error(sqlite_db):
    # Setup
    runner = CliRunner()
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    sqlite_db.insert_values_to_table(
        DataModel.__table__,
        {
            "data_model_id": 2,
            "label": "data_model",
            "code": "data_model",
            "version": "1.0",
            "status": "DISABLED",
        },
    )

    # Test when there more than one schema ids with the specific code and version
    with pytest.raises(DataBaseError):
        sqlite_db.get_data_model_id("data_model", "1.0")


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_dataset_id_with_db(sqlite_db):
    # Setup
    runner = CliRunner()
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    runner.invoke(
        add_dataset,
        [
            DATASET_FILE,
            "-d",
            "data_model",
            "-v",
            "1.0",
            "--copy_from_file",
            False,
        ]
        + SQLiteDB_OPTION
        + MONETDB_OPTIONS,
    )

    # Test
    dataset_id = sqlite_db.get_dataset_id("dataset", 1)
    assert dataset_id == 1


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_dataset_id_duplication_error(sqlite_db):
    # Setup
    runner = CliRunner()
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    runner.invoke(
        add_dataset,
        [
            DATASET_FILE,
            "-d",
            "data_model",
            "-v",
            "1.0",
            "--copy_from_file",
            False,
        ]
        + SQLiteDB_OPTION
        + MONETDB_OPTIONS,
    )

    sqlite_db.insert_values_to_table(
        Dataset.__table__,
        {
            "dataset_id": 2,
            "data_model_id": 1,
            "label": "dataset",
            "code": "dataset",
            "csv_path": "/opt/data/data_model/dataset.csv",
            "status": "DISABLED",
        },
    )

    # Test when there more than one dataset ids with the specific code and data_model_id
    with pytest.raises(DataBaseError):
        dataset_id = sqlite_db.get_dataset_id("dataset", 1)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_dataset_id_not_found_error(sqlite_db):
    # Setup
    runner = CliRunner()
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)

    # Test when there is no dataset in the database with the specific code and data_model_id
    with pytest.raises(DataBaseError):
        dataset_id = sqlite_db.get_dataset_id("dataset", 1)
