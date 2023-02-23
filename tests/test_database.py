from click.testing import CliRunner

from mipdb import add_dataset
from mipdb import add_data_model
from mipdb import init
import pytest

import sqlalchemy as sql

from mipdb.exceptions import DataBaseError
from mipdb.tables import TemporaryTable
from tests.conftest import DATASET_FILE
from tests.conftest import DATA_MODEL_FILE
from tests.conftest import PORT
from tests.mocks import MonetDBMock


def test_create_schema():
    db = MonetDBMock()
    db.create_schema("a_schema")
    assert "CREATE SCHEMA a_schema" in db.captured_queries[0]


def test_get_schemas():
    db = MonetDBMock()
    schemas = db.get_schemas()
    assert schemas == ["mipdb_metadata"]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_update_data_model_status(db):
    # Setup
    runner = CliRunner()
    runner.invoke(init, ["--port", PORT])
    runner.invoke(add_data_model, [DATA_MODEL_FILE, "--port", PORT])
    # Check the status of data model is disabled
    res = db.execute(
        'SELECT status from  "mipdb_metadata".data_models where data_model_id = 1'
    )
    assert list(res)[0][0] == "ENABLED"

    # Test
    db.update_data_model_status("DISABLED", 1)
    res = db.execute(
        'SELECT status from  "mipdb_metadata".data_models where data_model_id = 1'
    )
    assert list(res)[0][0] == "DISABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def update_dataset_status(db):
    # Setup
    runner = CliRunner()
    runner.invoke(init, ["--port", PORT])
    runner.invoke(add_data_model, [DATA_MODEL_FILE, "--port", PORT])
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
            "--port",
            PORT,
        ],
    )

    # Check the status of dataset is disabled
    res = db.execute(
        sql.text('SELECT status from  "mipdb_metadata".datasets where dataset_id = 1')
    )
    assert list(res)[0][0] == "DISABLED"

    # Test
    db.update_dataset_status("ENABLED", 1)
    res = db.execute(
        sql.text('SELECT status from  "mipdb_metadata".datasets where dataset_id = 1')
    )
    assert list(res)[0][0] == "ENABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_schemas_with_db(db):
    # Setup
    runner = CliRunner()
    # Check schema not present already
    assert "data_model:1.0" not in db.get_schemas()

    runner.invoke(init, ["--port", PORT])
    runner.invoke(add_data_model, [DATA_MODEL_FILE, "--port", PORT])

    # Check schema present
    schemas = db.get_schemas()
    assert "data_model:1.0" in db.get_schemas()


def test_get_datasets():
    db = MonetDBMock()
    datasets = db.get_values()
    assert datasets == [[1, 2]]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_datasets_with_db(db):
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    runner.invoke(init, ["--port", PORT])
    runner.invoke(add_data_model, [DATA_MODEL_FILE, "--port", PORT])
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
            "--port",
            PORT,
        ],
    )

    # Check dataset present
    datasets = db.get_values(columns=["code"])
    assert ("dataset",) in datasets
    assert len(datasets) == 1


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_data_model_id_with_db(db):
    # Setup
    runner = CliRunner()
    runner.invoke(init, ["--port", PORT])
    runner.invoke(add_data_model, [DATA_MODEL_FILE, "--port", PORT])

    # Test success
    data_model_id = db.get_data_model_id("data_model", "1.0")
    assert data_model_id == 1


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_data_model_id_not_found_error(db):
    # Setup
    runner = CliRunner()
    runner.invoke(init, ["--port", PORT])

    # Test when there is no schema in the database with the specific code and version
    with pytest.raises(DataBaseError):
        data_model_id = db.get_data_model_id("schema", "1.0")


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_data_model_id_duplication_error(db):
    # Setup
    runner = CliRunner()
    runner.invoke(init, ["--port", PORT])
    runner.invoke(add_data_model, [DATA_MODEL_FILE, "--port", PORT])
    db.execute(
        sql.text(
            'INSERT INTO "mipdb_metadata".data_models (data_model_id, code, version, status)'
            "VALUES (2, 'data_model', '1.0', 'DISABLED')"
        )
    )

    # Test when there more than one schema ids with the specific code and version
    with pytest.raises(DataBaseError):
        db.get_data_model_id("data_model", "1.0")


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_dataset_id_with_db(db):
    # Setup
    runner = CliRunner()
    runner.invoke(init, ["--port", PORT])
    runner.invoke(add_data_model, [DATA_MODEL_FILE, "--port", PORT])
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
            "--port",
            PORT,
        ],
    )

    # Test
    dataset_id = db.get_dataset_id("dataset", 1)
    assert dataset_id == 1


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_dataset_id_duplication_error(db):
    # Setup
    runner = CliRunner()
    runner.invoke(init, ["--port", PORT])
    runner.invoke(add_data_model, [DATA_MODEL_FILE, "--port", PORT])
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
            "--port",
            PORT,
        ],
    )

    db.execute(
        sql.text(
            'INSERT INTO "mipdb_metadata".datasets (dataset_id, data_model_id, code, status)'
            "VALUES (2, 1, 'dataset', 'DISABLED')"
        )
    )

    # Test when there more than one dataset ids with the specific code and data_model_id
    with pytest.raises(DataBaseError):
        dataset_id = db.get_dataset_id("dataset", 1)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_dataset_id_not_found_error(db):
    # Setup
    runner = CliRunner()
    runner.invoke(init, ["--port", PORT])
    runner.invoke(add_data_model, [DATA_MODEL_FILE, "--port", PORT])

    # Test when there is no dataset in the database with the specific code and data_model_id
    with pytest.raises(DataBaseError):
        dataset_id = db.get_dataset_id("dataset", 1)


def test_drop_schema():
    db = MonetDBMock()
    db.drop_schema("a_schema")
    assert 'DROP SCHEMA "a_schema" CASCADE' in db.captured_queries[0]


def test_create_table():
    db = MonetDBMock()
    table = sql.Table("a_table", sql.MetaData(), sql.Column("a_column", sql.Integer))
    db.create_table(table)
    assert "CREATE TABLE a_table" in db.captured_queries[0]


def test_drop_table():
    db = MonetDBMock()
    table = sql.Table("a_table", sql.MetaData(), sql.Column("a_column", sql.Integer))
    db.drop_table(table)
    assert "DROP TABLE a_table" in db.captured_queries[0]


def test_insert_values_to_table():
    db = MonetDBMock()
    table = sql.Table("a_table", sql.MetaData(), sql.Column("a_column", sql.Integer))
    values = [1, 2, 3]
    db.insert_values_to_table(table, values)
    assert "INSERT INTO a_table" in db.captured_queries[0]
    assert values == db.captured_multiparams[0][0]


def test_grant_select_to_executor():
    db = MonetDBMock()
    table = TemporaryTable({"col1": "int", "col2": "int"}, db)
    table.create(db)
    assert "CREATE TEMPORARY " in db.captured_queries[0]
    assert "GRANT SELECT" in db.captured_queries[1]
