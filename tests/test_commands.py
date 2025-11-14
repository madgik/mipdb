import json

from click.testing import CliRunner
import pytest

from mipdb.commands import entry as cli
from mipdb.duckdb.database import DataModel, Dataset
from mipdb.duckdb.metadata_tables import DataModelTable
from mipdb.duckdb.schema import Schema
from tests.conftest import DATA_MODEL_FILE, DATASET_FILE, SUCCESS_DATA_FOLDER


@pytest.fixture
def cli_runner(duckdb_option):
    def _run(command_name, *args):
        runner = CliRunner()
        result = runner.invoke(cli, [*duckdb_option, command_name, *args])
        if result.exit_code != 0:
            print(result.output)
        return result

    return _run


@pytest.fixture
def bootstrap_data_model(cli_runner):
    def _inner():
        assert cli_runner("init").exit_code == 0
        assert cli_runner("add-data-model", DATA_MODEL_FILE).exit_code == 0

    return _inner


@pytest.mark.database
def test_init_creates_tables(duckdb, cli_runner):
    data_model_table = DataModelTable()
    assert not data_model_table.exists(duckdb)

    result = cli_runner("init")
    assert result.exit_code == 0
    assert data_model_table.exists(duckdb)


@pytest.mark.database
def test_add_data_model_registers_metadata(duckdb, bootstrap_data_model):
    bootstrap_data_model()

    rows = duckdb.execute_fetchall("SELECT * FROM data_models")
    assert len(rows) == 1
    data_model_id, code, version, label, status, properties = rows[0]
    assert (data_model_id, code, version, label, status) == (
        1,
        "data_model",
        "1.0",
        "The Data Model",
        "ENABLED",
    )
    if isinstance(properties, str):
        props = json.loads(properties)
    else:
        props = properties
    assert "cdes" in props["properties"]

    metadata_rows = duckdb.execute_fetchall(
        'SELECT * FROM "data_model:1.0_variables_metadata"'
    )
    assert len(metadata_rows) == 6


@pytest.mark.database
def test_add_dataset_persists_rows(duckdb, bootstrap_data_model, cli_runner):
    bootstrap_data_model()

    result = cli_runner(
        "add-dataset",
        DATASET_FILE,
        "--data-model",
        "data_model",
        "-v",
        "1.0",
    )
    assert result.exit_code == 0

    datasets = duckdb.execute_fetchall("SELECT code FROM datasets")
    assert datasets == [("dataset",)]

    schema = Schema("data_model:1.0")
    primary_rows = duckdb.execute_fetchall(
        f'SELECT * FROM "{schema.db_name}__primary_data"'
    )
    assert len(primary_rows) == 5


@pytest.mark.database
def test_load_folder_imports_everything(duckdb, cli_runner):
    assert cli_runner("init").exit_code == 0
    result = cli_runner("load-folder", SUCCESS_DATA_FOLDER)
    assert result.exit_code == 0

    data_models = duckdb.execute_fetchall("SELECT code, version FROM data_models")
    assert data_models

    datasets = duckdb.execute_fetchall("SELECT code FROM datasets")
    assert datasets
