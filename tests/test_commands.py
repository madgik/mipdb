import json

import pytest
from click.testing import CliRunner

from mipdb import init, add_schema, delete_schema, add_dataset, delete_dataset
from mipdb.exceptions import ExitCode


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_init(db):
    # Setup
    runner = CliRunner()
    # Check schema not present already
    assert "mipdb_metadata" not in db.get_schemas()
    # Test
    result = runner.invoke(init, [])
    assert result.exit_code == ExitCode.OK
    assert "mipdb_metadata" in db.get_schemas()
    assert db.execute(f"select * from mipdb_metadata.schemas").fetchall() == []
    assert db.execute(f"select * from mipdb_metadata.actions").fetchall() == []


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_schema(db):
    # Setup
    runner = CliRunner()
    schema_file = "tests/data/schema.json"
    # Check schema not present already
    assert "schema:1.0" not in db.get_schemas()
    result = runner.invoke(init, [])
    # Test
    result = runner.invoke(add_schema, [schema_file, "-v", "1.0"])
    assert result.exit_code == ExitCode.OK
    assert "schema:1.0" in db.get_schemas()
    schemas = db.execute(f"select * from mipdb_metadata.schemas").fetchall()
    assert schemas == [(1, "schema", "1.0", "The Schema", "DISABLED", None)]
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()
    action_id, action = action_record[0]
    assert action_id == 1
    assert action != ""
    assert json.loads(action)["action"] == "ADD SCHEMA"
    metadata = db.execute(f'select * from "schema:1.0".variables_metadata').fetchall()
    # TODO better test
    assert len(metadata) == 5


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_schema(db):
    # Setup
    runner = CliRunner()
    schema_file = "tests/data/schema.json"
    # Check schema not present already
    assert "schema:1.0" not in db.get_schemas()
    result = runner.invoke(init, [])
    result = runner.invoke(add_schema, [schema_file, "-v", "1.0"])
    # Test
    result = runner.invoke(delete_schema, ["schema", "-v", "1.0", "-f"])
    assert result.exit_code == ExitCode.OK
    assert "schema:1.0" not in db.get_schemas()
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()
    action_id, action = action_record[1]
    assert action_id == 2
    assert action != ""
    assert json.loads(action)["action"] == "DELETE SCHEMA"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_dataset(db):
    # Setup
    runner = CliRunner()
    schema_file = "tests/data/schema.json"
    dataset_file = "tests/data/dataset.csv"

    # Check dataset not present already
    result = runner.invoke(init, [])
    result = runner.invoke(add_schema, [schema_file, "-v", "1.0"])
    assert "a_dataset" not in db.get_datasets()

    # Test
    result = runner.invoke(
        add_dataset, [dataset_file, "--schema", "schema", "-v", "1.0"]
    )
    assert "a_dataset" in db.get_datasets()
    assert result.exit_code == ExitCode.OK
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()
    action_id, action = action_record[1]
    assert action_id == 2
    assert action != ""
    assert json.loads(action)["action"] == "ADD DATASET"
    data = db.execute(f'select * from "schema:1.0".primary_data').fetchall()
    assert len(data) == 5


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_dataset(db):
    # Setup
    runner = CliRunner()
    schema_file = "tests/data/schema.json"
    dataset_file = "tests/data/dataset.csv"

    # Check dataset not present already
    runner.invoke(init, [])
    runner.invoke(add_schema, [schema_file, "-v", "1.0"])
    runner.invoke(add_dataset, [dataset_file, "--schema", "schema", "-v", "1.0"])
    assert "a_dataset" in db.get_datasets()

    # Test
    result = runner.invoke(
        delete_dataset, ["a_dataset", "--schema", "schema", "-v", "1.0"]
    )
    assert "a_dataset" not in db.get_datasets()
    assert result.exit_code == ExitCode.OK
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()
    action_id, action = action_record[2]
    assert action_id == 3
    assert action != ""
    assert json.loads(action)["action"] == "DELETE DATASET"
