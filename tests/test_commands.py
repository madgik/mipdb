import pytest
from click.testing import CliRunner

from mipdb import init, add_schema, delete_schema
from mipdb.commands import add_dataset
from mipdb.exceptions import UserInputError, ExitCode
from mipdb.constants import METADATA_TABLE, METADATA_SCHEMA


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_init(db):
    # Setup
    runner = CliRunner()
    # Check schema not present already
    assert METADATA_SCHEMA not in db.get_schemas()
    # Test
    result = runner.invoke(init, [])
    assert result.exit_code == ExitCode.OK
    assert METADATA_SCHEMA in db.get_schemas()
    assert db.execute(f"select * from {METADATA_SCHEMA}.schemas").fetchall() == []
    assert db.execute(f"select * from {METADATA_SCHEMA}.logs").fetchall() == []


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
    schemas = db.execute(f"select * from {METADATA_SCHEMA}.schemas").fetchall()
    assert schemas == [(1, "schema", "1.0", "The Schema", "DISABLED", None)]
    log_record = db.execute(f"select * from {METADATA_SCHEMA}.logs").fetchall()
    log_id, log = log_record[0]
    assert log_id == 1
    assert log != ""
    metadata = db.execute(f'select * from "schema:1.0".{METADATA_TABLE}').fetchall()
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
    result = runner.invoke(delete_schema, ["schema", "-v", "1.0"])
    assert result.exit_code == ExitCode.OK
    assert "schema:1.0" not in db.get_schemas()
    log_record = db.execute(f"select * from {METADATA_SCHEMA}.logs").fetchall()
    log_id, log = log_record[1]
    assert log_id == 2
    assert log != ""


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_dataset(db):
    # Setup
    runner = CliRunner()
    schema_file = "tests/data/schema.json"
    dataset_file = "tests/data/dataset.csv"
    # TODO Check dataset not present
    result = runner.invoke(init, [])
    result = runner.invoke(add_schema, [schema_file, "-v", "1.0"])
    # Test
    result = runner.invoke(
        add_dataset, [dataset_file, "--schema", "schema", "-v", "1.0"]
    )
    assert result.exit_code == ExitCode.OK
    res = db.execute(
        f"SELECT * FROM \"schema:1.0\".primary_data WHERE dataset='a_dataset'"
    ).fetchall()
    assert res != []
