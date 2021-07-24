import pytest
from click.testing import CliRunner

from mipdb import init, add_schema, delete_schema
from mipdb.commands import add_dataset
from mipdb.exceptions import UserInputError, ExitCode
from mipdb.constants import METADATA_TABLE, METADATA_SCHEMA


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_init(db):
    runner = CliRunner()
    assert METADATA_SCHEMA not in db.get_schemas()
    result = runner.invoke(init, [])
    assert result.exit_code == ExitCode.OK
    assert METADATA_SCHEMA in db.get_schemas()
    assert db.execute(f"select * from {METADATA_SCHEMA}.schemas").fetchall() == []
    assert db.execute(f"select * from {METADATA_SCHEMA}.actions").fetchall() == []


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_schema(db):
    runner = CliRunner()
    schema_file = "tests/data/schema.json"
    # Check schema not present
    assert "schema:1.0" not in db.get_schemas()
    # Need to call init first to create METADATA_SCHEMA
    result = runner.invoke(init, [])
    # Test add schema
    result = runner.invoke(add_schema, [schema_file, "-v", "1.0"])
    assert result.exit_code == ExitCode.OK
    assert "schema:1.0" in db.get_schemas()
    schemas = db.execute(f"select * from {METADATA_SCHEMA}.schemas").fetchall()
    assert schemas == [(1, "schema", "1.0", "The Schema", "DISABLED", None)]
    actions = db.execute(f"select * from {METADATA_SCHEMA}.actions").fetchall()
    assert actions == [
        (
            1,
            "ADD SCHEMA WITH id=1, code=schema, version=1.0",
            "TO BE DETERMINED",
            "TO BE DETERMINED",
        )
    ]
    metadata = db.execute(f'select * from "schema:1.0".{METADATA_TABLE}').fetchall()
    # TODO better test
    assert len(metadata) == 5


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_schema(db):
    runner = CliRunner()
    schema_file = "tests/data/schema.json"
    # Check schema not present
    assert "schema:1.0" not in db.get_schemas()
    # Need to call init and add_schema first
    result = runner.invoke(init, [])
    result = runner.invoke(add_schema, [schema_file, "-v", "1.0"])
    # Test delete schema
    result = runner.invoke(delete_schema, ["schema", "-v", "1.0"])
    assert result.exit_code == ExitCode.OK
    assert "schema:1.0" not in db.get_schemas()
    actions = db.execute(f"select * from {METADATA_SCHEMA}.actions").fetchall()
    assert actions == [
        (
            1,
            "ADD SCHEMA WITH id=1, code=schema, version=1.0",
            "TO BE DETERMINED",
            "TO BE DETERMINED",
        ),
        (
            2,
            "DELETE SCHEMA WITH id=1, code=schema, version=1.0",
            "TO BE DETERMINED",
            "TO BE DETERMINED",
        ),
    ]


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
