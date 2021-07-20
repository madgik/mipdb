import pytest
from click.testing import CliRunner

from mipdb import init, add_schema, delete_schema
from mipdb.commands import ExitCode
from mipdb.exceptions import UserInputError


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_init(db):
    runner = CliRunner()
    assert "mipdb_metadata" not in db.get_schemas()
    result = runner.invoke(init, [])
    assert result.exit_code == ExitCode.OK
    assert "mipdb_metadata" in db.get_schemas()
    assert db._execute("select * from mipdb_metadata.schemas").fetchall() == []
    assert db._execute("select * from mipdb_metadata.actions").fetchall() == []


# TODO remove explicit fetchall calls here, move to MonetDB._execute
@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_schema(db):
    runner = CliRunner()
    schema_file = "tests/data/schema.json"
    # Check schema not present
    assert "schema:1.0" not in db.get_schemas()
    # Need to call init first to create mipdb_metadata
    result = runner.invoke(init, [])
    # Test add schema
    result = runner.invoke(add_schema, [schema_file, "-v", "1.0"])
    assert result.exit_code == ExitCode.OK
    assert "schema:1.0" in db.get_schemas()
    schemas = db._execute("select * from mipdb_metadata.schemas").fetchall()
    assert schemas == [(1, "schema", "1.0", "The Schema", "DISABLED", None)]
    actions = db._execute("select * from mipdb_metadata.actions").fetchall()
    assert actions == [
        (
            1,
            "ADD SCHEMA WITH id=1, code=schema, version=1.0",
            "TO BE DETERMINED",
            "TO BE DETERMINED",
        )
    ]
    variables = db._execute('select * from "schema:1.0".variables').fetchall()
    assert variables == [
        ("var1", "Variable 1"),
        ("var2", "Variable 2"),
        ("var3", "Variable 3"),
        ("var4", "Variable 4"),
    ]
    enums = db._execute('select * from "schema:1.0".enumerations').fetchall()
    assert enums == [("l1", "var2", "Level1"), ("l2", "var2", "Level2")]
    domains = db._execute('select * from "schema:1.0".domains').fetchall()
    assert domains == [("var3", 0.0, 100.0)]
    units = db._execute('select * from "schema:1.0".units').fetchall()
    assert units == [("var4", "years")]


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
    actions = db._execute("select * from mipdb_metadata.actions").fetchall()
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
