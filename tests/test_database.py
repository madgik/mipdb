from click.testing import CliRunner

from mipdb import add_dataset
from mipdb import add_schema
from mipdb import init
import pytest

import sqlalchemy as sql

from mipdb.exceptions import DataBaseError
from tests.mocks import MonetDBMock


def test_create_schema():
    db = MonetDBMock()
    db.create_schema("a_schema")
    assert "CREATE SCHEMA a_schema" in db.captured_queries[0]


def test_get_schemas():
    db = MonetDBMock()
    schemas = db.get_schemas()
    assert schemas == []


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def update_schema_status(db):
    # Setup
    runner = CliRunner()
    schema_file = "tests/data/schema.json"
    runner.invoke(init, [])
    runner.invoke(add_schema, [schema_file, "-v", "1.0"])
    # Check the status of schema is disabled
    res = db.execute(
        sql.text('SELECT status from  "mipdb_metadata".schemas where schema_id = 1')
    )
    assert list(res)[0] == "DISABLED"

    # Test
    db.update_metadata_schema_status("ENABLED", "schema", 1)
    res = db.execute(
        sql.text('SELECT status from  "mipdb_metadata".schemas where schema_id = 1')
    )
    assert list(res)[0] == "ENABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def update_dataset_status(db):
    # Setup
    runner = CliRunner()
    schema_file = "tests/data/schema.json"
    dataset_file = "tests/data/dataset.csv"
    runner.invoke(init, [])
    runner.invoke(add_schema, [schema_file, "-v", "1.0"])
    runner.invoke(add_dataset, [dataset_file, "--schema", "schema", "-v", "1.0"])

    # Check the status of dataset is disabled
    res = db.execute(
        sql.text('SELECT status from  "mipdb_metadata".datasets where dataset_id = 1')
    )
    assert list(res)[0] == "DISABLED"

    # Test
    db.update_metadata_schema_status("ENABLED", "dataset", 1)
    res = db.execute(
        sql.text('SELECT status from  "mipdb_metadata".datasets where dataset_id = 1')
    )
    assert list(res)[0] == "ENABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_schemas(db):
    # Setup
    runner = CliRunner()
    schema_file = "tests/data/schema.json"
    # Check schema not present already
    assert db.get_schemas() == []

    runner.invoke(init, [])
    runner.invoke(add_schema, [schema_file, "-v", "1.0"])

    # Check schema present
    schemas = db.get_schemas()
    assert len(schemas) == 2


def test_get_datasets():
    db = MonetDBMock()
    datasets = db.get_datasets()
    assert datasets == []


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_datasets(db):
    # Setup
    runner = CliRunner()
    schema_file = "tests/data/schema.json"
    dataset_file = "tests/data/dataset.csv"

    # Check dataset not present already
    runner.invoke(init, [])
    runner.invoke(add_schema, [schema_file, "-v", "1.0"])
    runner.invoke(add_dataset, [dataset_file, "--schema", "schema", "-v", "1.0"])

    # Check dataset present
    datasets = db.get_datasets()
    assert "a_dataset" in datasets
    assert len(datasets) == 1


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_schema_id(db):
    # Setup
    runner = CliRunner()
    schema_file = "tests/data/schema.json"
    runner.invoke(init, [])
    runner.invoke(add_schema, [schema_file, "-v", "1.0"])

    # Test success
    schema_id = db.get_schema_id("schema", "1.0")
    assert schema_id == 1


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_schema_id_not_found_error(db):
    # Setup
    runner = CliRunner()
    schema_file = "tests/data/schema.json"
    runner.invoke(init, [])

    # Test when there is no schema in the database with the specific code and version
    with pytest.raises(DataBaseError):
        schema_id = db.get_schema_id("schema", "1.0")


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_schema_id_duplication_error(db):
    # Setup
    runner = CliRunner()
    schema_file = "tests/data/schema.json"
    runner.invoke(init, [])
    runner.invoke(add_schema, [schema_file, "-v", "1.0"])
    db.execute(
        sql.text(
            'INSERT INTO "mipdb_metadata".schemas (schema_id, code, version, status)'
            "VALUES (2, 'schema', '1.0', 'DISABLED')"
        )
    )

    # Test when there more than one schema ids with the specific code and version
    with pytest.raises(DataBaseError):
        db.get_schema_id("schema", "1.0")


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_dataset_id(db):
    # Setup
    runner = CliRunner()
    schema_file = "tests/data/schema.json"
    dataset_file = "tests/data/dataset.csv"
    runner.invoke(init, [])
    runner.invoke(add_schema, [schema_file, "-v", "1.0"])
    runner.invoke(add_dataset, [dataset_file, "--schema", "schema", "-v", "1.0"])

    # Test
    dataset_id = db.get_dataset_id("a_dataset", 1)
    assert dataset_id == 1


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_dataset_id_duplication_error(db):
    # Setup
    runner = CliRunner()
    schema_file = "tests/data/schema.json"
    dataset_file = "tests/data/dataset.csv"
    runner.invoke(init, [])
    runner.invoke(add_schema, [schema_file, "-v", "1.0"])
    runner.invoke(add_dataset, [dataset_file, "--schema", "schema", "-v", "1.0"])

    db.execute(
        sql.text(
            'INSERT INTO "mipdb_metadata".datasets (dataset_id, schema_id, code, status)'
            "VALUES (2, 1, 'a_dataset', 'DISABLED')"
        )
    )

    # Test when there more than one dataset ids with the specific code and schema_id
    with pytest.raises(DataBaseError):
        dataset_id = db.get_dataset_id("a_dataset", 1)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_get_dataset_id_not_found_error(db):
    # Setup
    runner = CliRunner()
    schema_file = "tests/data/schema.json"
    dataset_file = "tests/data/dataset.csv"
    runner.invoke(init, [])
    runner.invoke(add_schema, [schema_file, "-v", "1.0"])

    # Test when there is no dataset in the database with the specific code and schema_id
    with pytest.raises(DataBaseError):
        dataset_id = db.get_dataset_id("a_dataset", 1)


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
