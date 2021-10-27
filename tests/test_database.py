from click.testing import CliRunner

from mipdb import add_dataset
from mipdb import add_schema
from mipdb import init
import pytest

import sqlalchemy as sql

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
    runner.invoke(
        add_dataset, [dataset_file, "--schema", "schema", "-v", "1.0"]
    )

    # Check dataset present
    datasets = db.get_datasets()
    assert 'a_dataset' in datasets
    assert len(datasets) == 1


def test_drop_schema():
    db = MonetDBMock()
    db.drop_schema("a_schema")
    assert 'DROP SCHEMA "a_schema" CASCADE' in db.captured_queries[0]


def test_create_table():
    db = MonetDBMock()
    table = sql.Table("a_table", sql.MetaData(), sql.Column("a_column", sql.Integer))
    db.create_table(table)
    assert "CREATE TABLE a_table" in db.captured_queries[0]


def test_insert_values_to_table():
    db = MonetDBMock()
    table = sql.Table("a_table", sql.MetaData(), sql.Column("a_column", sql.Integer))
    values = [1, 2, 3]
    db.insert_values_to_table(table, values)
    assert "INSERT INTO a_table" in db.captured_queries[0]
    assert values == db.captured_multiparams[0][0]
