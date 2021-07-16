from mipdb.schema import Schema
from mipdb.tables import SchemasTable
from unittest.mock import Mock
import pytest

import sqlalchemy as sql

from tests.mocks import MonetDBMock


def test_create_schema():
    db = MonetDBMock()
    db.create_schema("a_schema")
    assert "CREATE SCHEMA a_schema" in db.captured_queries[0]


# TODO needs integration test
def test_get_schemas():
    db = MonetDBMock()
    schemas = db.get_schemas()
    assert schemas == []


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
