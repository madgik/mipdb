from unittest.mock import Mock

import pytest
from sqlalchemy.sql.schema import MetaData

from mipdb.schema import Schema
from mipdb.database import DataBase


@pytest.fixture
def mockdb():
    return Mock(spec_set=DataBase)


def test_create_schema(mockdb):
    schema = Schema("schema_v1_0")
    schema.create(mockdb)
    mockdb.create_schema.assert_called_with(schema.name)


def test_drop_schema(mockdb):
    schema = Schema("schema_v1_0")
    schema.drop(mockdb)
    mockdb.drop_schema.assert_called_with(schema.name)
