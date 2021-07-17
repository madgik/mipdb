from unittest.mock import Mock

import pytest
from sqlalchemy.sql.schema import MetaData

from mipdb.schema import Schema
from mipdb.dataelements import CommonDataElement, CategoricalCDE, NumericalCDE
from mipdb.tables import (
    DomainsTable,
    EnumerationsTable,
    UnitsTable,
    VariablesTable,
)
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


def test_variables_table():
    variables_table = VariablesTable(Schema("schema_v1_0"))
    cdes = [CommonDataElement("a_code", "a_label", "", "", "")]
    expected = [{"code": "a_code", "label": "a_label"}]
    result = variables_table.get_values_from_cdes(cdes)
    assert result == expected


def test_enumerations_table():
    enumerations_table = EnumerationsTable(Schema("schema_v1_0"))
    enumerations = [{"code": "a", "label": "Alpha"}, {"code": "b", "label": "Beta"}]
    cdes = [CategoricalCDE("a_code", "a_label", "", "", "", enumerations=enumerations)]
    expected = [
        {"variable_code": "a_code", "code": "a", "label": "Alpha"},
        {"variable_code": "a_code", "code": "b", "label": "Beta"},
    ]
    result = enumerations_table.get_values_from_cdes(cdes)
    assert result == expected


def test_domains_table():
    domains_table = DomainsTable(Schema("schema_v1_0"))
    cdes = [NumericalCDE("a_code", "a_label", "", "", "", -100, 100, "sec")]
    expected = [{"variable_code": "a_code", "min": -100, "max": 100}]
    result = domains_table.get_values_from_cdes(cdes)
    assert result == expected


def test_units_table():
    units_table = UnitsTable(Schema("schema_v1_0"))
    cdes = [NumericalCDE("a_code", "a_label", "", "", "", -100, 100, "sec")]
    expected = [{"variable_code": "a_code", "units": "sec"}]
    result = units_table.get_values_from_cdes(cdes)
    assert result == expected
