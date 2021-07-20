from mipdb.exceptions import DataBaseError
import pytest

from mipdb.schema import Schema
from mipdb.tables import (
    ActionsTable,
    SchemasTable,
    DatasetsTable,
    VariablesTable,
    NumericVariablesTable,
    EnumerationsTable,
    VariablesTable,
)
from mipdb.dataelements import (
    CommonDataElement,
    CategoricalCDE,
    NumericalCDE,
    make_cdes,
)
from mipdb.database import MonetDB, get_db_config
from tests.mocks import MonetDBMock


@pytest.fixture
def metadata():
    return Schema("mipdb_metadata")


def test_schemas_table_mockdb(metadata):
    db = MonetDBMock()
    SchemasTable(schema=metadata).create(db)
    assert "CREATE SEQUENCE mipdb_metadata.schema_id_seq" == db.captured_queries[0]
    expected_create = (
        "\nCREATE TABLE mipdb_metadata.schemas ("
        "\n\tschema_id INTEGER NOT NULL, "
        "\n\tcode VARCHAR(255) NOT NULL, "
        "\n\tversion VARCHAR(255) NOT NULL, "
        "\n\tlabel VARCHAR(255), "
        "\n\tstatus VARCHAR(255) NOT NULL, "
        "\n\tproperties JSON, "
        "\n\tPRIMARY KEY (schema_id)"
        "\n)\n\n"
    )
    assert expected_create == db.captured_queries[1]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_schemas_table_realdb(db):
    schema = Schema("schema")
    schema.create(db)
    SchemasTable(schema=schema).create(db)
    res = db._execute("SELECT * FROM sys.tables WHERE name='schemas' AND system=FALSE")
    assert res.fetchall() != []


def test_datasets_table(metadata):
    db = MonetDBMock()
    DatasetsTable(schema=metadata).create(db)
    assert "CREATE SEQUENCE mipdb_metadata.dataset_id_seq" in db.captured_queries[0]
    expected_create = (
        "\nCREATE TABLE mipdb_metadata.datasets ("
        "\n\tdataset_id INTEGER NOT NULL, "
        "\n\tschema_id INTEGER NOT NULL, "
        "\n\tversion VARCHAR(255) NOT NULL, "
        "\n\tlabel VARCHAR(255), "
        "\n\tstatus VARCHAR(255) NOT NULL, "
        "\n\tproperties JSON, "
        "\n\tPRIMARY KEY (dataset_id)"
        "\n)\n\n"
    )
    assert expected_create == db.captured_queries[1]


def test_actions_table(metadata):
    db = MonetDBMock()
    ActionsTable(schema=metadata).create(db)
    assert "CREATE SEQUENCE mipdb_metadata.action_id_seq" in db.captured_queries[0]
    assert "CREATE TABLE mipdb_metadata.actions" in db.captured_queries[1]


def test_get_schema_id(metadata):
    db = MonetDBMock()
    schemas_table = SchemasTable(schema=metadata)
    with pytest.raises(DataBaseError):
        schemas_table.get_schema_id(code="schema", version="1.0", db=db)
    expected = "SELECT schemas.schema_id FROM mipdb_metadata.schemas"
    assert expected in db.captured_queries[0]


def test_mark_schema_as_deleted(metadata):
    db = MonetDBMock()
    schemas_table = SchemasTable(schema=metadata)
    schemas_table.mark_schema_as_deleted(code="schema", version="1.0", db=db)
    expected = "UPDATE mipdb_metadata.schemas SET status = 'DELETED'"
    assert expected in db.captured_queries[0]


class TestVariavlesTable:
    def test_get_values_from_cdes(self):
        variables_table = VariablesTable(Schema("schema_v1_0"))
        cdes = [CommonDataElement("a_code", "a_label", "", "", "")]
        expected = [{"code": "a_code", "label": "a_label"}]
        result = variables_table.get_values_from_cdes(cdes)
        assert result == expected


class TestEnumerationsTable:
    def test_get_values_from_cdes(self):
        enumerations_table = EnumerationsTable(Schema("schema_v1_0"))
        enumerations = [{"code": "a", "label": "Alpha"}, {"code": "b", "label": "Beta"}]
        cdes = [
            CategoricalCDE("a_code", "a_label", "", "", "", enumerations=enumerations)
        ]
        expected = [
            {"variable_code": "a_code", "code": "a", "label": "Alpha"},
            {"variable_code": "a_code", "code": "b", "label": "Beta"},
        ]
        result = enumerations_table.get_values_from_cdes(cdes)
        assert result == expected


class TestNumericTable:
    def test_get_values_from_cdes(self):
        numeric_table = NumericVariablesTable(Schema("schema_v1_0"))
        cdes = [NumericalCDE("a_code", "a_label", "", "", "", -100, 100, "sec")]
        expected = [
            {"variable_code": "a_code", "min": -100, "max": 100, "units": "sec"}
        ]
        result = numeric_table.get_values_from_cdes(cdes)
        assert result == expected

    def test_get_values_from_cdes_full_schema_data(self, schema_data):
        numeric_table = NumericVariablesTable(Schema("schema_v1_0"))
        cdes = make_cdes(schema_data)
        result = numeric_table.get_values_from_cdes(cdes)
        expected = [
            {"variable_code": "var3", "min": 0.0, "max": 100.0, "units": None},
            {"variable_code": "var4", "min": None, "max": None, "units": "years"},
        ]
        assert result == expected
