from mipdb.exceptions import DataBaseError
import pytest

from mipdb.schema import Schema
from mipdb.tables import (
    ActionsTable,
    SchemasTable,
    DatasetsTable,
    PropertiesTable,
)
from tests.mocks import MonetDBMock

# NOTE Some of the tables under test below require other tables to have been
# created before so that SQLAlchemy can create the appropriate foreign key
# constraints. Whenever this is the case a comment is added next to the
# required table creation.


@pytest.fixture
def metadata():
    return Schema("mipdb_metadata")


def test_schemas_table(metadata):
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
        "\n\tPRIMARY KEY (schema_id)"
        "\n)\n\n"
    )
    assert expected_create == db.captured_queries[1]


def test_datasets_table(metadata):
    db = MonetDBMock()
    SchemasTable(schema=metadata)  # Required by SQLAlchemy for FK constraints
    DatasetsTable(schema=metadata).create(db)
    assert "CREATE SEQUENCE mipdb_metadata.dataset_id_seq" in db.captured_queries[0]
    assert "CREATE TABLE mipdb_metadata.datasets" in db.captured_queries[1]


def test_properties_table(metadata):
    db = MonetDBMock()
    SchemasTable(schema=metadata)  # Required by SQLAlchemy for FK constraints
    DatasetsTable(schema=metadata)  # Required by SQLAlchemy for FK constraints
    PropertiesTable(schema=metadata).create(db)
    assert "CREATE SEQUENCE mipdb_metadata.property_id_seq" in db.captured_queries[0]
    assert "CREATE TABLE mipdb_metadata.properties" in db.captured_queries[1]


def test_actions_table(metadata):
    db = MonetDBMock()
    SchemasTable(schema=metadata)  # Required by SQLAlchemy for FK constraints
    DatasetsTable(schema=metadata)  # Required by SQLAlchemy for FK constraints
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
