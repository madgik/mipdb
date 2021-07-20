from mipdb.exceptions import DataBaseError
import pytest

from mipdb.schema import Schema
from mipdb.tables import (
    ActionsTable,
    SchemasTable,
    DatasetsTable,
    VariablesTable,
)
from mipdb.database import MonetDB, get_db_config
from tests.mocks import MonetDBMock

# NOTE Some of the tables under test below require other tables to have been
# created before so that SQLAlchemy can create the appropriate foreign key
# constraints. Whenever this is the case a comment is added next to the
# required table creation.


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


# def test_json_field():
#     db = MonetDBMock()
#     schema = Schema("schema:1.0")
#     vars_table = VariablesTable(schema)
#     vars_table.create(db)
#     assert "lalala" in db.captured_queries[0]
