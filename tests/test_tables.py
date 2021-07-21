import json

import pytest
import sqlalchemy as sql

from mipdb.exceptions import DataBaseError
from mipdb.schema import Schema
from mipdb.tables import (
    ActionsTable,
    SchemasTable,
    DatasetsTable,
    MetadataTable,
)
from mipdb.dataelements import CommonDataElement, make_cdes
from mipdb.database import MonetDB, get_db_config
from mipdb.constants import METADATA_TABLE, METADATA_SCHEMA
from tests.mocks import MonetDBMock


@pytest.fixture
def metadata():
    return Schema(METADATA_SCHEMA)


def test_schemas_table_mockdb(metadata):
    db = MonetDBMock()
    SchemasTable(schema=metadata).create(db)
    assert f"CREATE SEQUENCE {METADATA_SCHEMA}.schema_id_seq" == db.captured_queries[0]
    expected_create = (
        f"\nCREATE TABLE {METADATA_SCHEMA}.schemas ("
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
    res = db.execute(
        "SELECT name, type FROM sys.columns WHERE "
        "table_id=(SELECT id FROM sys.tables "
        "WHERE name='schemas' AND system=FALSE)"
    )
    assert res.fetchall() != []


def test_datasets_table(metadata):
    db = MonetDBMock()
    DatasetsTable(schema=metadata).create(db)
    assert f"CREATE SEQUENCE {METADATA_SCHEMA}.dataset_id_seq" in db.captured_queries[0]
    expected_create = (
        f"\nCREATE TABLE {METADATA_SCHEMA}.datasets ("
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
    assert f"CREATE SEQUENCE {METADATA_SCHEMA}.action_id_seq" in db.captured_queries[0]
    assert f"CREATE TABLE {METADATA_SCHEMA}.actions" in db.captured_queries[1]


def test_get_schema_id(metadata):
    db = MonetDBMock()
    schemas_table = SchemasTable(schema=metadata)
    with pytest.raises(DataBaseError):
        schemas_table.get_schema_id(code="schema", version="1.0", db=db)
    expected = f"SELECT schemas.schema_id FROM {METADATA_SCHEMA}.schemas"
    assert expected in db.captured_queries[0]


def test_mark_schema_as_deleted(metadata):
    db = MonetDBMock()
    schemas_table = SchemasTable(schema=metadata)
    schemas_table.mark_schema_as_deleted(code="schema", version="1.0", db=db)
    expected = f"UPDATE {METADATA_SCHEMA}.schemas SET status = 'DELETED'"
    assert expected in db.captured_queries[0]


class TestVariablesMetadataTable:
    def test_create_table_mockdb(self):
        db = MonetDBMock()
        metadata_table = MetadataTable(Schema("schema:1.0"))
        metadata_table.create(db)
        assert f'CREATE TABLE "schema:1.0".{METADATA_TABLE}' in db.captured_queries[0]

    @pytest.mark.database
    @pytest.mark.usefixtures("monetdb_container", "cleanup_db")
    def test_create_table_with_db(self, db):
        schema = Schema("schema:1.0")
        schema.create(db)
        metadata_table = MetadataTable(schema)
        metadata_table.create(db)
        res = db.execute(f'SELECT * FROM "schema:1.0".{METADATA_TABLE}').fetchall()
        assert res == []

    @pytest.mark.database
    @pytest.mark.usefixtures("monetdb_container", "cleanup_db")
    def test_insert_values_with_db(self, db, schema_data):
        schema = Schema("schema:1.0")
        schema.create(db)
        metadata_table = MetadataTable(schema)
        metadata_table.create(db)
        values = metadata_table.get_values_from_cdes(make_cdes(schema_data))
        metadata_table.insert_values(values, db)
        res = db.execute(
            "SELECT code, json.filter(metadata, '$.isCategorical') "
            f'FROM "schema:1.0".{METADATA_TABLE}'
        )
        result = [(name, json.loads(val)) for name, val in res.fetchall()]
        assert result == [
            ("var1", [False]),
            ("var2", [True]),
            ("var3", [False]),
            ("var4", [False]),
        ]

    def test_get_values_from_cdes_full_schema_data(self, schema_data):
        metadata_table = MetadataTable(Schema("schema:1.0"))
        cdes = make_cdes(schema_data)
        result = metadata_table.get_values_from_cdes(cdes)
        assert len(result) == 4
