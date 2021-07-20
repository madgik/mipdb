from mipdb.exceptions import DataBaseError
import pytest
from mipdb.usecases import (
    AddSchema,
    DeleteSchema,
    InitDB,
    update_actions_on_schema_addition,
    update_actions_on_schema_deletion,
    update_schemas_on_schema_addition,
    update_schemas_on_schema_deletion,
)
from tests.mocks import MonetDBMock


# NOTE Some use cases have a main responsibility (e.g. add a new schema) which
# is followed by some additional actions (e.g. updating the schemas and actions
# table).  These additional actions are implemented as handlers using an event
# system. The use case tests below verify that the main queries are correct and
# that more queries have been issued by the handlers. Separate tests verify
# that the correct queries have been issued by the handlers.


def test_init_mock():
    db = MonetDBMock()
    InitDB(db).execute()
    assert "CREATE SCHEMA mipdb_metadata" in db.captured_queries[0]
    assert "CREATE TABLE mipdb_metadata.schemas" in db.captured_queries[2]
    assert "CREATE TABLE mipdb_metadata.datasets" in db.captured_queries[4]
    assert "CREATE TABLE mipdb_metadata.actions" in db.captured_queries[6]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_init_with_db(db):
    InitDB(db).execute()
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas


def test_add_schema_mock(schema_data):
    db = MonetDBMock()
    AddSchema(db).execute(schema_data)
    assert 'CREATE SCHEMA "schema:1.0"' in db.captured_queries[1]
    assert 'CREATE TABLE "schema:1.0".variables' in db.captured_queries[2]
    assert 'INSERT INTO "schema:1.0".variables' in db.captured_queries[3]
    assert 'CREATE TABLE "schema:1.0".enumerations' in db.captured_queries[4]
    assert 'INSERT INTO "schema:1.0".enumerations' in db.captured_queries[5]
    assert 'CREATE TABLE "schema:1.0".domains' in db.captured_queries[6]
    assert 'INSERT INTO "schema:1.0".domains' in db.captured_queries[7]
    assert 'CREATE TABLE "schema:1.0".units' in db.captured_queries[8]
    assert 'INSERT INTO "schema:1.0".units' in db.captured_queries[9]
    assert 'CREATE TABLE "schema:1.0".primary_data' in db.captured_queries[10]
    assert len(db.captured_queries) > 10  # verify that handlers issued more queries


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_schema_with_db(db, schema_data):
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "schema:1.0" in schemas


def test_update_schemas_on_schema_addition():
    db = MonetDBMock()
    record = {"code": "code", "version": "1.0", "label": "Label"}
    update_schemas_on_schema_addition(record, db)
    assert "INSERT INTO mipdb_metadata.schemas" in db.captured_queries[0]
    schemas_record = db.captured_multiparams[0][0]
    assert schemas_record["status"] == "DISABLED"


def test_update_actions_on_schema_addition():
    db = MonetDBMock()
    record = {"code": "code", "version": "1.0", "schema_id": 1}
    update_actions_on_schema_addition(record, db)
    assert "INSERT INTO mipdb_metadata.actions" in db.captured_queries[0]
    actions_record = db.captured_multiparams[0][0]
    assert set(record.values()) <= set(actions_record.values())


def test_delete_schema():
    db = MonetDBMock()
    code = "schema"
    version = "1.0"
    with pytest.raises(DataBaseError):
        DeleteSchema(db).execute(code, version)
    assert 'DROP SCHEMA "schema:1.0" CASCADE' in db.captured_queries[0]
    expected_select_id = "SELECT schemas.schema_id FROM mipdb_metadata.schemas"
    assert expected_select_id in db.captured_queries[1]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_schema_with_db(db, schema_data):
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "schema:1.0" in schemas
    DeleteSchema(db).execute(code=schema_data["code"], version=schema_data["version"])
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "schema:1.0" not in schemas


def test_update_schemas_on_schema_deletion():
    db = MonetDBMock()
    record = {"code": "code", "version": "1.0"}
    update_schemas_on_schema_deletion(record, db)
    expected = "UPDATE mipdb_metadata.schemas SET status = 'DELETED'"
    assert expected in db.captured_queries[0]
    assert db.captured_params[0] == record


def test_update_actions_on_schema_deletion():
    db = MonetDBMock()
    record = {"code": "code", "version": "1.0", "schema_id": 1}
    update_actions_on_schema_deletion(record, db)
    assert "INSERT INTO mipdb_metadata.actions" in db.captured_queries[0]
    actions_record = db.captured_multiparams[0][0]
    assert set(record.values()) <= set(actions_record.values())
