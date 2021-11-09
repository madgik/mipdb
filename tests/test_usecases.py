import ast

import pandas as pd
import pytest

from mipdb.exceptions import AccessError
from mipdb.exceptions import UserInputError
from mipdb.usecases import (
    AddSchema,
    AddDataset,
    DeleteSchema,
    DeleteDataset,
    InitDB,
    update_actions_on_schema_addition,
    update_actions_on_schema_deletion,
    update_schemas_on_schema_addition,
    update_schemas_on_schema_deletion,
    update_actions_on_dataset_addition,
    update_actions_on_dataset_deletion,
    update_datasets_on_dataset_addition,
    update_datasets_on_dataset_deletion,
)
from mipdb.usecases import TagDataset
from mipdb.usecases import TagSchema
from mipdb.usecases import update_actions_on_dataset_tagging
from mipdb.usecases import update_actions_on_schema_tagging
from mipdb.usecases import update_datasets_on_schema_deletion
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
    assert f"CREATE SCHEMA mipdb_metadata" in db.captured_queries[0]
    assert f"CREATE TABLE mipdb_metadata.schemas" in db.captured_queries[2]
    assert f"CREATE TABLE mipdb_metadata.datasets" in db.captured_queries[4]
    assert f"CREATE TABLE mipdb_metadata.actions" in db.captured_queries[6]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_init_with_db(db):
    # Setup
    InitDB(db).execute()

    # Test
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas


def test_add_schema_mock(schema_data):
    db = MonetDBMock()
    AddSchema(db).execute(schema_data)
    assert 'CREATE SCHEMA "schema:1.0"' in db.captured_queries[1]
    assert 'CREATE TABLE "schema:1.0".primary_data' in db.captured_queries[2]
    assert f'CREATE TABLE "schema:1.0".variables_metadata' in db.captured_queries[3]
    assert f'INSERT INTO "schema:1.0".variables_metadata' in db.captured_queries[4]
    assert len(db.captured_queries) > 5  # verify that handlers issued more queries


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_schema_with_db(db, schema_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)

    # Test
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "schema:1.0" in schemas


def test_update_schemas_on_schema_addition():
    db = MonetDBMock()
    record = {"code": "code", "version": "1.0", "label": "Label"}
    update_schemas_on_schema_addition(record, db)
    assert f"INSERT INTO mipdb_metadata.schemas" in db.captured_queries[0]
    schemas_record = db.captured_multiparams[0][0]
    assert schemas_record["status"] == "DISABLED"


def test_delete_schema():
    db = MonetDBMock()
    code = "schema"
    version = "1.0"
    force = True
    DeleteSchema(db).execute(code, version, force)
    assert 'DROP SCHEMA "schema:1.0" CASCADE' in db.captured_queries[0]
    assert "DELETE FROM mipdb_metadata.datasets" in db.captured_queries[1]
    assert "DELETE FROM mipdb_metadata.datasets" in db.captured_queries[2]
    assert "DELETE FROM mipdb_metadata.schemas" in db.captured_queries[3]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_schema_with_db(db, schema_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "schema:1.0" in schemas

    # Test with force False
    DeleteSchema(db).execute(code=schema_data["code"], version=schema_data["version"], force=False)
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "schema:1.0" not in schemas


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_schema_with_db_with_force(db, schema_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "schema:1.0" in schemas

    # Test with force True
    DeleteSchema(db).execute(code=schema_data["code"], version=schema_data["version"], force=True)
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "schema:1.0" not in schemas



@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_schema_with_datasets_with_db(db, schema_data, dataset_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "schema:1.0" in schemas
    data = pd.DataFrame(
        {
            "var1": [1, 2, 3, 4, 5],
            "var2": ["l1", "l2", "l1", "l1", "l2"],
            "var3": [11, 12, 13, 14, 15],
            "var4": [21, 22, 23, 24, 25],
            "dataset": ["a_ds", "a_ds", "a_ds", "a_ds", "a_ds"],
        }
    )
    AddDataset(db).execute(data, "schema", "1.0")

    # Test with force False
    with pytest.raises(AccessError):
        DeleteSchema(db).execute(code=schema_data["code"], version=schema_data["version"], force=False)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_schema_with_datasets_with_db_with_force(db, schema_data, dataset_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "schema:1.0" in schemas
    data = pd.DataFrame(
        {
            "var1": [1, 2, 3, 4, 5],
            "var2": ["l1", "l2", "l1", "l1", "l2"],
            "var3": [11, 12, 13, 14, 15],
            "var4": [21, 22, 23, 24, 25],
            "dataset": ["a_ds", "a_ds", "a_ds", "a_ds", "a_ds"],
        }
    )
    AddDataset(db).execute(data, "schema", "1.0")

    # Test with force True
    DeleteSchema(db).execute(code=schema_data["code"], version=schema_data["version"], force=True)
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "schema:1.0" not in schemas


def test_update_schemas_on_schema_deletion():
    db = MonetDBMock()
    record = {"code": "code", "version": "1.0"}
    update_schemas_on_schema_deletion(record, db)
    expected = f"DELETE FROM mipdb_metadata.schemas WHERE code = :code AND version = :version "
    assert expected in db.captured_queries[0]
    assert db.captured_params[0] == record


def test_update_datasets_on_schema_deletion():
    db = MonetDBMock()
    record = {"dataset_id": 1, "schema_id" : 1}
    update_datasets_on_schema_deletion(record, db)
    expected = f"DELETE FROM mipdb_metadata.datasets WHERE "
    assert expected in db.captured_queries[0]
    assert db.captured_params[0] == record


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_dataset(db, schema_data, dataset_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    data = pd.DataFrame(
        {
            "var1": [1, 2, 3, 4, 5],
            "var2": ["l1", "l2", "l1", "l1", "l2"],
            "var3": [11, 12, 13, 14, 15],
            "var4": [21, 22, 23, 24, 25],
            "dataset": ["a_ds", "a_ds", "a_ds", "a_ds", "a_ds"],
        }
    )
    # Test success
    AddDataset(db).execute(data, "schema", "1.0")
    res = db.execute('SELECT * FROM "schema:1.0".primary_data').fetchall()
    assert res != []

    # Test that it is not possible to add the same dataset
    with pytest.raises(UserInputError):
        AddDataset(db).execute(data, "schema", "1.0")


def test_add_dataset_mock(schema_data, dataset_data):
    db = MonetDBMock()
    data = pd.DataFrame(
        {
            "var1": [1, 2, 3, 4, 5],
            "var2": ["l1", "l2", "l1", "l1", "l2"],
            "var3": [11, 12, 13, 14, 15],
            "var4": [21, 22, 23, 24, 25],
            "dataset": ["a_ds", "a_ds", "a_ds", "a_ds", "a_ds"],
        }
    )
    AddDataset(db).execute(data, "schema", "1.0")
    assert "Sequence('dataset_id_seq'" in db.captured_queries[0]
    assert 'INSERT INTO "schema:1.0".primary_data' in db.captured_queries[1]
    assert 'INSERT INTO mipdb_metadata.datasets' in db.captured_queries[2]
    assert "Sequence('action_id_seq'" in db.captured_queries[3]
    assert 'INSERT INTO "mipdb_metadata".actions' in db.captured_queries[4]
    assert len(db.captured_queries) > 3  # verify that handlers issued more queries


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_dataset_with_db(db, schema_data, dataset_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)

    # Test
    AddDataset(db).execute(dataset_data, "schema", "1.0")
    datasets = db.get_datasets()
    assert len(datasets) == 1
    assert datasets[0] == "a_dataset"


def test_update_datasets_on_dataset_addition():
    db = MonetDBMock()
    record = dict(
        schema_id=1,
        dataset_id=1,
        code="a_dataset",
    )
    update_datasets_on_dataset_addition(record, db)
    assert f"INSERT INTO mipdb_metadata.datasets" in db.captured_queries[0]
    datasets_record = db.captured_multiparams[0][0]
    assert datasets_record["status"] == "DISABLED"


def test_delete_dataset():
    db = MonetDBMock()
    dataset = "a_dataset"
    code = "schema"
    version = "1.0"
    DeleteDataset(db).execute(dataset, code, version)
    assert 'DELETE FROM "schema:1.0"."primary_data"' in db.captured_queries[0]
    assert 'DELETE FROM mipdb_metadata.datasets ' in db.captured_queries[1]
    assert "Sequence('action_id_seq'" in db.captured_queries[2]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[3]
    assert len(db.captured_queries) > 2  # verify that handlers issued more queries


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_dataset_with_db(db, schema_data, dataset_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    AddDataset(db).execute(dataset_data, "schema", "1.0")
    datasets = db.get_datasets()
    assert len(datasets) == 1
    assert "a_dataset" in datasets

    # Test
    DeleteDataset(db).execute(datasets[0], schema_data["code"], schema_data["version"])
    datasets = db.get_datasets()
    assert len(datasets) == 0
    assert "a_dataset" not in datasets


def test_update_datasets_on_dataset_deletion():
    db = MonetDBMock()
    record = dict(
        dataset_id=1,
        schema_id=1,
    )
    update_datasets_on_dataset_deletion(record, db)
    expected = f"DELETE FROM mipdb_metadata.datasets WHERE dataset_id = :dataset_id AND schema_id = :schema_id"
    assert expected in db.captured_queries[0]
    assert db.captured_params[0] == record


def test_tag_schema():
    db = MonetDBMock()
    code = "schema"
    version = "1.0"
    tag = "tag"
    key_value = ("key", "value")
    remove_flag = False
    TagSchema(db).execute(code, version, tag, key_value, remove_flag)
    assert 'UPDATE mipdb_metadata.schemas SET properties' in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[2]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_tag_schema_addition_with_db(db, schema_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)

    # Test
    TagSchema(db).execute(schema_data["code"],
                          schema_data["version"],
                          "tag",
                          ("key", "value"),
                          False)

    properties = db.get_schema_properties(1)
    assert properties == '{"tags": ["tag"], "key": "value"}'


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_tag_schema_deletion_with_db(db, schema_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    TagSchema(db).execute(schema_data["code"],
                          schema_data["version"],
                          "tag",
                          ("key", "value"),
                          False)

    # Test
    TagSchema(db).execute(schema_data["code"],
                          schema_data["version"],
                          "tag",
                          ("key", "value"),
                          True)
    properties = db.get_schema_properties(1)
    assert properties == '{"tags": []}'


def test_tag_dataset():
    db = MonetDBMock()
    dataset = "a_dataset"
    code = "schema"
    version = "1.0"
    tag = "tag"
    key_value = ("key", "value")
    remove_flag = False
    TagDataset(db).execute(dataset, code, version, tag, key_value, remove_flag)
    assert 'UPDATE mipdb_metadata.datasets SET properties' in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[2]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_tag_dataset_addition_with_db(db, schema_data, dataset_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    AddDataset(db).execute(dataset_data, "schema", "1.0")

    # Test
    TagDataset(db).execute("a_dataset",
                           schema_data["code"],
                           schema_data["version"],
                           "tag",
                           ("key", "value"),
                           False)

    properties = db.get_dataset_properties(1)
    assert properties == '{"tags": ["tag"], "key": "value"}'


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_tag_dataset_deletion_with_db(db, schema_data, dataset_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    AddDataset(db).execute(dataset_data, "schema", "1.0")
    TagDataset(db).execute("a_dataset",
                           schema_data["code"],
                           schema_data["version"],
                           "tag",
                           ("key", "value"),
                           False)

    # Test
    TagDataset(db).execute("a_dataset",
                           schema_data["code"],
                           schema_data["version"],
                           "tag",
                           ("key", "value"),
                           True)
    properties = db.get_dataset_properties(1)
    assert properties == '{"tags": []}'


record_and_funcs = [
    ({"code": "code", "version": "1.0", "schema_id": 1}, update_actions_on_schema_addition),
    ({"dataset_id": 1, "schema_id": 1, "code": "a_dataset"}, update_actions_on_schema_deletion),
    ({"code": "code", "version": "1.0", "schema_id": 1}, update_actions_on_dataset_addition),
    ({"dataset_id": 1, "schema_id": 1, "version": "1.0"}, update_actions_on_dataset_deletion),
    ({"code": "code", "version": "1.0", "schema_id": 1, "action": "REMOVE SCHEMA TAG"}, update_actions_on_schema_tagging),
    ({"dataset_id": 1, "schema_id": 1, "version": "1.0", "action": "ADD DATASET TAG"}, update_actions_on_dataset_tagging),

]


@pytest.mark.parametrize("record,func", record_and_funcs)
def test_update_actions(record, func):
    db = MonetDBMock()
    func(record, db)
    assert f'INSERT INTO "mipdb_metadata".actions' in db.captured_queries[1]
    actions_record = db.captured_multiparams[1][0]
    actions_record = actions_record["action"]
    actions_record = ast.literal_eval(actions_record)
    assert set(record.values()) <= set(actions_record.values())
