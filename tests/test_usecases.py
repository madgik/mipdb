
import pandas as pd
import pytest

from mipdb.exceptions import ForeignKeyError
from mipdb.exceptions import UserInputError
from mipdb.usecases import AddPropertyToDataset
from mipdb.usecases import AddPropertyToSchema
from mipdb.usecases import (
    AddSchema,
    AddDataset,
    DeleteSchema,
    DeleteDataset,
    EnableSchema,
    DisableSchema,
    EnableDataset,
    DisableDataset,
    InitDB,
    update_schemas_on_schema_addition,
    update_schemas_on_schema_deletion,
    update_datasets_on_dataset_addition,
    update_datasets_on_dataset_deletion,
)
from mipdb.usecases import RemovePropertyFromDataset
from mipdb.usecases import RemovePropertyFromSchema
from mipdb.usecases import TagDataset
from mipdb.usecases import UntagDataset
from mipdb.usecases import TagSchema
from mipdb.usecases import UntagSchema
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
    AddSchema(db).execute(schema_data=schema_data)
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
    AddSchema(db).execute(schema_data=schema_data)

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
    DeleteSchema(db).execute(code=code, version=version, force=force)
    assert 'DROP SCHEMA "schema:1.0" CASCADE' in db.captured_queries[0]
    assert "DELETE FROM mipdb_metadata.datasets" in db.captured_queries[1]
    assert "DELETE FROM mipdb_metadata.datasets" in db.captured_queries[2]
    assert "DELETE FROM mipdb_metadata.schemas" in db.captured_queries[3]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_schema_with_db(db, schema_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data=schema_data)
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "schema:1.0" in schemas

    # Test with force False
    DeleteSchema(db).execute(
        code=schema_data["code"], version=schema_data["version"], force=False
    )
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "schema:1.0" not in schemas


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_schema_with_db_with_force(db, schema_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data=schema_data)
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "schema:1.0" in schemas

    # Test with force True
    DeleteSchema(db).execute(
        code=schema_data["code"], version=schema_data["version"], force=True
    )
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
    AddDataset(db).execute(dataset_data=data, code="schema", version="1.0")

    # Test with force False
    with pytest.raises(ForeignKeyError):
        DeleteSchema(db).execute(
            code=schema_data["code"], version=schema_data["version"], force=False
        )


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
    AddDataset(db).execute(dataset_data=data, code="schema", version="1.0")

    # Test with force True
    DeleteSchema(db).execute(
        code=schema_data["code"], version=schema_data["version"], force=True
    )
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "schema:1.0" not in schemas


def test_update_schemas_on_schema_deletion():
    db = MonetDBMock()
    record = {"code": "code", "version": "1.0"}
    update_schemas_on_schema_deletion(record, db)
    expected = (
        f"DELETE FROM mipdb_metadata.schemas WHERE code = :code AND version = :version "
    )
    assert expected in db.captured_queries[0]
    assert db.captured_params[0] == record


def test_update_datasets_on_schema_deletion():
    db = MonetDBMock()
    record = {"dataset_ids": [1], "schema_id": 1}
    update_datasets_on_schema_deletion(record, db)
    expected = f"DELETE FROM mipdb_metadata.datasets WHERE "
    assert expected in db.captured_queries[0]


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
    AddDataset(db).execute(dataset_data=data, code="schema", version="1.0")
    res = db.execute('SELECT * FROM "schema:1.0".primary_data').fetchall()
    assert res != []

    # Test that it is not possible to add the same dataset
    with pytest.raises(UserInputError):
        AddDataset(db).execute(dataset_data=data, code="schema", version="1.0")


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
    assert "INSERT INTO mipdb_metadata.datasets" in db.captured_queries[2]
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
    AddDataset(db).execute(dataset_data=dataset_data, code="schema", version="1.0")
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
    DeleteDataset(db).execute(dataset=dataset, schema_code=code, version=version)
    assert 'DELETE FROM "schema:1.0"."primary_data"' in db.captured_queries[0]
    assert "DELETE FROM mipdb_metadata.datasets " in db.captured_queries[1]
    assert "Sequence('action_id_seq'" in db.captured_queries[2]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[3]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_dataset_with_db(db, schema_data, dataset_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    AddDataset(db).execute(dataset_data=dataset_data, code="schema", version="1.0")
    datasets = db.get_datasets()
    assert len(datasets) == 1
    assert "a_dataset" in datasets

    # Test
    DeleteDataset(db).execute(dataset=datasets[0], schema_code=schema_data["code"], version=schema_data["version"])
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


def test_enable_schema():
    db = MonetDBMock()
    code = "schema"
    version = "1.0"
    EnableSchema(db).execute(code=code, version=version)
    assert "UPDATE mipdb_metadata.schemas" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions' in db.captured_queries[2]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_enable_schema_with_db(db, schema_data):
    InitDB(db).execute()
    AddSchema(db).execute(schema_data=schema_data)
    status = db.execute(f"SELECT status FROM mipdb_metadata.schemas").fetchone()
    assert status[0] == "DISABLED"
    EnableSchema(db).execute(code=schema_data["code"], version=schema_data["version"])
    status = db.execute(f"SELECT status FROM mipdb_metadata.schemas").fetchone()
    assert status[0] == "ENABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_enable_schema_already_enabled_with_db(db, schema_data):
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    EnableSchema(db).execute(code=schema_data["code"], version=schema_data["version"])
    status = db.execute(f"SELECT status FROM mipdb_metadata.schemas").fetchone()
    assert status[0] == "ENABLED"

    with pytest.raises(UserInputError):
        EnableSchema(db).execute(code=schema_data["code"], version=schema_data["version"])


def test_disable_schema():
    db = MonetDBMock()
    code = "schema"
    version = "1.0"
    DisableSchema(db).execute(code, version)
    assert "UPDATE mipdb_metadata.schemas" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions' in db.captured_queries[2]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_disable_schema_with_db(db, schema_data):
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    EnableSchema(db).execute(code=schema_data["code"], version=schema_data["version"])
    status = db.execute(f"SELECT status FROM mipdb_metadata.schemas").fetchone()
    assert status[0] == "ENABLED"
    DisableSchema(db).execute(code=schema_data["code"], version=schema_data["version"])
    status = db.execute(f"SELECT status FROM mipdb_metadata.schemas").fetchone()
    assert status[0] == "DISABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_disable_schema_already_disabled_with_db(db, schema_data):
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    status = db.execute(f"SELECT status FROM mipdb_metadata.schemas").fetchone()
    assert status[0] == "DISABLED"

    with pytest.raises(UserInputError):
        DisableSchema(db).execute(code=schema_data["code"], version=schema_data["version"])


def test_enable_dataset():
    db = MonetDBMock()
    dataset = "a_dataset"
    code = "schema"
    version = "1.0"
    EnableDataset(db).execute(dataset, code, version)
    assert "UPDATE mipdb_metadata.datasets" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions' in db.captured_queries[2]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_enable_dataset_with_db(db, schema_data, dataset_data):
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    AddDataset(db).execute(dataset_data=dataset_data, code="schema", version="1.0")
    datasets = db.get_datasets()
    status = db.execute(f"SELECT status FROM mipdb_metadata.datasets").fetchone()
    assert status[0] == "DISABLED"
    EnableDataset(db).execute(dataset=datasets[0], schema_code=schema_data["code"], version=schema_data["version"])
    status = db.execute(f"SELECT status FROM mipdb_metadata.datasets").fetchone()
    assert status[0] == "ENABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_enable_dataset_already_enabled_with_db(db, schema_data, dataset_data):
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    AddDataset(db).execute(dataset_data=dataset_data, code="schema", version="1.0")
    datasets = db.get_datasets()
    EnableDataset(db).execute(dataset=datasets[0], schema_code=schema_data["code"], version=schema_data["version"])
    status = db.execute(f"SELECT status FROM mipdb_metadata.datasets").fetchone()
    assert status[0] == "ENABLED"

    with pytest.raises(UserInputError):
        EnableDataset(db).execute(
            dataset=datasets[0], schema_code=schema_data["code"], version=schema_data["version"]
        )


def test_disable_dataset():
    db = MonetDBMock()
    dataset = "a_dataset"
    code = "schema"
    version = "1.0"
    DisableDataset(db).execute(dataset, code, version)
    assert "UPDATE mipdb_metadata.datasets" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions' in db.captured_queries[2]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_disable_dataset_with_db(db, schema_data, dataset_data):
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    AddDataset(db).execute(dataset_data=dataset_data, code="schema", version="1.0")
    datasets = db.get_datasets()
    EnableDataset(db).execute(dataset=datasets[0], schema_code=schema_data["code"], version=schema_data["version"])
    status = db.execute(f"SELECT status FROM mipdb_metadata.datasets").fetchone()
    assert status[0] == "ENABLED"
    DisableDataset(db).execute(dataset=datasets[0], schema_code=schema_data["code"], version=schema_data["version"])
    status = db.execute(f"SELECT status FROM mipdb_metadata.datasets").fetchone()
    assert status[0] == "DISABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_disable_dataset_already_disabled_with_db(db, schema_data, dataset_data):
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    AddDataset(db).execute(dataset_data=dataset_data, code="schema", version="1.0")
    datasets = db.get_datasets()
    status = db.execute(f"SELECT status FROM mipdb_metadata.datasets").fetchone()
    assert status[0] == "DISABLED"

    with pytest.raises(UserInputError):
        DisableDataset(db).execute(
            dataset=datasets[0], schema_code=schema_data["code"], version=schema_data["version"]
        )


def test_tag_schema():
    db = MonetDBMock()
    TagSchema(db).execute(code="schema", version="1.0", tag="tag")
    assert "UPDATE mipdb_metadata.schemas SET properties" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[2]


def test_untag_schema():
    db = MonetDBMock()
    UntagSchema(db).execute(code="schema", version="1.0", tag="tag1")
    assert "UPDATE mipdb_metadata.schemas SET properties" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[2]


def test_add_property2schema():
    db = MonetDBMock()
    AddPropertyToSchema(db).execute(code="schema", version="1.0", key="key", value="value", force=False)
    assert "UPDATE mipdb_metadata.schemas SET properties" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[2]


def test_remove_property_from_schema():
    db = MonetDBMock()
    RemovePropertyFromSchema(db).execute(code="schema", version="1.0", key="key1", value="value1")
    assert "UPDATE mipdb_metadata.schemas SET properties" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[2]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_tag_schema_with_db(db, schema_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)

    # Test
    TagSchema(db).execute(
        code=schema_data["code"], version=schema_data["version"], tag="tag"
    )

    properties = db.get_schema_properties(1)
    assert properties == '{"tags": ["tag"], "properties": {}}'


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_untag_schema_with_db(db, schema_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    TagSchema(db).execute(
        code=schema_data["code"], version=schema_data["version"], tag="tag1"
    )
    TagSchema(db).execute(
        code=schema_data["code"], version=schema_data["version"], tag="tag2"
    )
    TagSchema(db).execute(
        code=schema_data["code"], version=schema_data["version"], tag="tag3"
    )

    # Test
    UntagSchema(db).execute(
        code=schema_data["code"], version=schema_data["version"], tag="tag1"
    )
    properties = db.get_schema_properties(1)
    assert properties == '{"tags": ["tag2", "tag3"], "properties": {}}'


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_property2schema_with_db(db, schema_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)

    # Test
    AddPropertyToSchema(db).execute(
        code=schema_data["code"], version=schema_data["version"], key="key", value="value", force=False
    )

    properties = db.get_schema_properties(1)
    assert properties == '{"tags": [], "properties": {"key": "value"}}'


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_property2schema_with_force_and_db(db, schema_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    AddPropertyToSchema(db).execute(
        code=schema_data["code"], version=schema_data["version"], key="key", value="value", force=False
    )

    # Test
    AddPropertyToSchema(db).execute(
        code=schema_data["code"], version=schema_data["version"], key="key", value="value1", force=True
    )

    properties = db.get_schema_properties(1)
    assert properties == '{"tags": [], "properties": {"key": "value1"}}'


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_remove_property_from_schema_with_db(db, schema_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    AddPropertyToSchema(db).execute(
        code=schema_data["code"],
        version=schema_data["version"],
        key="key1",
        value="value1",
        force=False
    )
    AddPropertyToSchema(db).execute(
        code=schema_data["code"],
        version=schema_data["version"],
        key="key2",
        value="value2",
        force=False
    )

    # Test
    RemovePropertyFromSchema(db).execute(
        code=schema_data["code"], version=schema_data["version"], key="key1", value="value1"
    )
    properties = db.get_schema_properties(1)
    assert properties == '{"tags": [], "properties": {"key2": "value2"}}'


def test_tag_dataset():
    db = MonetDBMock()
    TagDataset(db).execute(dataset="a_dataset", schema_code="schema", version="1.0", tag="tag")
    assert "UPDATE mipdb_metadata.datasets SET properties" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[2]


def test_untag_dataset():
    db = MonetDBMock()
    UntagDataset(db).execute(dataset="a_dataset", schema_code="schema", version="1.0", tag="tag1")
    assert "UPDATE mipdb_metadata.datasets SET properties" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[2]


def test_add_property2dataset():
    db = MonetDBMock()
    AddPropertyToDataset(db).execute(dataset="a_dataset", schema_code="schema", version="1.0", key="key", value="value", force=False)
    assert "UPDATE mipdb_metadata.datasets SET properties" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[2]


def test_remove_property_from_dataset():
    db = MonetDBMock()
    RemovePropertyFromDataset(db).execute(
        dataset="a_dataset",
        schema_code="schema",
        version="1.0",
        key="key1",
        value="value1")
    assert "UPDATE mipdb_metadata.datasets SET properties" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[2]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_tag_dataset_with_db(db, schema_data, dataset_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    AddDataset(db).execute(dataset_data, "schema", "1.0")

    # Test
    TagDataset(db).execute(
        dataset="a_dataset",
        schema_code=schema_data["code"],
        version=schema_data["version"],
        tag="tag",
    )

    properties = db.get_dataset_properties(1)
    assert properties == '{"tags": ["tag"], "properties": {}}'


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_untag_dataset_with_db(db, schema_data, dataset_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    AddDataset(db).execute(dataset_data, "schema", "1.0")
    TagDataset(db).execute(
        dataset="a_dataset",
        schema_code=schema_data["code"],
        version=schema_data["version"],
        tag="tag1",
    )
    TagDataset(db).execute(
        dataset="a_dataset",
        schema_code=schema_data["code"],
        version=schema_data["version"],
        tag="tag2",
    )
    TagDataset(db).execute(
        dataset="a_dataset",
        schema_code=schema_data["code"],
        version=schema_data["version"],
        tag="tag3",
    )

    # Test
    UntagDataset(db).execute(
        dataset="a_dataset",
        schema_code=schema_data["code"],
        version=schema_data["version"],
        tag="tag1",
    )

    properties = db.get_dataset_properties(1)
    assert properties == '{"tags": ["tag2", "tag3"], "properties": {}}'


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_property2dataset_with_db(db, schema_data, dataset_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    AddDataset(db).execute(dataset_data, "schema", "1.0")

    # Test
    AddPropertyToDataset(db).execute(
        dataset="a_dataset",
        schema_code=schema_data["code"],
        version=schema_data["version"],
        key="key",
        value="value",
        force=False
    )

    properties = db.get_dataset_properties(1)
    assert properties == '{"tags": [], "properties": {"key": "value"}}'


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_remove_property_from_dataset_with_db(db, schema_data, dataset_data):
    # Setup
    InitDB(db).execute()
    AddSchema(db).execute(schema_data)
    AddDataset(db).execute(dataset_data, "schema", "1.0")
    AddPropertyToDataset(db).execute(
        dataset="a_dataset",
        schema_code=schema_data["code"],
        version=schema_data["version"],
        key="key",
        value="value",
        force=False
    )
    AddPropertyToDataset(db).execute(
            dataset="a_dataset",
            schema_code=schema_data["code"],
            version=schema_data["version"],
            key="key1",
            value="value1",
            force=False
    )
    AddPropertyToDataset(db).execute(
            dataset="a_dataset",
            schema_code=schema_data["code"],
            version=schema_data["version"],
            key="key2",
            value="value2",
            force=False
    )

    # Test
    RemovePropertyFromDataset(db).execute(
        dataset="a_dataset",
        schema_code=schema_data["code"],
        version=schema_data["version"],
        key="key2",
        value="value2"
    )
    properties = db.get_dataset_properties(1)
    assert properties == '{"tags": [], "properties": {"key": "value", "key1": "value1"}}'
