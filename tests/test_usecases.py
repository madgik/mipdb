import json

import pandas as pd
import pytest

from mipdb.database import METADATA_SCHEMA
from mipdb.exceptions import ForeignKeyError
from mipdb.exceptions import InvalidDatasetError
from mipdb.exceptions import UserInputError
from mipdb.schema import Schema
from mipdb.tables import DataModelTable, DatasetsTable, ActionsTable
from mipdb.usecases import AddPropertyToDataset
from mipdb.usecases import AddPropertyToDataModel
from mipdb.usecases import (
    AddDataModel,
    AddDataset,
    DeleteDataModel,
    DeleteDataset,
    EnableDataModel,
    DisableDataModel,
    EnableDataset,
    DisableDataset,
    InitDB,
)
from mipdb.usecases import RemovePropertyFromDataset
from mipdb.usecases import RemovePropertyFromDataModel
from mipdb.usecases import TagDataset
from mipdb.usecases import UntagDataset
from mipdb.usecases import TagDataModel
from mipdb.usecases import UntagDataModel
from mipdb.usecases import ValidateDataset
from mipdb.usecases import is_db_initialized
from tests.mocks import MonetDBMock


# NOTE Some use cases have a main responsibility (e.g. add a new data_model) which
# is followed by some additional actions (e.g. updating the data_models and actions
# table).  These additional actions are implemented as handlers using an event
# system. The use case tests below verify that the main queries are correct and
# that more queries have been issued by the handlers. Separate tests verify
# that the correct queries have been issued by the handlers.

@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_init_with_db(db):
    # Setup
    InitDB(db).execute()
    metadata = Schema(METADATA_SCHEMA)
    data_model_table = DataModelTable(schema=metadata)
    datasets_table = DatasetsTable(schema=metadata)
    actions_table = ActionsTable(schema=metadata)

    # Test
    assert "mipdb_metadata" in db.get_schemas()
    assert data_model_table.exists(db)
    assert datasets_table.exists(db)
    assert actions_table.exists(db)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_is_db_initialized_with_db_fail(db):
    with pytest.raises(UserInputError):
        is_db_initialized(db=db)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_is_db_initialized_with_db_fail(db):
    InitDB(db).execute()
    assert is_db_initialized(db=db)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_init_with_db(db):
    # Setup
    InitDB(db).execute()
    metadata = Schema(METADATA_SCHEMA)
    data_model_table = DataModelTable(schema=metadata)
    datasets_table = DatasetsTable(schema=metadata)
    actions_table = ActionsTable(schema=metadata)
    InitDB(db).execute()

    # Test
    assert "mipdb_metadata" in db.get_schemas()
    assert data_model_table.exists(db)
    assert datasets_table.exists(db)
    assert actions_table.exists(db)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_re_init_with_missing_schema_with_db(db):
    # Setup
    InitDB(db).execute()
    metadata = Schema(METADATA_SCHEMA)
    data_model_table = DataModelTable(schema=metadata)
    datasets_table = DatasetsTable(schema=metadata)
    actions_table = ActionsTable(schema=metadata)
    db.execute(f'DROP SCHEMA "mipdb_metadata" CASCADE')
    assert "mipdb_metadata" not in db.get_schemas()
    InitDB(db).execute()

    # Test
    assert "mipdb_metadata" in db.get_schemas()
    assert data_model_table.exists(db)
    assert datasets_table.exists(db)
    assert actions_table.exists(db)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_re_init_with_missing_actions_table_with_db(db):
    # Setup
    InitDB(db).execute()
    metadata = Schema(METADATA_SCHEMA)
    data_model_table = DataModelTable(schema=metadata)
    datasets_table = DatasetsTable(schema=metadata)
    actions_table = ActionsTable(schema=metadata)
    db.execute(f'DROP TABLE "mipdb_metadata".actions')
    assert "mipdb_metadata" in db.get_schemas()
    assert data_model_table.exists(db)
    assert datasets_table.exists(db)
    assert not actions_table.exists(db)
    InitDB(db).execute()

    # Test
    assert "mipdb_metadata" in db.get_schemas()
    assert data_model_table.exists(db)
    assert datasets_table.exists(db)
    assert actions_table.exists(db)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_re_init_with_missing_data_models_table_with_db(db):
    # Setup
    InitDB(db).execute()
    metadata = Schema(METADATA_SCHEMA)
    data_model_table = DataModelTable(schema=metadata)
    datasets_table = DatasetsTable(schema=metadata)
    actions_table = ActionsTable(schema=metadata)
    db.execute(f'DROP TABLE "mipdb_metadata".data_models CASCADE')
    assert "mipdb_metadata" in db.get_schemas()
    assert not data_model_table.exists(db)
    assert datasets_table.exists(db)
    assert actions_table.exists(db)
    InitDB(db).execute()

    # Test
    assert "mipdb_metadata" in db.get_schemas()
    assert data_model_table.exists(db)
    assert datasets_table.exists(db)
    assert actions_table.exists(db)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_re_init_with_missing_datasets_table_with_db(db):
    # Setup
    InitDB(db).execute()
    metadata = Schema(METADATA_SCHEMA)
    data_model_table = DataModelTable(schema=metadata)
    datasets_table = DatasetsTable(schema=metadata)
    actions_table = ActionsTable(schema=metadata)
    db.execute(f'DROP TABLE "mipdb_metadata".datasets')
    assert "mipdb_metadata" in db.get_schemas()
    assert data_model_table.exists(db)
    assert not datasets_table.exists(db)
    assert actions_table.exists(db)
    InitDB(db).execute()

    # Test
    assert "mipdb_metadata" in db.get_schemas()
    assert data_model_table.exists(db)
    assert datasets_table.exists(db)
    assert actions_table.exists(db)


def test_add_data_model_mock(data_model_metadata):
    db = MonetDBMock()
    AddDataModel(db).execute(data_model_metadata=data_model_metadata)
    assert 'CREATE SCHEMA "data_model:1.0"' in db.captured_queries[1]
    assert 'CREATE TABLE "data_model:1.0".primary_data' in db.captured_queries[2]
    assert f'CREATE TABLE "data_model:1.0".variables_metadata' in db.captured_queries[3]
    assert f'INSERT INTO "data_model:1.0".variables_metadata' in db.captured_queries[4]
    assert len(db.captured_queries) > 5  # verify that handlers issued more queries


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_data_model_with_db(db, data_model_metadata):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata=data_model_metadata)

    # Test
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "data_model:1.0" in schemas


def test_delete_data_model():
    db = MonetDBMock()
    code = "data_model"
    version = "1.0"
    force = True
    DeleteDataModel(db).execute(code=code, version=version, force=force)

    assert 'DELETE FROM "data_model:1.0"."primary_data"' in db.captured_queries[0]
    assert "DELETE FROM mipdb_metadata.datasets" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions' in db.captured_queries[3]
    assert 'DROP SCHEMA "data_model:1.0" CASCADE' in db.captured_queries[4]
    assert "DELETE FROM mipdb_metadata.data_models" in db.captured_queries[5]
    assert 'INSERT INTO "mipdb_metadata".actions' in db.captured_queries[7]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_data_model_with_db(db, data_model_metadata):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata=data_model_metadata)
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "data_model:1.0" in schemas

    # Test with force False
    DeleteDataModel(db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        force=False,
    )
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "data_model:1.0" not in schemas


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_data_model_with_db_with_force(db, data_model_metadata):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata=data_model_metadata)
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "data_model:1.0" in schemas

    # Test with force True
    DeleteDataModel(db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        force=True,
    )
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "data_model:1.0" not in schemas


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_data_model_with_datasets_with_db(db, data_model_metadata, dataset_data):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "data_model:1.0" in schemas
    data = pd.DataFrame(
        {
            "var1": [1, 2, 3, 4, 5],
            "var2": ["l1", "l2", "l1", "l1", "l2"],
            "var3": [11, 12, 13, 14, 15],
            "var4": [21, 22, 23, 24, 25],
            "dataset": ["dataset1", "dataset1", "dataset1", "dataset1", "dataset1"],
        }
    )
    AddDataset(db).execute(
        dataset_data=data, data_model_code="data_model", data_model_version="1.0"
    )

    # Test with force False
    with pytest.raises(ForeignKeyError):
        DeleteDataModel(db).execute(
            code=data_model_metadata["code"],
            version=data_model_metadata["version"],
            force=False,
        )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_data_model_with_datasets_with_db_with_force(
    db, data_model_metadata, dataset_data
):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "data_model:1.0" in schemas
    data = pd.DataFrame(
        {
            "var1": [1, 2, 3, 4, 5],
            "var2": ["l1", "l2", "l1", "l1", "l2"],
            "var3": [11, 12, 13, 14, 15],
            "var4": [21, 22, 23, 24, 25],
            "dataset": ["dataset1", "dataset1", "dataset1", "dataset1", "dataset1"],
        }
    )
    AddDataset(db).execute(
        dataset_data=data, data_model_code="data_model", data_model_version="1.0"
    )

    # Test with force True
    DeleteDataModel(db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        force=True,
    )
    schemas = db.get_schemas()
    assert "mipdb_metadata" in schemas
    assert "data_model:1.0" not in schemas


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_dataset(db, data_model_metadata, dataset_data):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    data = pd.DataFrame(
        {
            "var1": [1, 2, 3, 4, 5],
            "var2": ["l1", "l2", "l1", "l1", "l2"],
            "var3": [11, 12, 13, 14, 15],
            "var4": [21, 22, 23, 24, 25],
            "dataset": ["dataset1", "dataset1", "dataset1", "dataset1", "dataset1"],
        }
    )
    # Test success
    AddDataset(db).execute(
        dataset_data=data, data_model_code="data_model", data_model_version="1.0"
    )
    res = db.execute('SELECT * FROM "data_model:1.0".primary_data').fetchall()
    assert res != []

    # Test that it is not possible to add the same dataset
    with pytest.raises(UserInputError):
        AddDataset(db).execute(
            dataset_data=data, data_model_code="data_model", data_model_version="1.0"
        )


def test_add_dataset_mock(data_model_metadata, dataset_data):
    db = MonetDBMock()
    data = pd.DataFrame(
        {
            "var1": [1, 2, 3, 4, 5],
            "var2": ["l1", "l2", "l1", "l1", "l2"],
            "var3": [11, 12, 13, 14, 15],
            "var4": [21, 22, 23, 24, 25],
            "dataset": ["dataset1", "dataset1", "dataset1", "dataset1", "dataset1"],
        }
    )
    AddDataset(db).execute(data, "data_model", "1.0")
    assert "Sequence('dataset_id_seq'" in db.captured_queries[0]
    assert 'INSERT INTO "data_model:1.0".primary_data' in db.captured_queries[1]
    assert "INSERT INTO mipdb_metadata.datasets" in db.captured_queries[2]
    assert "Sequence('action_id_seq'" in db.captured_queries[3]
    assert 'INSERT INTO "mipdb_metadata".actions' in db.captured_queries[4]
    assert len(db.captured_queries) > 3  # verify that handlers issued more queries


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_dataset_with_db(db, data_model_metadata, dataset_data):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)

    # Test
    AddDataset(db).execute(
        dataset_data=dataset_data,
        data_model_code="data_model",
        data_model_version="1.0",
    )
    datasets = db.get_datasets(columns=["code"])
    assert len(datasets) == 1
    assert datasets[0] == ("dataset",)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_validate_dataset(db, data_model_metadata, dataset_data):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    data = pd.DataFrame(
        {
            "subjectcode": [2, 2, 2, 4, 4],
            "var1": [1, 2, 3, 4, 5],
            "var2": ["l1", "l2", "l1", "l1", "l2"],
            "var3": [11, 12, 13, 14, 15],
            "var4": [21, 22, 23, 24, 25],
            "dataset": ["dataset", "dataset", "dataset", "dataset", "dataset"],
        }
    )
    # Test success
    ValidateDataset(db).execute(
        dataset_data=data, data_model_code="data_model", data_model_version="1.0"
    )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_validate_dataset_without_subjectcode(db, data_model_metadata, dataset_data):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    data = pd.DataFrame(
        {
            "var1": [1, 2, 3, 4, 5],
            "var2": ["l1", "l2", "l1", "l1", "l2"],
            "var3": [11, 12, 13, 14, 15],
            "var4": [21, 22, 23, 24, 25],
            "dataset": ["dataset", "dataset", "dataset", "dataset", "dataset"],
        }
    )

    with pytest.raises(InvalidDatasetError):
        ValidateDataset(db).execute(
            dataset_data=data, data_model_code="data_model", data_model_version="1.0"
        )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_validate_dataset_non_existing_column(db, data_model_metadata, dataset_data):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    data = pd.DataFrame(
        {
            "subjectcode": [1, 2, 3, 4, 5],
            "invalid_column": ["l1", "l2", "l1", "l1", "l2"],
            "var3": [11, 12, 13, 14, 15],
            "var4": [21, 22, 23, 24, 25],
            "dataset": ["dataset", "dataset", "dataset", "dataset", "dataset"],
        }
    )

    with pytest.raises(InvalidDatasetError):
        ValidateDataset(db).execute(
            dataset_data=data, data_model_code="data_model", data_model_version="1.0"
        )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_validate_dataset_with_invalid_column(db, data_model_metadata, dataset_data):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    data = pd.DataFrame(
        {
            "subjectcode": [1, 2, 3, 4, 5],
            "var3": [11, 12, 13, 14, 15],
            "var4": ["invalid_type", 22, 23, 24, 25],
            "dataset": ["dataset", "dataset", "dataset", "dataset", "dataset"],
        }
    )

    with pytest.raises(InvalidDatasetError):
        ValidateDataset(db).execute(
            dataset_data=data, data_model_code="data_model", data_model_version="1.0"
        )


def test_delete_dataset():
    db = MonetDBMock()
    dataset = "dataset"
    code = "data_model"
    version = "1.0"
    DeleteDataset(db).execute(
        dataset_code=dataset, data_model_code=code, data_model_version=version
    )

    assert (
        'DELETE FROM "data_model:1.0"."primary_data" WHERE dataset = :dataset_name '
        in db.captured_queries[0]
    )
    assert (
        "DELETE FROM mipdb_metadata.datasets WHERE dataset_id = :dataset_id AND data_model_id = :data_model_id "
        in db.captured_queries[1]
    )
    assert (
        'INSERT INTO "mipdb_metadata".actions VALUES(:action_id, :action)'
        in db.captured_queries[3]
    )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_dataset_with_db(db, data_model_metadata, dataset_data):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    AddDataset(db).execute(
        dataset_data=dataset_data,
        data_model_code="data_model",
        data_model_version="1.0",
    )
    datasets = db.get_datasets(columns=["code"])
    assert len(datasets) == 1
    assert ("dataset",) in datasets

    # Test
    DeleteDataset(db).execute(
        dataset_code="dataset",
        data_model_code=data_model_metadata["code"],
        data_model_version=data_model_metadata["version"],
    )
    datasets = db.get_datasets(columns=["code"])
    assert len(datasets) == 0
    assert ("dataset",) not in datasets


def test_enable_data_model():
    db = MonetDBMock()
    code = "data_model"
    version = "1.0"
    EnableDataModel(db).execute(code=code, version=version)
    assert "UPDATE mipdb_metadata.data_models" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions' in db.captured_queries[2]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_enable_data_model_with_db(db, data_model_metadata):
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata=data_model_metadata)
    DisableDataModel(db).execute(
        code=data_model_metadata["code"], version=data_model_metadata["version"]
    )
    status = db.execute(f"SELECT status FROM mipdb_metadata.data_models").fetchone()
    assert status[0] == "DISABLED"
    EnableDataModel(db).execute(
        code=data_model_metadata["code"], version=data_model_metadata["version"]
    )
    status = db.execute(f"SELECT status FROM mipdb_metadata.data_models").fetchone()
    assert status[0] == "ENABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_enable_data_model_already_enabled_with_db(db, data_model_metadata):
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    status = db.execute(f"SELECT status FROM mipdb_metadata.data_models").fetchone()
    assert status[0] == "ENABLED"

    with pytest.raises(UserInputError):
        EnableDataModel(db).execute(
            code=data_model_metadata["code"], version=data_model_metadata["version"]
        )


def test_disable_data_model():
    db = MonetDBMock()
    code = "data_model"
    version = "1.0"
    DisableDataModel(db).execute(code, version)
    assert "UPDATE mipdb_metadata.data_models" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions' in db.captured_queries[2]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_disable_data_model_with_db(db, data_model_metadata):
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    status = db.execute(f"SELECT status FROM mipdb_metadata.data_models").fetchone()
    assert status[0] == "ENABLED"
    DisableDataModel(db).execute(
        code=data_model_metadata["code"], version=data_model_metadata["version"]
    )
    status = db.execute(f"SELECT status FROM mipdb_metadata.data_models").fetchone()
    assert status[0] == "DISABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_disable_data_model_already_disabled_with_db(db, data_model_metadata):
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    DisableDataModel(db).execute(
        code=data_model_metadata["code"], version=data_model_metadata["version"]
    )
    status = db.execute(f"SELECT status FROM mipdb_metadata.data_models").fetchone()
    assert status[0] == "DISABLED"

    with pytest.raises(UserInputError):
        DisableDataModel(db).execute(
            code=data_model_metadata["code"], version=data_model_metadata["version"]
        )


def test_enable_dataset():
    db = MonetDBMock()
    dataset = "dataset"
    code = "data_model"
    version = "1.0"
    EnableDataset(db).execute(dataset, code, version)
    assert "UPDATE mipdb_metadata.datasets" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions' in db.captured_queries[2]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_enable_dataset_with_db(db, data_model_metadata, dataset_data):
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    AddDataset(db).execute(
        dataset_data=dataset_data,
        data_model_code="data_model",
        data_model_version="1.0",
    )
    DisableDataset(db).execute(
        dataset_code="dataset",
        data_model_code=data_model_metadata["code"],
        data_model_version=data_model_metadata["version"],
    )
    status = db.execute(f"SELECT status FROM mipdb_metadata.datasets").fetchone()
    assert status[0] == "DISABLED"
    EnableDataset(db).execute(
        dataset_code="dataset",
        data_model_code=data_model_metadata["code"],
        data_model_version=data_model_metadata["version"],
    )
    status = db.execute(f"SELECT status FROM mipdb_metadata.datasets").fetchone()
    assert status[0] == "ENABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_enable_dataset_already_enabled_with_db(db, data_model_metadata, dataset_data):
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    AddDataset(db).execute(
        dataset_data=dataset_data,
        data_model_code="data_model",
        data_model_version="1.0",
    )
    status = db.execute(f"SELECT status FROM mipdb_metadata.datasets").fetchone()
    assert status[0] == "ENABLED"

    with pytest.raises(UserInputError):
        EnableDataset(db).execute(
            dataset_code="dataset",
            data_model_code=data_model_metadata["code"],
            data_model_version=data_model_metadata["version"],
        )


def test_disable_dataset():
    db = MonetDBMock()
    dataset = "dataset"
    code = "data_model"
    version = "1.0"
    DisableDataset(db).execute(dataset, code, version)
    assert "UPDATE mipdb_metadata.datasets" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions' in db.captured_queries[2]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_disable_dataset_with_db(db, data_model_metadata, dataset_data):
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    AddDataset(db).execute(
        dataset_data=dataset_data,
        data_model_code="data_model",
        data_model_version="1.0",
    )
    status = db.execute(f"SELECT status FROM mipdb_metadata.datasets").fetchone()
    assert status[0] == "ENABLED"
    DisableDataset(db).execute(
        dataset_code="dataset",
        data_model_code=data_model_metadata["code"],
        data_model_version=data_model_metadata["version"],
    )
    status = db.execute(f"SELECT status FROM mipdb_metadata.datasets").fetchone()
    assert status[0] == "DISABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_disable_dataset_already_disabled_with_db(
    db, data_model_metadata, dataset_data
):
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    AddDataset(db).execute(
        dataset_data=dataset_data,
        data_model_code="data_model",
        data_model_version="1.0",
    )
    DisableDataset(db).execute(
        dataset_code="dataset",
        data_model_code=data_model_metadata["code"],
        data_model_version=data_model_metadata["version"],
    )
    status = db.execute(f"SELECT status FROM mipdb_metadata.datasets").fetchone()
    assert status[0] == "DISABLED"

    with pytest.raises(UserInputError):
        DisableDataset(db).execute(
            dataset_code="dataset",
            data_model_code=data_model_metadata["code"],
            data_model_version=data_model_metadata["version"],
        )


def test_tag_data_model():
    db = MonetDBMock()
    TagDataModel(db).execute(code="data_model", version="1.0", tag="tag")
    assert "UPDATE mipdb_metadata.data_models SET properties" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[2]


def test_untag_data_model():
    db = MonetDBMock()
    UntagDataModel(db).execute(code="data_model", version="1.0", tag="tag1")
    assert "UPDATE mipdb_metadata.data_models SET properties" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[2]


def test_add_property2data_model():
    db = MonetDBMock()
    AddPropertyToDataModel(db).execute(
        code="data_model", version="1.0", key="key", value="value", force=False
    )
    assert "UPDATE mipdb_metadata.data_models SET properties" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[2]


def test_remove_property_from_data_model():
    db = MonetDBMock()
    RemovePropertyFromDataModel(db).execute(
        code="data_model", version="1.0", key="key1", value="value1"
    )
    assert "UPDATE mipdb_metadata.data_models SET properties" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[2]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_tag_data_model_with_db(db, data_model_metadata):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)

    # Test
    TagDataModel(db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        tag="tag",
    )

    properties = db.get_data_model_properties(1)
    assert json.loads(
        properties
    )["tags"] == ["tag"]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_untag_data_model_with_db(db, data_model_metadata):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    TagDataModel(db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        tag="tag1",
    )
    TagDataModel(db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        tag="tag2",
    )
    TagDataModel(db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        tag="tag3",
    )

    # Test
    UntagDataModel(db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        tag="tag1",
    )
    properties = db.get_data_model_properties(1)
    assert json.loads(
        properties
    )["tags"] == ["tag2", "tag3"]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_property2data_model_with_db(db, data_model_metadata):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)

    # Test
    AddPropertyToDataModel(db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key",
        value="value",
        force=False,
    )

    properties = db.get_data_model_properties(1)
    assert "key" in json.loads(
        properties
    )["properties"] and json.loads(
        properties
    )["properties"]["key"] == "value"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_property2data_model_with_force_and_db(db, data_model_metadata):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    AddPropertyToDataModel(db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key",
        value="value",
        force=False,
    )

    # Test
    AddPropertyToDataModel(db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key",
        value="value1",
        force=True,
    )

    properties = db.get_data_model_properties(1)
    assert "key" in json.loads(
        properties
    )["properties"] and json.loads(
        properties
    )["properties"]["key"] == "value1"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_remove_property_from_data_model_with_db(db, data_model_metadata):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    AddPropertyToDataModel(db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key1",
        value="value1",
        force=False,
    )
    AddPropertyToDataModel(db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key2",
        value="value2",
        force=False,
    )

    # Test
    RemovePropertyFromDataModel(db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key1",
        value="value1",
    )
    properties = db.get_data_model_properties(1)
    assert "key2" in json.loads(
        properties
    )["properties"] and json.loads(
        properties
    )["properties"]["key2"] == "value2"


def test_tag_dataset():
    db = MonetDBMock()
    TagDataset(db).execute(
        dataset_code="dataset",
        data_model_code="data_model",
        data_model_version="1.0",
        tag="tag",
    )
    assert "UPDATE mipdb_metadata.datasets SET properties" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[2]


def test_untag_dataset():
    db = MonetDBMock()
    UntagDataset(db).execute(
        dataset="dataset", data_model_code="data_model", version="1.0", tag="tag1"
    )
    assert "UPDATE mipdb_metadata.datasets SET properties" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[2]


def test_add_property2dataset():
    db = MonetDBMock()
    AddPropertyToDataset(db).execute(
        dataset="dataset",
        data_model_code="data_model",
        version="1.0",
        key="key",
        value="value",
        force=False,
    )
    assert "UPDATE mipdb_metadata.datasets SET properties" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[2]


def test_remove_property_from_dataset():
    db = MonetDBMock()
    RemovePropertyFromDataset(db).execute(
        dataset="dataset",
        data_model_code="data_model",
        version="1.0",
        key="key1",
        value="value1",
    )
    assert "UPDATE mipdb_metadata.datasets SET properties" in db.captured_queries[0]
    assert "Sequence('action_id_seq'" in db.captured_queries[1]
    assert 'INSERT INTO "mipdb_metadata".actions ' in db.captured_queries[2]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_tag_dataset_with_db(db, data_model_metadata, dataset_data):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    AddDataset(db).execute(dataset_data, "data_model", "1.0")

    # Test
    TagDataset(db).execute(
        dataset_code="dataset",
        data_model_code=data_model_metadata["code"],
        data_model_version=data_model_metadata["version"],
        tag="tag",
    )

    properties = db.get_dataset_properties(1)
    assert properties == '{"tags": ["tag"], "properties": {}}'


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_untag_dataset_with_db(db, data_model_metadata, dataset_data):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    AddDataset(db).execute(dataset_data, "data_model", "1.0")
    TagDataset(db).execute(
        dataset_code="dataset",
        data_model_code=data_model_metadata["code"],
        data_model_version=data_model_metadata["version"],
        tag="tag1",
    )
    TagDataset(db).execute(
        dataset_code="dataset",
        data_model_code=data_model_metadata["code"],
        data_model_version=data_model_metadata["version"],
        tag="tag2",
    )
    TagDataset(db).execute(
        dataset_code="dataset",
        data_model_code=data_model_metadata["code"],
        data_model_version=data_model_metadata["version"],
        tag="tag3",
    )

    # Test
    UntagDataset(db).execute(
        dataset="dataset",
        data_model_code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        tag="tag1",
    )

    properties = db.get_dataset_properties(1)
    assert properties == '{"tags": ["tag2", "tag3"], "properties": {}}'


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_property2dataset_with_db(db, data_model_metadata, dataset_data):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    AddDataset(db).execute(dataset_data, "data_model", "1.0")

    # Test
    AddPropertyToDataset(db).execute(
        dataset="dataset",
        data_model_code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key",
        value="value",
        force=False,
    )

    properties = db.get_dataset_properties(1)
    assert properties == '{"tags": [], "properties": {"key": "value"}}'


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_remove_property_from_dataset_with_db(db, data_model_metadata, dataset_data):
    # Setup
    InitDB(db).execute()
    AddDataModel(db).execute(data_model_metadata)
    AddDataset(db).execute(dataset_data, "data_model", "1.0")
    AddPropertyToDataset(db).execute(
        dataset="dataset",
        data_model_code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key",
        value="value",
        force=False,
    )
    AddPropertyToDataset(db).execute(
        dataset="dataset",
        data_model_code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key1",
        value="value1",
        force=False,
    )
    AddPropertyToDataset(db).execute(
        dataset="dataset",
        data_model_code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key2",
        value="value2",
        force=False,
    )

    # Test
    RemovePropertyFromDataset(db).execute(
        dataset="dataset",
        data_model_code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key2",
        value="value2",
    )
    properties = db.get_dataset_properties(1)
    assert (
        properties == '{"tags": [], "properties": {"key": "value", "key1": "value1"}}'
    )
