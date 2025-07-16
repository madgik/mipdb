from unittest.mock import patch

import pandas as pd
import pytest

from mipdb.monetdb.monetdb import MonetDB
from mipdb.exceptions import ForeignKeyError, DataBaseError, InvalidDatasetError
from mipdb.exceptions import UserInputError
from mipdb.sqlite.sqlite import Dataset
from mipdb.sqlite.sqlite_tables import DataModelTable, DatasetsTable
from mipdb.usecases import (
    AddPropertyToDataset,
    check_unique_longitudinal_dataset_primary_keys,
)
from mipdb.usecases import AddPropertyToDataModel
from mipdb.usecases import (
    AddDataModel,
    ImportCSV,
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
from tests.conftest import DATASET_FILE, ABSOLUTE_PATH_DATASET_FILE


# NOTE Some use cases have a main responsibility (e.g. add a new data_model) which
# is followed by some additional actions (e.g. updating the data_models and actions
# table).  These additional actions are implemented as handlers using an event
# system. The use case tests below verify that the main queries are correct and
# that more queries have been issued by the handlers. Separate tests verify
# that the correct queries have been issued by the handlers.


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_init_with_db(db):
    # Setup
    InitDB(sqlite_db).execute()
    data_model_table = DataModelTable()
    datasets_table = DatasetsTable()

    # Test

    assert data_model_table.exists(db)
    assert datasets_table.exists(db)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_is_db_initialized_with_db_fail(db):
    with pytest.raises(UserInputError):
        is_db_initialized(db=db)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_is_db_initialized_with_db_fail(sqlite_db):
    InitDB(sqlite_db).execute()
    is_db_initialized(db=sqlite_db)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_init_with_db(sqlite_db):
    # Setup
    InitDB(sqlite_db).execute()
    InitDB(sqlite_db).execute()
    data_model_table = DataModelTable()
    datasets_table = DatasetsTable()
    # Test

    assert data_model_table.exists(sqlite_db)
    assert datasets_table.exists(sqlite_db)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_re_init_with_missing_schema_with_db(sqlite_db):
    # Setup
    InitDB(sqlite_db).execute()
    data_model_table = DataModelTable()
    datasets_table = DatasetsTable()
    InitDB(sqlite_db).execute()

    assert data_model_table.exists(sqlite_db)
    assert datasets_table.exists(sqlite_db)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_re_init_with_missing_actions_table_with_db(sqlite_db):
    # Setup
    InitDB(sqlite_db).execute()
    data_model_table = DataModelTable()
    datasets_table = DatasetsTable()

    assert data_model_table.exists(sqlite_db)
    assert datasets_table.exists(sqlite_db)
    InitDB(sqlite_db).execute()

    # Test

    assert data_model_table.exists(sqlite_db)
    assert datasets_table.exists(sqlite_db)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_re_init_with_missing_data_models_table_with_db(sqlite_db):
    # Setup
    InitDB(sqlite_db).execute()
    data_model_table = DataModelTable()
    datasets_table = DatasetsTable()
    sqlite_db.execute(f"DROP TABLE data_models")

    assert not data_model_table.exists(sqlite_db)
    assert datasets_table.exists(sqlite_db)
    InitDB(sqlite_db).execute()

    # Test

    assert data_model_table.exists(sqlite_db)
    assert datasets_table.exists(sqlite_db)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_re_init_with_missing_datasets_table_with_db(sqlite_db):
    # Setup
    InitDB(sqlite_db).execute()

    data_model_table = DataModelTable()
    datasets_table = DatasetsTable()
    sqlite_db.execute(f"DROP TABLE datasets")

    assert data_model_table.exists(sqlite_db)
    assert not datasets_table.exists(sqlite_db)
    InitDB(sqlite_db).execute()

    # Test

    assert data_model_table.exists(sqlite_db)
    assert datasets_table.exists(sqlite_db)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_add_data_model_with_db(sqlite_db, monetdb, data_model_metadata):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db=sqlite_db, monetdb=monetdb).execute(
        data_model_metadata=data_model_metadata
    )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_delete_data_model_with_db(sqlite_db, monetdb, data_model_metadata):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata=data_model_metadata)

    # Test with force False
    DeleteDataModel(sqlite_db, monetdb).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        force=False,
    )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_delete_data_model_with_db_with_force(sqlite_db, monetdb, data_model_metadata):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata=data_model_metadata)

    # Test with force True
    DeleteDataModel(sqlite_db, monetdb).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        force=True,
    )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_delete_data_model_with_datasets_with_db(
    sqlite_db, monetdb, data_model_metadata
):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)

    ImportCSV(sqlite_db, monetdb).execute(
        csv_path=DATASET_FILE,
        copy_from_file=False,
        data_model_code="data_model",
        data_model_version="1.0",
    )

    # Test with force False
    with pytest.raises(ForeignKeyError):
        DeleteDataModel(sqlite_db, monetdb).execute(
            code=data_model_metadata["code"],
            version=data_model_metadata["version"],
            force=False,
        )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_delete_data_model_with_datasets_with_db_with_force(
    sqlite_db, monetdb, data_model_metadata
):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)

    ImportCSV(sqlite_db, monetdb).execute(
        csv_path=DATASET_FILE,
        copy_from_file=False,
        data_model_code="data_model",
        data_model_version="1.0",
    )

    # Test with force True
    DeleteDataModel(sqlite_db, monetdb).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        force=True,
    )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_add_dataset(sqlite_db, monetdb, data_model_metadata):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    # Test success
    ImportCSV(sqlite_db, monetdb).execute(
        csv_path=ABSOLUTE_PATH_DATASET_FILE,
        copy_from_file=True,
        data_model_code="data_model",
        data_model_version="1.0",
    )
    (code, csv_path), *_ = sqlite_db.execute_fetchall(
        f"SELECT code, csv_path FROM datasets"
    )
    assert "dataset.csv" in csv_path
    res = monetdb.execute('SELECT * FROM "data_model:1.0".primary_data')
    assert res != []


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_add_dataset_with_db_with_multiple_datasets(
    sqlite_db, monetdb, data_model_metadata
):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    # Test
    ImportCSV(sqlite_db, monetdb).execute(
        csv_path="tests/data/success/data_model_v_1_0/dataset123.csv",
        copy_from_file=False,
        data_model_code="data_model",
        data_model_version="1.0",
    )
    datasets = sqlite_db.get_values(
        table=Dataset.__table__, columns=["data_model_id", "code"]
    )
    assert len(datasets) == 3
    assert all(code in ["dataset", "dataset1", "dataset2"] for dmi, code in datasets)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_add_dataset_with_small_record_copy(sqlite_db, monetdb, data_model_metadata):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    with patch("mipdb.monetdb.monetdb_tables.RECORDS_PER_COPY", 1):
        # Test
        ImportCSV(sqlite_db, monetdb).execute(
            csv_path=DATASET_FILE,
            copy_from_file=False,
            data_model_code="data_model",
            data_model_version="1.0",
        )
    records = monetdb.execute(
        f'SELECT count(*) FROM "data_model:1.0".primary_data'
    ).fetchall()
    assert 5 == records[0][0]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_add_dataset_with_small_record_copy_with_volume(
    sqlite_db, monetdb, data_model_metadata
):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    with patch("mipdb.monetdb.monetdb_tables.RECORDS_PER_COPY", 1):
        # Test
        ImportCSV(sqlite_db, monetdb).execute(
            csv_path=ABSOLUTE_PATH_DATASET_FILE,
            copy_from_file=True,
            data_model_code="data_model",
            data_model_version="1.0",
        )
    records = monetdb.execute(
        f'SELECT count(*) FROM "data_model:1.0".primary_data'
    ).fetchall()
    assert 5 == records[0][0]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_csv_legnth_equals_records_per_copy(sqlite_db, monetdb, data_model_metadata):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    with patch("mipdb.monetdb.monetdb_tables.RECORDS_PER_COPY", 5):
        # Test
        ImportCSV(sqlite_db, monetdb).execute(
            csv_path=ABSOLUTE_PATH_DATASET_FILE,
            copy_from_file=True,
            data_model_code="data_model",
            data_model_version="1.0",
        )
    records = monetdb.execute(
        f'SELECT count(*) FROM "data_model:1.0".primary_data'
    ).fetchall()
    assert 5 == records[0][0]


def test_check_duplicate_pairs_success():
    df = pd.DataFrame({"visitid": [1, 2, 3, 4], "subjectid": [10, 20, 30, 40]})
    check_unique_longitudinal_dataset_primary_keys(df)


def test_check_duplicate_pairs_fail():
    df = pd.DataFrame(
        {"visitid": [1, 2, 3, 3, 3, 4], "subjectid": [10, 20, 20, 30, 30, 40]}
    )
    expected_output = "Invalid csv: the following visitid and subjectid pairs are duplicated:\n   visitid  subjectid\n3        3         30\n4        3         30"

    with pytest.raises(InvalidDatasetError, match=expected_output):
        check_unique_longitudinal_dataset_primary_keys(df)


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_validate_dataset(sqlite_db, monetdb, data_model_metadata):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    # Test success
    ValidateDataset(sqlite_db, monetdb).execute(
        csv_path=DATASET_FILE,
        copy_from_file=False,
        data_model_code="data_model",
        data_model_version="1.0",
    )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_delete_dataset_with_db(sqlite_db, monetdb, data_model_metadata):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    ImportCSV(sqlite_db, monetdb).execute(
        csv_path=DATASET_FILE,
        copy_from_file=False,
        data_model_code="data_model",
        data_model_version="1.0",
    )
    datasets = sqlite_db.get_values(table=Dataset.__table__, columns=["code"])
    assert len(datasets) == 1

    assert "dataset" in datasets[0][0]

    # Test
    DeleteDataset(sqlite_db, monetdb).execute(
        dataset_code="dataset",
        data_model_code=data_model_metadata["code"],
        data_model_version=data_model_metadata["version"],
    )
    datasets = sqlite_db.get_values(table=Dataset.__table__, columns=["code"])
    assert len(datasets) == 0
    assert ("dataset",) not in datasets


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_enable_data_model_with_db(sqlite_db, monetdb, data_model_metadata):
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata=data_model_metadata)
    DisableDataModel(sqlite_db).execute(
        code=data_model_metadata["code"], version=data_model_metadata["version"]
    )
    status = sqlite_db.execute_fetchall(f"SELECT status FROM data_models")
    assert status[0][0] == "DISABLED"
    EnableDataModel(sqlite_db).execute(
        code=data_model_metadata["code"], version=data_model_metadata["version"]
    )
    status = sqlite_db.execute_fetchall(f"SELECT status FROM data_models")
    assert status[0][0] == "ENABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_enable_data_model_already_enabled_with_db(
    sqlite_db, monetdb, data_model_metadata
):
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    status = sqlite_db.execute_fetchall(f"SELECT status FROM data_models")
    assert status[0][0] == "ENABLED"

    with pytest.raises(UserInputError):
        EnableDataModel(sqlite_db).execute(
            code=data_model_metadata["code"], version=data_model_metadata["version"]
        )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_disable_data_model_with_db(sqlite_db, monetdb, data_model_metadata):
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    status = sqlite_db.execute_fetchall(f"SELECT status FROM data_models")
    assert status[0][0] == "ENABLED"
    DisableDataModel(sqlite_db).execute(
        code=data_model_metadata["code"], version=data_model_metadata["version"]
    )
    status = sqlite_db.execute_fetchall(f"SELECT status FROM data_models")
    assert status[0][0] == "DISABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_disable_data_model_already_disabled_with_db(
    sqlite_db, monetdb, data_model_metadata
):
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    DisableDataModel(sqlite_db).execute(
        code=data_model_metadata["code"], version=data_model_metadata["version"]
    )
    status = sqlite_db.execute_fetchall(f"SELECT status FROM data_models")
    assert status[0][0] == "DISABLED"

    with pytest.raises(UserInputError):
        DisableDataModel(sqlite_db).execute(
            code=data_model_metadata["code"], version=data_model_metadata["version"]
        )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_enable_dataset_with_db(sqlite_db, monetdb, data_model_metadata):
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    ImportCSV(sqlite_db, monetdb).execute(
        csv_path=DATASET_FILE,
        copy_from_file=False,
        data_model_code="data_model",
        data_model_version="1.0",
    )
    DisableDataset(sqlite_db).execute(
        dataset_code="dataset",
        data_model_code=data_model_metadata["code"],
        data_model_version=data_model_metadata["version"],
    )
    status = sqlite_db.execute_fetchall(f"SELECT status FROM datasets")
    assert status[0][0] == "DISABLED"
    EnableDataset(sqlite_db).execute(
        dataset_code="dataset",
        data_model_code=data_model_metadata["code"],
        data_model_version=data_model_metadata["version"],
    )
    status = sqlite_db.execute_fetchall(f"SELECT status FROM datasets")
    assert status[0][0] == "ENABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_enable_dataset_already_enabled_with_db(
    sqlite_db, monetdb, data_model_metadata
):
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    ImportCSV(sqlite_db, monetdb).execute(
        csv_path=DATASET_FILE,
        copy_from_file=False,
        data_model_code="data_model",
        data_model_version="1.0",
    )
    status = sqlite_db.execute_fetchall(f"SELECT status FROM datasets")
    assert status[0][0] == "ENABLED"

    with pytest.raises(UserInputError):
        EnableDataset(sqlite_db).execute(
            dataset_code="dataset",
            data_model_code=data_model_metadata["code"],
            data_model_version=data_model_metadata["version"],
        )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_disable_dataset_with_db(sqlite_db, monetdb, data_model_metadata):
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    ImportCSV(sqlite_db, monetdb).execute(
        csv_path=DATASET_FILE,
        copy_from_file=False,
        data_model_code="data_model",
        data_model_version="1.0",
    )
    status = sqlite_db.execute_fetchall(f"SELECT status FROM datasets")
    assert status[0][0] == "ENABLED"
    DisableDataset(sqlite_db).execute(
        dataset_code="dataset",
        data_model_code=data_model_metadata["code"],
        data_model_version=data_model_metadata["version"],
    )
    status = sqlite_db.execute_fetchall(f"SELECT status FROM datasets")
    assert status[0][0] == "DISABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_disable_dataset_already_disabled_with_db(
    sqlite_db, monetdb, data_model_metadata
):
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    ImportCSV(sqlite_db, monetdb).execute(
        csv_path=DATASET_FILE,
        copy_from_file=False,
        data_model_code="data_model",
        data_model_version="1.0",
    )
    DisableDataset(sqlite_db).execute(
        dataset_code="dataset",
        data_model_code=data_model_metadata["code"],
        data_model_version=data_model_metadata["version"],
    )
    status = sqlite_db.execute_fetchall(f"SELECT status FROM datasets")
    assert status[0][0] == "DISABLED"

    with pytest.raises(UserInputError):
        DisableDataset(sqlite_db).execute(
            dataset_code="dataset",
            data_model_code=data_model_metadata["code"],
            data_model_version=data_model_metadata["version"],
        )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_tag_data_model_with_db(sqlite_db, monetdb, data_model_metadata):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)

    # Test
    TagDataModel(sqlite_db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        tag="tag",
    )

    properties = sqlite_db.get_data_model_properties(1)
    assert properties["tags"] == ["tag"]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_untag_data_model_with_db(sqlite_db, monetdb, data_model_metadata):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    TagDataModel(sqlite_db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        tag="tag1",
    )
    TagDataModel(sqlite_db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        tag="tag2",
    )
    TagDataModel(sqlite_db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        tag="tag3",
    )

    # Test
    UntagDataModel(sqlite_db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        tag="tag1",
    )
    properties = sqlite_db.get_data_model_properties(1)
    assert properties["tags"] == ["tag2", "tag3"]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_add_property2data_model_with_db(sqlite_db, monetdb, data_model_metadata):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)

    # Test
    AddPropertyToDataModel(sqlite_db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key",
        value="value",
        force=False,
    )

    properties = sqlite_db.get_data_model_properties(1)
    assert (
        "key" in properties["properties"] and properties["properties"]["key"] == "value"
    )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_add_property2data_model_with_force_and_db(
    sqlite_db, monetdb, data_model_metadata
):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    AddPropertyToDataModel(sqlite_db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key",
        value="value",
        force=False,
    )

    # Test
    AddPropertyToDataModel(sqlite_db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key",
        value="value1",
        force=True,
    )

    properties = sqlite_db.get_data_model_properties(1)
    assert (
        "key" in properties["properties"]
        and properties["properties"]["key"] == "value1"
    )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_remove_property_from_data_model_with_db(
    sqlite_db, monetdb, data_model_metadata
):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    AddPropertyToDataModel(sqlite_db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key1",
        value="value1",
        force=False,
    )
    AddPropertyToDataModel(sqlite_db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key2",
        value="value2",
        force=False,
    )

    # Test
    RemovePropertyFromDataModel(sqlite_db).execute(
        code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key1",
        value="value1",
    )
    properties = sqlite_db.get_data_model_properties(1)
    assert (
        "key2" in properties["properties"]
        and properties["properties"]["key2"] == "value2"
    )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_tag_dataset_with_db(sqlite_db, monetdb, data_model_metadata):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    ImportCSV(sqlite_db, monetdb).execute(DATASET_FILE, False, "data_model", "1.0")

    # Test
    TagDataset(sqlite_db).execute(
        dataset_code="dataset",
        data_model_code=data_model_metadata["code"],
        data_model_version=data_model_metadata["version"],
        tag="tag",
    )

    properties = sqlite_db.get_dataset_properties(1)
    assert properties == {"tags": ["tag"], "properties": {}}


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_untag_dataset_with_db(sqlite_db, monetdb, data_model_metadata):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    ImportCSV(sqlite_db, monetdb).execute(DATASET_FILE, False, "data_model", "1.0")
    TagDataset(sqlite_db).execute(
        dataset_code="dataset",
        data_model_code=data_model_metadata["code"],
        data_model_version=data_model_metadata["version"],
        tag="tag1",
    )
    TagDataset(sqlite_db).execute(
        dataset_code="dataset",
        data_model_code=data_model_metadata["code"],
        data_model_version=data_model_metadata["version"],
        tag="tag2",
    )
    TagDataset(sqlite_db).execute(
        dataset_code="dataset",
        data_model_code=data_model_metadata["code"],
        data_model_version=data_model_metadata["version"],
        tag="tag3",
    )

    # Test
    UntagDataset(sqlite_db).execute(
        dataset="dataset",
        data_model_code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        tag="tag1",
    )

    properties = sqlite_db.get_dataset_properties(1)
    assert properties == {"tags": ["tag2", "tag3"], "properties": {}}


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_add_property2dataset_with_db(sqlite_db, monetdb, data_model_metadata):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    ImportCSV(sqlite_db, monetdb).execute(DATASET_FILE, False, "data_model", "1.0")

    # Test
    AddPropertyToDataset(sqlite_db).execute(
        dataset="dataset",
        data_model_code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key",
        value="value",
        force=False,
    )

    properties = sqlite_db.get_dataset_properties(1)
    assert properties == {"tags": [], "properties": {"key": "value"}}


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_remove_property_from_dataset_with_db(sqlite_db, monetdb, data_model_metadata):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    ImportCSV(sqlite_db, monetdb).execute(DATASET_FILE, False, "data_model", "1.0")
    AddPropertyToDataset(sqlite_db).execute(
        dataset="dataset",
        data_model_code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key",
        value="value",
        force=False,
    )
    AddPropertyToDataset(sqlite_db).execute(
        dataset="dataset",
        data_model_code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key1",
        value="value1",
        force=False,
    )
    AddPropertyToDataset(sqlite_db).execute(
        dataset="dataset",
        data_model_code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key2",
        value="value2",
        force=False,
    )

    # Test
    RemovePropertyFromDataset(sqlite_db).execute(
        dataset="dataset",
        data_model_code=data_model_metadata["code"],
        version=data_model_metadata["version"],
        key="key2",
        value="value2",
    )
    properties = sqlite_db.get_dataset_properties(1)
    assert properties == {"tags": [], "properties": {"key": "value", "key1": "value1"}}


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_grant_select_access_rights(sqlite_db, monetdb, data_model_metadata):
    # Setup
    InitDB(sqlite_db).execute()
    AddDataModel(sqlite_db, monetdb).execute(data_model_metadata)
    ImportCSV(sqlite_db, monetdb).execute(DATASET_FILE, False, "data_model", "1.0")

    # Validation that the user 'executor' can only access data but not drop the data models table
    executor_config = {
        "ip": "localhost",
        "port": 50123,
        "dbfarm": "db",
        "username": "executor",
        "password": "executor",
    }

    db_connected_by_executor = MonetDB.from_config(executor_config)
    result = db_connected_by_executor.execute(
        f'select * from "data_model:1.0"."primary_data"'
    )
    assert result != []
    with pytest.raises(DataBaseError):
        db_connected_by_executor.execute('DROP TABLE "data_model:1.0"."primary_data"')
