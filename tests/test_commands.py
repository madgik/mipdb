import json

import pytest
from click.testing import CliRunner

from mipdb import init
from mipdb import add_data_model
from mipdb import delete_data_model
from mipdb import add_dataset
from mipdb import delete_dataset
from mipdb import disable_dataset
from mipdb import disable_data_model
from mipdb import enable_dataset
from mipdb import enable_data_model
from mipdb import load_folder
from mipdb import tag_dataset
from mipdb import tag_data_model
from mipdb import list_data_models
from mipdb import list_datasets
from mipdb import validate_dataset
from mipdb.exceptions import ExitCode
from mipdb.sqlite import Dataset, DataModel
from mipdb.sqlite_tables import DataModelTable
from tests.conftest import (
    DATASET_FILE,
    ABSOLUTE_PATH_DATASET_FILE,
    ABSOLUTE_PATH_SUCCESS_DATA_FOLDER,
    SUCCESS_DATA_FOLDER,
    ABSOLUTE_PATH_FAIL_DATA_FOLDER,
    MONETDB_OPTIONS,
    SQLiteDB_OPTION,
    ABSOLUTE_PATH_DATASET_FILE_MULTIPLE_DATASET,
)
from tests.conftest import DATA_MODEL_FILE


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_init(sqlite_db):
    # Setup
    runner = CliRunner()
    data_model_table = DataModelTable()
    assert not data_model_table.exists(sqlite_db)
    result = runner.invoke(init, SQLiteDB_OPTION)
    assert result.exit_code == ExitCode.OK
    assert sqlite_db.execute_fetchall(f"select * from data_models") == []


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_data_model(sqlite_db):
    # Setup
    runner = CliRunner()
    # Check data_model not present already

    runner.invoke(init, SQLiteDB_OPTION)
    # Test
    result = runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    assert result.exit_code == ExitCode.OK

    data_models = sqlite_db.execute_fetchall(f"select * from data_models")
    data_model_id, code, version, desc, status, properties = data_models[0]
    assert (
        data_model_id == 1
        and code == "data_model"
        and version == "1.0"
        and desc == "The Data Model"
        and status == "ENABLED"
    )
    assert properties
    properties = json.loads(properties)
    assert "tags" in properties
    assert "properties" in properties
    assert "cdes" in properties["properties"]
    cdes = properties["properties"]["cdes"]
    assert "groups" in cdes or "variables" in cdes
    assert "code" in cdes and "label" in cdes and "version" in cdes
    metadata = sqlite_db.execute_fetchall(
        f'select * from "data_model:1.0_variables_metadata"'
    )
    assert len(metadata) == 6


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_data_model(sqlite_db):
    # Setup
    runner = CliRunner()
    # Check data_model not present already

    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    assert sqlite_db.get_data_models(["data_model_id"])[0][0] == 1
    # Test
    result = runner.invoke(
        delete_data_model,
        ["data_model", "-v", "1.0", "-f"] + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )
    assert result.exit_code == ExitCode.OK


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_dataset_with_volume(sqlite_db, monetdb):
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)

    assert not sqlite_db.get_values(table=Dataset.__table__, columns=["code"])

    # Test
    result = runner.invoke(
        add_dataset,
        [
            ABSOLUTE_PATH_DATASET_FILE,
            "--data-model",
            "data_model",
            "-v",
            "1.0",
        ]
        + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )
    assert result.exit_code == ExitCode.OK

    assert (
        "dataset"
        == sqlite_db.get_values(table=Dataset.__table__, columns=["code"])[0][0]
    )

    assert result.exit_code == ExitCode.OK
    data = monetdb.execute(f'select * from "data_model:1.0".primary_data').fetchall()
    assert len(data) == 5


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_dataset(sqlite_db, monetdb):
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    assert not sqlite_db.get_values(table=Dataset.__table__, columns=["code"])

    # Test
    result = runner.invoke(
        add_dataset,
        [
            DATASET_FILE,
            "--data-model",
            "data_model",
            "-v",
            "1.0",
            "--copy_from_file",
            False,
        ]
        + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )
    assert result.exit_code == ExitCode.OK

    assert (
        "dataset"
        == sqlite_db.get_values(table=Dataset.__table__, columns=["code"])[0][0]
    )

    assert result.exit_code == ExitCode.OK
    data = monetdb.execute(f'select * from "data_model:1.0".primary_data').fetchall()
    assert len(data) == 5


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_two_datasets_with_same_name_different_data_model(sqlite_db):
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    runner.invoke(
        add_data_model,
        ["tests/data/success/data_model1_v_1_0/CDEsMetadata.json"] + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )

    # Test
    result = runner.invoke(
        add_dataset,
        [
            ABSOLUTE_PATH_SUCCESS_DATA_FOLDER + "/data_model_v_1_0/dataset10.csv",
            "--data-model",
            "data_model",
            "-v",
            "1.0",
        ]
        + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )
    result = runner.invoke(
        add_dataset,
        [
            ABSOLUTE_PATH_SUCCESS_DATA_FOLDER + "/data_model1_v_1_0/dataset10.csv",
            "--data-model",
            "data_model1",
            "-v",
            "1.0",
        ]
        + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )
    assert result.exit_code == ExitCode.OK
    assert [(1, "dataset10"), (2, "dataset10")] == sqlite_db.get_values(
        Dataset.__table__, columns=["data_model_id", "code"]
    )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_validate_dataset_with_volume(sqlite_db):
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    assert not sqlite_db.get_values(table=Dataset.__table__, columns=["code"])

    # Test
    result = runner.invoke(
        validate_dataset,
        [
            ABSOLUTE_PATH_DATASET_FILE,
            "-d",
            "data_model",
            "-v",
            "1.0",
        ]
        + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )
    assert result.exit_code == ExitCode.OK


dataset_files = [
    (
        "data_model",
        "dataset_exceeds_max.csv",
        "In the column: 'var3' the following values are invalid: '(100.0,)'",
    ),
    (
        "data_model",
        "dataset_exceeds_min.csv",
        "In the column: 'var3' the following values are invalid: '(0.0,)'",
    ),
    (
        "data_model",
        "invalid_enum.csv",
        "In the column: 'var2' the following values are invalid: '('l3',)'",
    ),
    (
        "data_model",
        "invalid_type1.csv",
        "Failed to import table 'temp', line 2: column 3 var3: 'double' expected in 'invalid'",
    ),
    (
        "data_model",
        "missing_column_dataset.csv",
        "Dataset error: The 'dataset' column is required to exist in the csv.",
    ),
    (
        "data_model",
        "column_not_present_in_cdes.csv",
        "Columns:{'non_existing_col'} are not present in the CDEs",
    ),
    (
        "data_model_longitudinal",
        "dataset.csv",
        """Dataset error: Invalid csv: the following visitid and subjectid pairs are duplicated:
    subjectid visitid
1  subjectid2     FL1
2  subjectid2     FL1""",
    ),
]


@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
@pytest.mark.parametrize("data_model,dataset,exception_message", dataset_files)
def test_invalid_dataset_error_cases(data_model, dataset, exception_message):
    runner = CliRunner()

    runner.invoke(init, SQLiteDB_OPTION)
    result = runner.invoke(
        add_data_model,
        [
            ABSOLUTE_PATH_FAIL_DATA_FOLDER
            + "/"
            + data_model
            + "_v_1_0/CDEsMetadata.json",
        ]
        + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )
    assert result.exit_code == ExitCode.OK

    validation_result = runner.invoke(
        validate_dataset,
        [
            ABSOLUTE_PATH_FAIL_DATA_FOLDER + "/" + data_model + "_v_1_0/" + dataset,
            "-d",
            data_model,
            "-v",
            "1.0",
        ]
        + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )

    assert (
        validation_result.exception.__str__() == exception_message
        or exception_message in validation_result.stdout
    )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_validate_dataset(sqlite_db):
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    assert not sqlite_db.get_values(table=Dataset.__table__, columns=["code"])

    # Test
    result = runner.invoke(
        validate_dataset,
        [
            DATASET_FILE,
            "-d",
            "data_model",
            "-v",
            "1.0",
            "--copy_from_file",
            False,
        ]
        + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )
    assert result.exit_code == ExitCode.OK


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_dataset_with_volume(sqlite_db):
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    runner.invoke(
        add_dataset,
        [
            ABSOLUTE_PATH_DATASET_FILE,
            "--data-model",
            "data_model",
            "-v",
            "1.0",
        ]
        + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )
    assert (
        "dataset"
        == sqlite_db.get_values(table=Dataset.__table__, columns=["code"])[0][0]
    )

    # Test
    result = runner.invoke(
        delete_dataset,
        ["dataset", "-d", "data_model", "-v", "1.0"] + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )
    assert result.exit_code == ExitCode.OK

    assert not sqlite_db.get_values(table=Dataset.__table__, columns=["code"])


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_load_folder_with_volume(sqlite_db, monetdb):
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    result = runner.invoke(init, SQLiteDB_OPTION)
    assert not sqlite_db.get_values(table=Dataset.__table__, columns=["code"])

    # Test
    result = runner.invoke(
        load_folder, [ABSOLUTE_PATH_SUCCESS_DATA_FOLDER] + SQLiteDB_OPTION + MONETDB_OPTIONS
    )
    assert result.exit_code == ExitCode.OK

    datasets = sqlite_db.get_values(table=Dataset.__table__, columns=["code"])
    dataset_codes = [code for code, *_ in datasets]
    expected = [
        "dataset",
        "dataset1",
        "dataset2",
        "dataset10",
        "dataset20",
        "dataset_longitudinal",
    ]
    assert set(expected) == set(dataset_codes)
    row_ids = monetdb.execute(
        f'select row_id from "data_model:1.0".primary_data'
    ).fetchall()
    assert list(range(1, len(row_ids) + 1)) == [row[0] for row in row_ids]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_load_folder(sqlite_db, monetdb):
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    result = runner.invoke(init, SQLiteDB_OPTION)
    assert not sqlite_db.get_values(table=Dataset.__table__, columns=["code"])

    # Test
    result = runner.invoke(
        load_folder,
        [SUCCESS_DATA_FOLDER, "--copy_from_file", False] + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )
    assert result.exit_code == ExitCode.OK

    datasets = sqlite_db.get_values(table=Dataset.__table__, columns=["code"])
    dataset_codes = [code for code, *_ in datasets]
    expected = [
        "dataset",
        "dataset1",
        "dataset2",
        "dataset10",
        "dataset20",
        "dataset_longitudinal",
    ]
    assert set(expected) == set(dataset_codes)
    row_ids = monetdb.execute(
        f'select row_id from "data_model:1.0".primary_data'
    ).fetchall()
    assert list(range(1, len(row_ids) + 1)) == [row[0] for row in row_ids]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_load_folder_twice_with_volume(sqlite_db, monetdb):
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    result = runner.invoke(init, SQLiteDB_OPTION)
    assert not sqlite_db.get_values(table=Dataset.__table__, columns=["code"])
    result = runner.invoke(
        load_folder, [ABSOLUTE_PATH_SUCCESS_DATA_FOLDER] + SQLiteDB_OPTION + MONETDB_OPTIONS
    )
    assert result.exit_code == ExitCode.OK

    # Test
    result = runner.invoke(
        load_folder, [ABSOLUTE_PATH_SUCCESS_DATA_FOLDER] + SQLiteDB_OPTION + MONETDB_OPTIONS
    )
    assert result.exit_code == ExitCode.OK

    datasets = sqlite_db.get_values(table=Dataset.__table__, columns=["code"])
    dataset_codes = [dataset[0] for dataset in datasets]
    expected = [
        "dataset",
        "dataset1",
        "dataset2",
        "dataset10",
        "dataset20",
        "dataset_longitudinal",
    ]
    assert set(expected) == set(dataset_codes)
    row_ids = monetdb.execute(
        f'select row_id from "data_model:1.0".primary_data'
    ).fetchall()
    assert list(range(1, len(row_ids) + 1)) == [row[0] for row in row_ids]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_tag_data_model(sqlite_db):
    # Setup
    runner = CliRunner()

    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)

    # Test
    result = runner.invoke(
        tag_data_model,
        ["data_model", "-t", "tag", "-v", "1.0"] + SQLiteDB_OPTION,
    )
    assert result.exit_code == ExitCode.OK
    result = sqlite_db.get_values(table=DataModel.__table__, columns=["properties"])
    properties = result[0][0]
    assert properties["tags"] == ["tag"]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_untag_data_model(sqlite_db):
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    runner.invoke(
        tag_data_model,
        ["data_model", "-t", "tag", "-v", "1.0"] + SQLiteDB_OPTION,
    )

    # Test
    result = runner.invoke(
        tag_data_model,
        ["data_model", "-t", "tag", "-v", "1.0", "-r"] + SQLiteDB_OPTION,
    )
    assert result.exit_code == ExitCode.OK
    result = sqlite_db.get_values(table=DataModel.__table__, columns=["properties"])
    properties = result[0][0]
    assert properties["tags"] == []


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_property_data_model_addition(sqlite_db):
    # Setup
    runner = CliRunner()

    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)

    # Test
    result = runner.invoke(
        tag_data_model,
        ["data_model", "-t", "key=value", "-v", "1.0"] + SQLiteDB_OPTION,
    )
    assert result.exit_code == ExitCode.OK
    result = sqlite_db.get_values(table=DataModel.__table__, columns=["properties"])
    properties = result[0][0]
    assert (
        "key" in properties["properties"] and properties["properties"]["key"] == "value"
    )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_property_data_model_deletion(sqlite_db):
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    runner.invoke(
        tag_data_model,
        ["data_model", "-t", "key=value", "-v", "1.0"] + SQLiteDB_OPTION,
    )

    # Test
    result = runner.invoke(
        tag_data_model,
        [
            "data_model",
            "-t",
            "key=value",
            "-v",
            "1.0",
            "-r",
        ]
        + SQLiteDB_OPTION,
    )
    assert result.exit_code == ExitCode.OK
    result = sqlite_db.get_values(table=DataModel.__table__, columns=["properties"])
    properties = result[0][0]
    assert "key" not in properties["properties"]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_tag_dataset(sqlite_db):
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    runner.invoke(
        add_dataset,
        [
            DATASET_FILE,
            "--data-model",
            "data_model",
            "-v",
            "1.0",
            "--copy_from_file",
            False,
        ]
        + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )

    # Test
    result = runner.invoke(
        tag_dataset,
        [
            "dataset",
            "-t",
            "tag",
            "-d",
            "data_model",
            "-v",
            "1.0",
        ]
        + SQLiteDB_OPTION,
    )
    assert result.exit_code == ExitCode.OK
    properties = sqlite_db.get_values(table=Dataset.__table__, columns=["properties"])

    assert {"tags": ["tag"], "properties": {}} == properties[0][0]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_untag_dataset(sqlite_db):
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    runner.invoke(init, SQLiteDB_OPTION)
    result = runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    assert result.exit_code == ExitCode.OK

    result = runner.invoke(
        add_dataset,
        [
            DATASET_FILE,
            "-d",
            "data_model",
            "-v",
            "1.0",
            "--copy_from_file",
            False,
        ]
        + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )
    assert result.exit_code == ExitCode.OK
    result = runner.invoke(
        tag_dataset,
        [
            "dataset",
            "-t",
            "tag",
            "-d",
            "data_model",
            "-v",
            "1.0",
        ]
        + SQLiteDB_OPTION,
    )
    assert result.exit_code == ExitCode.OK

    # Test
    result = runner.invoke(
        tag_dataset,
        [
            "dataset",
            "-t",
            "tag",
            "-d",
            "data_model",
            "-v",
            "1.0",
            "-r",
        ]
        + SQLiteDB_OPTION,
    )
    assert result.exit_code == ExitCode.OK
    properties = sqlite_db.get_values(table=Dataset.__table__, columns=["properties"])

    assert {"tags": [], "properties": {}} == properties[0][0]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_property_dataset_addition(sqlite_db):
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    runner.invoke(
        add_dataset,
        [
            DATASET_FILE,
            "-d",
            "data_model",
            "-v",
            "1.0",
            "--copy_from_file",
            False,
        ]
        + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )

    # Test
    result = runner.invoke(
        tag_dataset,
        [
            "dataset",
            "-t",
            "key=value",
            "-d",
            "data_model",
            "-v",
            "1.0",
        ]
        + SQLiteDB_OPTION,
    )
    assert result.exit_code == ExitCode.OK
    properties = sqlite_db.get_values(table=Dataset.__table__, columns=["properties"])

    assert {"tags": [], "properties": {"key": "value"}} == properties[0][0]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_property_dataset_deletion(sqlite_db):
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    runner.invoke(init, SQLiteDB_OPTION)
    result = runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    assert result.exit_code == ExitCode.OK

    result = runner.invoke(
        add_dataset,
        [
            DATASET_FILE,
            "-d",
            "data_model",
            "-v",
            "1.0",
            "--copy_from_file",
            False,
        ]
        + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )
    assert result.exit_code == ExitCode.OK
    result = runner.invoke(
        tag_dataset,
        [
            "dataset",
            "-t",
            "key=value",
            "-d",
            "data_model",
            "-v",
            "1.0",
        ]
        + SQLiteDB_OPTION,
    )
    assert result.exit_code == ExitCode.OK

    # Test
    result = runner.invoke(
        tag_dataset,
        [
            "dataset",
            "-t",
            "key=value",
            "-d",
            "data_model",
            "-v",
            "1.0",
            "-r",
        ]
        + SQLiteDB_OPTION,
    )
    assert result.exit_code == ExitCode.OK
    properties = sqlite_db.get_values(table=Dataset.__table__, columns=["properties"])

    assert {"tags": [], "properties": {}} == properties[0][0]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_enable_data_model(sqlite_db):
    # Setup
    runner = CliRunner()

    # Check status is disabled
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    result = runner.invoke(
        disable_data_model, ["data_model", "-v", "1.0"] + SQLiteDB_OPTION
    )
    assert _get_status(sqlite_db, "data_models") == "DISABLED"

    # Test
    result = runner.invoke(
        enable_data_model, ["data_model", "-v", "1.0"] + SQLiteDB_OPTION
    )
    assert result.exit_code == ExitCode.OK
    assert _get_status(sqlite_db, "data_models") == "ENABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_disable_data_model(sqlite_db):
    # Setup
    runner = CliRunner()

    # Check status is enabled
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    assert _get_status(sqlite_db, "data_models") == "ENABLED"

    # Test
    result = runner.invoke(
        disable_data_model, ["data_model", "-v", "1.0"] + SQLiteDB_OPTION
    )
    assert result.exit_code == ExitCode.OK
    assert _get_status(sqlite_db, "data_models") == "DISABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_enable_dataset(sqlite_db):
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    runner.invoke(
        add_dataset,
        [
            DATASET_FILE,
            "-d",
            "data_model",
            "-v",
            "1.0",
            "--copy_from_file",
            False,
        ]
        + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )
    result = runner.invoke(
        disable_dataset,
        ["dataset", "-d", "data_model", "-v", "1.0"] + SQLiteDB_OPTION,
    )
    assert _get_status(sqlite_db, "datasets") == "DISABLED"

    # Test
    result = runner.invoke(
        enable_dataset,
        ["dataset", "-d", "data_model", "-v", "1.0"] + SQLiteDB_OPTION,
    )
    assert result.exit_code == ExitCode.OK
    assert _get_status(sqlite_db, "datasets") == "ENABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_disable_dataset(sqlite_db):
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    runner.invoke(
        add_dataset,
        [
            DATASET_FILE,
            "-d",
            "data_model",
            "-v",
            "1.0",
            "--copy_from_file",
            False,
        ]
        + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )
    assert _get_status(sqlite_db, "datasets") == "ENABLED"

    # Test
    result = runner.invoke(
        disable_dataset,
        ["dataset", "-d", "data_model", "-v", "1.0"] + SQLiteDB_OPTION,
    )
    assert _get_status(sqlite_db, "datasets") == "DISABLED"
    assert result.exit_code == ExitCode.OK


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_list_data_models():
    # Setup
    runner = CliRunner()

    # Check data_model not present already

    runner.invoke(init, SQLiteDB_OPTION)
    result = runner.invoke(list_data_models, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    result_with_data_model = runner.invoke(list_data_models, SQLiteDB_OPTION)
    runner.invoke(
        add_dataset,
        [
            DATASET_FILE,
            "--data-model",
            "data_model",
            "-v",
            "1.0",
            "--copy_from_file",
            False,
        ]
        + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )
    result_with_data_model_and_dataset = runner.invoke(list_data_models, SQLiteDB_OPTION)

    # Test
    assert result.exit_code == ExitCode.OK
    assert result.stdout == "There are no data models.\n"
    assert result_with_data_model.exit_code == ExitCode.OK
    assert (
        "data_model_id        code version           label   status  count"
        in result_with_data_model.stdout
    )
    assert (
        "0              1  data_model     1.0  The Data Model  ENABLED      0"
        in result_with_data_model.stdout
    )
    assert result_with_data_model_and_dataset.exit_code == ExitCode.OK
    assert (
        "data_model_id        code version           label   status  count"
        in result_with_data_model_and_dataset.stdout
    )
    assert (
        "0              1  data_model     1.0  The Data Model  ENABLED      1"
        in result_with_data_model_and_dataset.stdout
    )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_list_datasets():
    # Setup
    runner = CliRunner()

    # Check dataset not present already
    runner.invoke(init, SQLiteDB_OPTION)
    runner.invoke(add_data_model, [DATA_MODEL_FILE] + SQLiteDB_OPTION + MONETDB_OPTIONS)
    result = runner.invoke(list_datasets, SQLiteDB_OPTION + MONETDB_OPTIONS)
    runner.invoke(
        add_dataset,
        [
            ABSOLUTE_PATH_DATASET_FILE_MULTIPLE_DATASET,
            "--data-model",
            "data_model",
            "-v",
            "1.0",
            "--copy_from_file",
            True,
        ]
        + SQLiteDB_OPTION + MONETDB_OPTIONS,
    )
    result_with_dataset = runner.invoke(list_datasets, SQLiteDB_OPTION + MONETDB_OPTIONS)

    # Test
    assert result.exit_code == ExitCode.OK
    assert result.stdout == "There are no datasets.\n"
    assert result_with_dataset.exit_code == ExitCode.OK
    assert "dataset_id  data_model_id      code      label   status  count".strip(
        " "
    ) in result_with_dataset.stdout.strip(" ")
    assert "dataset2  Dataset 2  ENABLED      2".strip(
        " "
    ) in result_with_dataset.stdout.strip(" ")
    assert "dataset1  Dataset 1  ENABLED      2".strip(
        " "
    ) in result_with_dataset.stdout.strip(" ")
    assert "dataset    Dataset  ENABLED      1".strip(
        " "
    ) in result_with_dataset.stdout.strip(" ")


def _get_status(db, schema_name):
    (status, *_), *_ = db.execute_fetchall(f"SELECT status FROM {schema_name}")
    return status
