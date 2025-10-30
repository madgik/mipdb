import json

import pytest
from click.testing import CliRunner
from mipdb.exceptions import ExitCode
from mipdb.sqlite.sqlite import Dataset, DataModel
from mipdb.sqlite.sqlite_tables import DataModelTable
from mipdb.commands import entry as cli
from tests.conftest import (
    DATASET_FILE,
    ABSOLUTE_PATH_DATASET_FILE,
    ABSOLUTE_PATH_SUCCESS_DATA_FOLDER,
    SUCCESS_DATA_FOLDER,
    ABSOLUTE_PATH_FAIL_DATA_FOLDER,
    MONETDB_OPTIONS,
    SQLiteDB_OPTION,
    ABSOLUTE_PATH_DATASET_FILE_MULTIPLE_DATASET,
    NO_MONETDB_OPTIONS,
)
from tests.conftest import DATA_MODEL_FILE
import pytest


def _bootstrap_data_model(sqlite_db):
    run_cli_command("init")
    # Test
    result = run_cli_command("add-data-model", DATA_MODEL_FILE)
    assert result.exit_code == ExitCode.OK


def _bootstrap_dataset(sqlite_db):
    _bootstrap_data_model(sqlite_db)
    res = run_cli_command(
        "add-dataset",
        DATASET_FILE,
        "-d",
        "data_model",
        "-v",
        "1.0",
        "--no-copy",
    )
    assert res.exit_code == ExitCode.OK
    assert _get_status(sqlite_db, "datasets") == "ENABLED"


def _bootstrap_dataset_without_monetdb(sqlite_db):
    run_cli_command_without_monetdb("init")
    run_cli_command_without_monetdb("add-data-model", DATA_MODEL_FILE)
    res = run_cli_command_without_monetdb(
        "add-dataset",
        DATASET_FILE,
        "-d",
        "data_model",
        "-v",
        "1.0",
        "--no-copy",
    )
    assert res.exit_code == ExitCode.OK
    assert _get_status(sqlite_db, "datasets") == "ENABLED"


def _bootstrap_disabled_dataset(sqlite_db):
    _bootstrap_dataset(sqlite_db)
    res = run_cli_command(
        "disable-dataset",
        "dataset",
        "-d",
        "data_model",
        "-v",
        "1.0",
    )
    assert res.exit_code == ExitCode.OK
    assert _get_status(sqlite_db, "datasets") == "DISABLED"


def run_cli_command(command_name, *args):
    runner = CliRunner()
    return runner.invoke(cli, [*SQLiteDB_OPTION, *MONETDB_OPTIONS, command_name, *args])


def run_cli_command_without_monetdb(command_name, *args):
    runner = CliRunner()
    return runner.invoke(
        cli, [*SQLiteDB_OPTION, *NO_MONETDB_OPTIONS, command_name, *args]
    )


@pytest.mark.database
@pytest.mark.usefixtures("cleanup_sqlite")
def test_init(sqlite_db):

    data_model_table = DataModelTable()
    assert not data_model_table.exists(sqlite_db)
    result = run_cli_command("init")
    assert result.exit_code == ExitCode.OK
    assert sqlite_db.execute_fetchall(f"select * from data_models") == []


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_add_data_model(sqlite_db):

    # Check data_model not present already

    _bootstrap_data_model(sqlite_db)

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
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_delete_data_model(sqlite_db):

    # Check data_model not present already

    _bootstrap_data_model(sqlite_db)
    assert sqlite_db.get_data_models(["data_model_id"])[0]["data_model_id"] == 1
    # Test
    result = run_cli_command("delete-data-model", "data_model", "-v", "1.0", "-f")

    assert result.exit_code == ExitCode.OK


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_add_dataset_with_volume(sqlite_db, monetdb):

    # Check dataset not present already
    _bootstrap_data_model(sqlite_db)

    assert not sqlite_db.get_values(table=Dataset.__table__, columns=["code"])

    # Test
    result = run_cli_command(
        "add-dataset",
        ABSOLUTE_PATH_DATASET_FILE,
        "--data-model",
        "data_model",
        "-v",
        "1.0",
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
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_add_dataset(sqlite_db, monetdb):

    # Check dataset not present already
    _bootstrap_data_model(sqlite_db)
    assert not sqlite_db.get_values(table=Dataset.__table__, columns=["code"])

    # Test
    result = run_cli_command(
        "add-dataset",
        DATASET_FILE,
        "--data-model",
        "data_model",
        "-v",
        "1.0",
        "--no-copy",
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
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_add_two_datasets_with_same_name_different_data_model(sqlite_db):

    # Check dataset not present already
    _bootstrap_data_model(sqlite_db)
    run_cli_command(
        "add-data-model", "tests/data/success/data_model1_v_1_0/CDEsMetadata.json"
    )

    # Test
    run_cli_command(
        "add-dataset",
        ABSOLUTE_PATH_SUCCESS_DATA_FOLDER + "/data_model_v_1_0/dataset10.csv",
        "--data-model",
        "data_model",
        "-v",
        "1.0",
    )
    result = run_cli_command(
        "add-dataset",
        ABSOLUTE_PATH_SUCCESS_DATA_FOLDER + "/data_model1_v_1_0/dataset10.csv",
        "--data-model",
        "data_model1",
        "-v",
        "1.0",
    )

    assert result.exit_code == ExitCode.OK
    assert [(1, "dataset10"), (2, "dataset10")] == sqlite_db.get_values(
        Dataset.__table__, columns=["data_model_id", "code"]
    )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_validate_dataset_with_volume(sqlite_db):

    # Check dataset not present already
    _bootstrap_data_model(sqlite_db)
    assert not sqlite_db.get_values(table=Dataset.__table__, columns=["code"])

    # Test

    result = run_cli_command(
        "validate-dataset", ABSOLUTE_PATH_DATASET_FILE, "-d", "data_model", "-v", "1.0"
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


@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
@pytest.mark.parametrize("data_model,dataset,exception_message", dataset_files)
def test_invalid_dataset_error_cases(data_model, dataset, exception_message):
    run_cli_command("init")

    result = run_cli_command(
        "add-data-model",
        ABSOLUTE_PATH_FAIL_DATA_FOLDER + "/" + data_model + "_v_1_0/CDEsMetadata.json",
    )

    assert result.exit_code == ExitCode.OK
    validation_result = run_cli_command(
        "validate-dataset",
        ABSOLUTE_PATH_FAIL_DATA_FOLDER + "/" + data_model + "_v_1_0/" + dataset,
        "-d",
        data_model,
        "-v",
        "1.0",
    )

    assert (
        validation_result.exception.__str__() == exception_message
        or exception_message in validation_result.stdout
    )


def test_validate_no_db():
    validation_result = run_cli_command(
        "validate-folder", ABSOLUTE_PATH_FAIL_DATA_FOLDER
    )
    assert validation_result.exit_code != ExitCode.OK


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_validate_dataset(sqlite_db):

    # Check dataset not present already
    _bootstrap_data_model(sqlite_db)
    assert not sqlite_db.get_values(table=Dataset.__table__, columns=["code"])

    # Test
    result = run_cli_command(
        "validate-dataset", DATASET_FILE, "-d", "data_model", "-v", "1.0", "--no-copy"
    )

    assert result.exit_code == ExitCode.OK


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_delete_dataset_with_volume(sqlite_db):

    # Check dataset not present already
    _bootstrap_data_model(sqlite_db)
    run_cli_command(
        "add-dataset",
        ABSOLUTE_PATH_DATASET_FILE,
        "--data-model",
        "data_model",
        "-v",
        "1.0",
    )
    assert (
        "dataset"
        == sqlite_db.get_values(table=Dataset.__table__, columns=["code"])[0][0]
    )

    # Test
    result = run_cli_command(
        "delete-dataset", "dataset", "-d", "data_model", "-v", "1.0"
    )
    assert result.exit_code == ExitCode.OK

    assert not sqlite_db.get_values(table=Dataset.__table__, columns=["code"])


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_load_folder_with_volume(sqlite_db, monetdb):

    # Check dataset not present already
    result = run_cli_command("init")
    assert not sqlite_db.get_values(table=Dataset.__table__, columns=["code"])

    # Test

    result = run_cli_command(
        "load-folder",
        ABSOLUTE_PATH_SUCCESS_DATA_FOLDER,
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
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_load_folder(sqlite_db, monetdb):

    # Check dataset not present already
    result = run_cli_command("init")
    assert not sqlite_db.get_values(table=Dataset.__table__, columns=["code"])

    # Test
    result = run_cli_command("load-folder", SUCCESS_DATA_FOLDER, "--no-copy")
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


@pytest.mark.usefixtures("cleanup_sqlite")
def test_load_folder_no_monetdb(sqlite_db, monetdb):

    # Check dataset not present already
    result = run_cli_command_without_monetdb("init")
    assert not sqlite_db.get_values(table=Dataset.__table__, columns=["code"])

    # Test
    result = run_cli_command_without_monetdb("load-folder", SUCCESS_DATA_FOLDER)
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


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_load_folder_monetdb_deployed_not_used_monetdb(sqlite_db, monetdb):

    # Check dataset not present already
    result = run_cli_command_without_monetdb("init")
    assert not sqlite_db.get_values(table=Dataset.__table__, columns=["code"])

    # Test
    result = run_cli_command_without_monetdb("load-folder", SUCCESS_DATA_FOLDER)
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


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_load_folder_twice_with_volume(sqlite_db, monetdb):

    # Check dataset not present already
    result = run_cli_command("init")
    assert not sqlite_db.get_values(table=Dataset.__table__, columns=["code"])
    result = run_cli_command("load-folder", ABSOLUTE_PATH_SUCCESS_DATA_FOLDER)

    assert result.exit_code == ExitCode.OK

    # Test
    result = run_cli_command("load-folder", ABSOLUTE_PATH_SUCCESS_DATA_FOLDER)
    assert result.exit_code == ExitCode.OK

    datasets = sqlite_db.get_values(
        table=Dataset.__table__, columns=["code", "properties"]
    )
    expected = [
        (
            "dataset20",
            {
                "tags": [],
                "properties": {
                    "variables": ["subjectcode", "var2", "var3", "var4", "dataset"]
                },
            },
        ),
        (
            "dataset10",
            {
                "tags": [],
                "properties": {"variables": ["subjectcode", "var1", "var3", "dataset"]},
            },
        ),
        (
            "dataset10",
            {
                "tags": [],
                "properties": {"variables": ["subjectcode", "var1", "var3", "dataset"]},
            },
        ),
        (
            "dataset",
            {
                "tags": [],
                "properties": {
                    "variables": [
                        "subjectcode",
                        "var1",
                        "var2",
                        "var3",
                        "var4",
                        "dataset",
                    ]
                },
            },
        ),
        (
            "dataset2",
            {
                "tags": [],
                "properties": {
                    "variables": ["subjectcode", "var2", "var3", "var4", "dataset"]
                },
            },
        ),
        (
            "dataset1",
            {
                "tags": [],
                "properties": {
                    "variables": ["subjectcode", "var2", "var3", "var4", "dataset"]
                },
            },
        ),
        (
            "dataset_longitudinal",
            {
                "tags": [],
                "properties": {"variables": ["subjectid", "visitid", "dataset"]},
            },
        ),
    ]
    assert expected == datasets
    row_ids = monetdb.execute(
        f'select row_id from "data_model:1.0".primary_data'
    ).fetchall()
    assert list(range(1, len(row_ids) + 1)) == [row[0] for row in row_ids]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_tag_data_model(sqlite_db):

    _bootstrap_data_model(sqlite_db)

    # Test
    result = run_cli_command("tag-data-model", "data_model", "-t", "tag", "-v", "1.0")
    assert result.exit_code == ExitCode.OK
    result = sqlite_db.get_values(table=DataModel.__table__, columns=["properties"])
    properties = result[0][0]
    assert properties["tags"] == ["tag"]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_untag_data_model(sqlite_db):

    # Check dataset not present already
    _bootstrap_data_model(sqlite_db)
    result = run_cli_command("tag-data-model", "data_model", "-t", "tag", "-v", "1.0")

    # Test
    result = run_cli_command(
        "tag-data-model", "data_model", "-t", "tag", "-v", "1.0", "-r"
    )
    assert result.exit_code == ExitCode.OK
    result = sqlite_db.get_values(table=DataModel.__table__, columns=["properties"])
    properties = result[0][0]
    assert properties["tags"] == []


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_property_data_model_addition(sqlite_db):

    _bootstrap_data_model(sqlite_db)

    # Test
    result = run_cli_command(
        "tag-data-model", "data_model", "-t", "key=value", "-v", "1.0"
    )
    assert result.exit_code == ExitCode.OK
    result = sqlite_db.get_values(table=DataModel.__table__, columns=["properties"])
    properties = result[0][0]
    assert (
        "key" in properties["properties"] and properties["properties"]["key"] == "value"
    )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_property_data_model_deletion(sqlite_db):

    # Check dataset not present already
    _bootstrap_data_model(sqlite_db)
    run_cli_command("tag-data-model", "data_model", "-t", "key=value", "-v", "1.0")

    # Test
    result = run_cli_command(
        "tag-data-model", "data_model", "-t", "key=value", "-v", "1.0", "-r"
    )

    assert result.exit_code == ExitCode.OK
    result = sqlite_db.get_values(table=DataModel.__table__, columns=["properties"])
    properties = result[0][0]
    assert "key" not in properties["properties"]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_tag_dataset(sqlite_db):

    # Check dataset not present already
    _bootstrap_data_model(sqlite_db)
    run_cli_command(
        "add-dataset",
        DATASET_FILE,
        "--data-model",
        "data_model",
        "-v",
        "1.0",
        "--no-copy",
    )

    # Test
    result = run_cli_command(
        "tag-dataset", "dataset", "-t", "tag", "-d", "data_model", "-v", "1.0"
    )
    assert result.exit_code == ExitCode.OK
    properties = sqlite_db.get_values(table=Dataset.__table__, columns=["properties"])

    assert {
        "tags": ["tag"],
        "properties": {
            "variables": ["subjectcode", "var1", "var2", "var3", "var4", "dataset"]
        },
    } == properties[0][0]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_untag_dataset(sqlite_db):

    # Check dataset not present already
    run_cli_command("init")
    run_cli_command("add-data-model", DATA_MODEL_FILE)
    result = run_cli_command(
        "add-dataset",
        DATASET_FILE,
        "--data-model",
        "data_model",
        "-v",
        "1.0",
        "--no-copy",
    )

    assert result.exit_code == ExitCode.OK
    result = run_cli_command(
        "tag-dataset", "dataset", "-t", "tag", "-d", "data_model", "-v", "1.0"
    )
    assert result.exit_code == ExitCode.OK

    # Test
    result = run_cli_command(
        "tag-dataset", "dataset", "-t", "tag", "-d", "data_model", "-v", "1.0", "-r"
    )
    assert result.exit_code == ExitCode.OK
    properties = sqlite_db.get_values(table=Dataset.__table__, columns=["properties"])

    assert {
        "tags": [],
        "properties": {
            "variables": ["subjectcode", "var1", "var2", "var3", "var4", "dataset"]
        },
    } == properties[0][0]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_property_dataset_addition(sqlite_db):

    # Check dataset not present already
    _bootstrap_data_model(sqlite_db)
    run_cli_command(
        "add-dataset",
        DATASET_FILE,
        "--data-model",
        "data_model",
        "-v",
        "1.0",
        "--no-copy",
    )

    # Test
    result = run_cli_command(
        "tag-dataset", "dataset", "-t", "key=value", "-d", "data_model", "-v", "1.0"
    )
    assert result.exit_code == ExitCode.OK
    properties = sqlite_db.get_values(table=Dataset.__table__, columns=["properties"])

    assert {
        "tags": [],
        "properties": {
            "variables": ["subjectcode", "var1", "var2", "var3", "var4", "dataset"],
            "key": "value",
        },
    } == properties[0][0]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_property_dataset_deletion(sqlite_db):

    # Check dataset not present already
    _bootstrap_dataset(sqlite_db)
    result = run_cli_command(
        "tag-dataset", "dataset", "-t", "key=value", "-d", "data_model", "-v", "1.0"
    )
    assert result.exit_code == ExitCode.OK

    # Test
    result = run_cli_command(
        "tag-dataset",
        "dataset",
        "-t",
        "key=value",
        "-d",
        "data_model",
        "-v",
        "1.0",
        "-r",
    )
    assert result.exit_code == ExitCode.OK
    properties = sqlite_db.get_values(table=Dataset.__table__, columns=["properties"])

    assert {
        "tags": [],
        "properties": {
            "variables": ["subjectcode", "var1", "var2", "var3", "var4", "dataset"]
        },
    } == properties[0][0]


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_enable_data_model(sqlite_db):

    # Check status is disabled
    _bootstrap_data_model(sqlite_db)
    run_cli_command("disable-data-model", "data_model", "-v", "1.0")
    assert _get_status(sqlite_db, "data_models") == "DISABLED"

    # Test
    result = run_cli_command("enable-data-model", "data_model", "-v", "1.0")
    assert result.exit_code == ExitCode.OK
    assert _get_status(sqlite_db, "data_models") == "ENABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_disable_data_model(sqlite_db):
    # Check status is enabled
    _bootstrap_data_model(sqlite_db)
    assert _get_status(sqlite_db, "data_models") == "ENABLED"

    # Test
    result = run_cli_command("disable-data-model", "data_model", "-v", "1.0")
    assert result.exit_code == ExitCode.OK
    assert _get_status(sqlite_db, "data_models") == "DISABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_enable_dataset(sqlite_db):
    _bootstrap_disabled_dataset(sqlite_db)
    result = run_cli_command(
        "enable-dataset",
        "dataset",
        "-d",
        "data_model",
        "-v",
        "1.0",
    )

    assert result.exit_code == ExitCode.OK
    assert _get_status(sqlite_db, "datasets") == "ENABLED"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_disable_dataset(sqlite_db):

    # Check dataset not present already
    _bootstrap_dataset(sqlite_db)

    # Test
    result = run_cli_command(
        "disable-dataset",
        "dataset",
        "-d",
        "data_model",
        "-v",
        "1.0",
    )
    assert _get_status(sqlite_db, "datasets") == "DISABLED"
    assert result.exit_code == ExitCode.OK


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_list_data_models():

    # Check data_model not present already

    run_cli_command("init")
    result = run_cli_command("list-data-models")
    run_cli_command("add-data-model", DATA_MODEL_FILE)
    result_with_data_model = run_cli_command("list-data-models")
    run_cli_command(
        "add-dataset",
        DATASET_FILE,
        "-d",
        "data_model",
        "-v",
        "1.0",
        "--no-copy",
    )

    result_with_data_model_and_dataset = run_cli_command("list-data-models")
    # Test
    assert result.exit_code == ExitCode.OK
    assert result.stdout == "There are no data models.\n"
    assert result_with_data_model.exit_code == ExitCode.OK
    assert (
        "data_model_id        code version           label   status"
        in result_with_data_model.stdout
    )
    assert (
        "0              1  data_model     1.0  The Data Model  ENABLED"
        in result_with_data_model.stdout
    )
    assert result_with_data_model_and_dataset.exit_code == ExitCode.OK
    assert (
        "data_model_id        code version           label   status"
        in result_with_data_model_and_dataset.stdout
    )
    assert (
        "0              1  data_model     1.0  The Data Model  ENABLED"
        in result_with_data_model_and_dataset.stdout
    )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_monetdb", "cleanup_sqlite")
def test_list_datasets(sqlite_db):
    # Check dataset not present already
    _bootstrap_data_model(sqlite_db)
    result = run_cli_command("list-datasets")
    run_cli_command(
        "add-dataset",
        ABSOLUTE_PATH_DATASET_FILE_MULTIPLE_DATASET,
        "--data-model",
        "data_model",
        "-v",
        "1.0",
        "--no-copy",
    )
    result_with_dataset = run_cli_command("list-datasets")

    # Test
    assert result.exit_code == ExitCode.OK
    assert result.stdout == "There are no datasets.\n"
    assert result_with_dataset.exit_code == ExitCode.OK
    assert "dataset_id  data_model_id      code      label   status".strip(
        " "
    ) in result_with_dataset.stdout.strip(" ")
    assert "dataset2  Dataset 2  ENABLED".strip(
        " "
    ) in result_with_dataset.stdout.strip(" ")
    assert "dataset1  Dataset 1  ENABLED".strip(
        " "
    ) in result_with_dataset.stdout.strip(" ")
    assert "dataset    Dataset  ENABLED".strip(" ") in result_with_dataset.stdout.strip(
        " "
    )


def _get_status(db, schema_name):
    (status, *_), *_ = db.execute_fetchall(f"SELECT status FROM {schema_name}")
    return status
