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
from mipdb import tag_dataset
from mipdb import tag_data_model
from mipdb import list_data_models
from mipdb import list_datasets
from mipdb import validate_dataset
from mipdb.exceptions import ExitCode


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_init(db):
    # Setup
    runner = CliRunner()
    # Check data_model not present already
    assert "mipdb_metadata" not in db.get_schemas()
    # Test
    result = runner.invoke(init, [])
    assert result.exit_code == ExitCode.OK
    assert "mipdb_metadata" in db.get_schemas()
    assert db.execute(f"select * from mipdb_metadata.data_models").fetchall() == []
    assert db.execute(f"select * from mipdb_metadata.actions").fetchall() == []


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_data_model(db):
    # Setup
    runner = CliRunner()
    data_model_file = "tests/data/data_model.json"
    # Check data_model not present already
    assert "data_model:1.0" not in db.get_schemas()
    runner.invoke(init, [])
    # Test
    result = runner.invoke(add_data_model, [data_model_file, "-v", "1.0"])
    assert result.exit_code == ExitCode.OK
    assert "data_model:1.0" in db.get_schemas()
    data_models = db.execute(f"select * from mipdb_metadata.data_models").fetchall()
    assert data_models == [(1, "data_model", "1.0", "The Data Model", "DISABLED", None)]
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()
    action_id, action = action_record[0]
    assert action_id == 1
    assert action != ""
    assert json.loads(action)["action"] == "ADD DATA MODEL"
    metadata = db.execute(
        f'select * from "data_model:1.0".variables_metadata'
    ).fetchall()
    # TODO better test
    assert len(metadata) == 5


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_data_model(db):
    # Setup
    runner = CliRunner()
    data_model_file = "tests/data/data_model.json"
    # Check data_model not present already
    assert "data_model:1.0" not in db.get_schemas()
    runner.invoke(init, [])
    runner.invoke(add_data_model, [data_model_file, "-v", "1.0"])
    # Test
    result = runner.invoke(delete_data_model, ["data_model", "-v", "1.0", "-f"])
    assert result.exit_code == ExitCode.OK
    assert "data_model:1.0" not in db.get_schemas()
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()
    action_id, action = action_record[1]
    assert action_id == 2
    assert action != ""
    assert json.loads(action)["action"] == "DELETE DATA MODEL"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_add_dataset(db):
    # Setup
    runner = CliRunner()
    data_model_file = "tests/data/data_model.json"
    dataset_file = "tests/data/dataset.csv"

    # Check dataset not present already
    runner.invoke(init, [])
    runner.invoke(add_data_model, [data_model_file, "-v", "1.0"])
    assert not db.get_datasets(columns=["code"])

    # Test
    result = runner.invoke(
        add_dataset, [dataset_file, "--data-model", "data_model", "-v", "1.0"]
    )
    assert "dataset1" == db.get_datasets(columns=["code"])[0][0]

    assert result.exit_code == ExitCode.OK
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()
    action_id, action = action_record[1]
    assert action_id == 2
    assert action != ""
    assert json.loads(action)["action"] == "ADD DATASET"
    data = db.execute(f'select * from "data_model:1.0".primary_data').fetchall()
    assert len(data) == 5


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_validate_dataset(db):
    # Setup
    runner = CliRunner()
    data_model_file = "tests/data/data_model.json"
    dataset_file = "tests/data/dataset.csv"

    # Check dataset not present already
    runner.invoke(init, [])
    runner.invoke(add_data_model, [data_model_file, "-v", "1.0"])
    assert not db.get_datasets(columns=["code"])

    # Test
    result = runner.invoke(
        validate_dataset, [dataset_file, "-d", "data_model", "-v", "1.0"]
    )
    assert result.exit_code == ExitCode.OK


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_delete_dataset(db):
    # Setup
    runner = CliRunner()
    data_model_file = "tests/data/data_model.json"
    dataset_file = "tests/data/dataset.csv"

    # Check dataset not present already
    runner.invoke(init, [])
    runner.invoke(add_data_model, [data_model_file, "-v", "1.0"])
    runner.invoke(
        add_dataset, [dataset_file, "--data-model", "data_model", "-v", "1.0"]
    )
    assert "dataset1" == db.get_datasets(columns=["code"])[0][0]

    # Test
    result = runner.invoke(
        delete_dataset, ["dataset1", "-d", "data_model", "-v", "1.0"]
    )
    assert result.exit_code == ExitCode.OK

    assert not db.get_datasets(columns=["code"])
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()
    action_id, action = action_record[2]
    assert action_id == 3
    assert action != ""
    assert json.loads(action)["action"] == "DELETE DATASET"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_tag_data_model(db):
    # Setup
    runner = CliRunner()
    data_model_file = "tests/data/data_model.json"

    runner.invoke(init, [])
    runner.invoke(add_data_model, [data_model_file, "-v", "1.0"])

    # Test
    result = runner.invoke(tag_data_model, ["data_model", "-t", "tag", "-v", "1.0"])
    assert result.exit_code == ExitCode.OK
    (properties, *_), *_ = db.execute(
        f"select properties from mipdb_metadata.data_models"
    ).fetchall()
    assert '{"tags": ["tag"], "properties": {}}' == properties
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()
    action_id, action = action_record[1]
    assert action_id == 2
    assert action != ""
    assert json.loads(action)["action"] == "ADD DATA MODEL TAG"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_untag_data_model(db):
    # Setup
    runner = CliRunner()
    data_model_file = "tests/data/data_model.json"

    # Check dataset not present already
    runner.invoke(init, [])
    runner.invoke(add_data_model, [data_model_file, "-v", "1.0"])
    runner.invoke(tag_data_model, ["data_model", "-t", "tag", "-v", "1.0"])

    # Test
    result = runner.invoke(
        tag_data_model, ["data_model", "-t", "tag", "-v", "1.0", "-r"]
    )
    assert result.exit_code == ExitCode.OK
    (properties, *_), *_ = db.execute(
        f"select properties from mipdb_metadata.data_models"
    ).fetchall()
    assert '{"tags": [], "properties": {}}' == properties
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()

    action_id, action = action_record[2]
    assert action_id == 3
    assert action != ""
    assert json.loads(action)["action"] == "REMOVE DATA MODEL TAG"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_property_data_model_addition(db):
    # Setup
    runner = CliRunner()
    data_model_file = "tests/data/data_model.json"

    runner.invoke(init, [])
    runner.invoke(add_data_model, [data_model_file, "-v", "1.0"])

    # Test
    result = runner.invoke(
        tag_data_model, ["data_model", "-t", "key=value", "-v", "1.0"]
    )
    assert result.exit_code == ExitCode.OK
    (properties, *_), *_ = db.execute(
        f"select properties from mipdb_metadata.data_models"
    ).fetchall()
    assert '{"tags": [], "properties": {"key": "value"}}' == properties
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()
    action_id, action = action_record[1]
    assert action_id == 2
    assert action != ""
    assert json.loads(action)["action"] == "ADD DATA MODEL TAG"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_property_data_model_deletion(db):
    # Setup
    runner = CliRunner()
    data_model_file = "tests/data/data_model.json"

    # Check dataset not present already
    runner.invoke(init, [])
    runner.invoke(add_data_model, [data_model_file, "-v", "1.0"])
    runner.invoke(tag_data_model, ["data_model", "-t", "key=value", "-v", "1.0"])

    # Test
    result = runner.invoke(
        tag_data_model, ["data_model", "-t", "key=value", "-v", "1.0", "-r"]
    )
    assert result.exit_code == ExitCode.OK
    (properties, *_), *_ = db.execute(
        f"select properties from mipdb_metadata.data_models"
    ).fetchall()
    assert '{"tags": [], "properties": {}}' == properties
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()

    action_id, action = action_record[2]
    assert action_id == 3
    assert action != ""
    assert json.loads(action)["action"] == "REMOVE DATA MODEL TAG"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_tag_dataset(db):
    # Setup
    runner = CliRunner()
    data_model_file = "tests/data/data_model.json"
    dataset_file = "tests/data/dataset.csv"

    # Check dataset not present already
    runner.invoke(init, [])
    runner.invoke(add_data_model, [data_model_file, "-v", "1.0"])
    runner.invoke(
        add_dataset, [dataset_file, "--data-model", "data_model", "-v", "1.0"]
    )

    # Test
    result = runner.invoke(
        tag_dataset,
        ["dataset1", "-t", "tag", "-d", "data_model", "-v", "1.0"],
    )
    assert result.exit_code == ExitCode.OK
    (properties, *_), *_ = db.execute(
        f"select properties from mipdb_metadata.datasets"
    ).fetchall()
    assert '{"tags": ["tag"], "properties": {}}' == properties
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()
    action_id, action = action_record[2]
    assert action_id == 3
    assert action != ""
    assert json.loads(action)["action"] == "ADD DATASET TAG"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_untag_dataset(db):
    # Setup
    runner = CliRunner()
    data_model_file = "tests/data/data_model.json"
    dataset_file = "tests/data/dataset.csv"

    # Check dataset not present already
    runner.invoke(init, [])
    result = runner.invoke(add_data_model, [data_model_file, "-v", "1.0"])
    assert result.exit_code == ExitCode.OK

    result = runner.invoke(add_dataset, [dataset_file, "-d", "data_model", "-v", "1.0"])
    assert result.exit_code == ExitCode.OK
    result = runner.invoke(
        tag_dataset,
        ["dataset1", "-t", "tag", "-d", "data_model", "-v", "1.0"],
    )
    assert result.exit_code == ExitCode.OK

    # Test
    result = runner.invoke(
        tag_dataset,
        ["dataset1", "-t", "tag", "-d", "data_model", "-v", "1.0", "-r"],
    )
    assert result.exit_code == ExitCode.OK
    (properties, *_), *_ = db.execute(
        f"select properties from mipdb_metadata.datasets"
    ).fetchall()
    assert '{"tags": [], "properties": {}}' == properties
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()
    action_id, action = action_record[2]
    assert action_id == 3
    assert action != ""
    assert json.loads(action)["action"] == "ADD DATASET TAG"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_property_dataset_addition(db):
    # Setup
    runner = CliRunner()
    data_model_file = "tests/data/data_model.json"
    dataset_file = "tests/data/dataset.csv"

    # Check dataset not present already
    runner.invoke(init, [])
    runner.invoke(add_data_model, [data_model_file, "-v", "1.0"])
    runner.invoke(add_dataset, [dataset_file, "-d", "data_model", "-v", "1.0"])

    # Test
    result = runner.invoke(
        tag_dataset,
        ["dataset1", "-t", "key=value", "-d", "data_model", "-v", "1.0"],
    )
    assert result.exit_code == ExitCode.OK
    (properties, *_), *_ = db.execute(
        f"select properties from mipdb_metadata.datasets"
    ).fetchall()
    assert '{"tags": [], "properties": {"key": "value"}}' == properties
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()
    action_id, action = action_record[2]
    assert action_id == 3
    assert action != ""
    assert json.loads(action)["action"] == "ADD DATASET TAG"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_property_dataset_deletion(db):
    # Setup
    runner = CliRunner()
    data_model_file = "tests/data/data_model.json"
    dataset_file = "tests/data/dataset.csv"

    # Check dataset not present already
    runner.invoke(init, [])
    result = runner.invoke(add_data_model, [data_model_file, "-v", "1.0"])
    assert result.exit_code == ExitCode.OK

    result = runner.invoke(add_dataset, [dataset_file, "-d", "data_model", "-v", "1.0"])
    assert result.exit_code == ExitCode.OK
    result = runner.invoke(
        tag_dataset,
        ["dataset1", "-t", "key=value", "-d", "data_model", "-v", "1.0"],
    )
    assert result.exit_code == ExitCode.OK

    # Test
    result = runner.invoke(
        tag_dataset,
        ["dataset1", "-t", "key=value", "-d", "data_model", "-v", "1.0", "-r"],
    )
    assert result.exit_code == ExitCode.OK
    (properties, *_), *_ = db.execute(
        f"select properties from mipdb_metadata.datasets"
    ).fetchall()
    assert '{"tags": [], "properties": {}}' == properties
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()
    action_id, action = action_record[2]
    assert action_id == 3
    assert action != ""
    assert json.loads(action)["action"] == "ADD DATASET TAG"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_enable_data_model(db):
    # Setup
    runner = CliRunner()
    data_model_file = "tests/data/data_model.json"
    # Check status is disabled
    runner.invoke(init, [])
    runner.invoke(add_data_model, [data_model_file, "-v", "1.0"])
    assert _get_status(db, "data_models") == "DISABLED"

    # Test
    result = runner.invoke(enable_data_model, ["data_model", "-v", "1.0"])
    assert result.exit_code == ExitCode.OK
    assert _get_status(db, "data_models") == "ENABLED"
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()
    action_id, action = action_record[1]
    assert action_id == 2
    assert action != ""
    assert json.loads(action)["action"] == "ENABLE DATA MODEL"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_disable_data_model(db):
    # Setup
    runner = CliRunner()
    data_model_file = "tests/data/data_model.json"
    # Check status is enabled
    runner.invoke(init, [])
    runner.invoke(add_data_model, [data_model_file, "-v", "1.0"])
    runner.invoke(enable_data_model, ["data_model", "-v", "1.0"])
    assert _get_status(db, "data_models") == "ENABLED"

    # Test
    result = runner.invoke(disable_data_model, ["data_model", "-v", "1.0"])
    assert result.exit_code == ExitCode.OK
    assert _get_status(db, "data_models") == "DISABLED"
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()
    action_id, action = action_record[2]
    assert action_id == 3
    assert action != ""
    assert json.loads(action)["action"] == "DISABLE DATA MODEL"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_enable_dataset(db):
    # Setup
    runner = CliRunner()
    data_model_file = "tests/data/data_model.json"
    dataset_file = "tests/data/dataset.csv"

    # Check dataset not present already
    runner.invoke(init, [])
    runner.invoke(add_data_model, [data_model_file, "-v", "1.0"])
    runner.invoke(add_dataset, [dataset_file, "-d", "data_model", "-v", "1.0"])
    assert _get_status(db, "datasets") == "DISABLED"

    # Test
    result = runner.invoke(
        enable_dataset, ["dataset1", "-d", "data_model", "-v", "1.0"]
    )
    assert result.exit_code == ExitCode.OK
    assert _get_status(db, "datasets") == "ENABLED"
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()
    action_id, action = action_record[2]
    assert action_id == 3
    assert action != ""
    assert json.loads(action)["action"] == "ENABLE DATASET"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_disable_dataset(db):
    # Setup
    runner = CliRunner()
    data_model_file = "tests/data/data_model.json"
    dataset_file = "tests/data/dataset.csv"

    # Check dataset not present already
    runner.invoke(init, [])
    runner.invoke(add_data_model, [data_model_file, "-v", "1.0"])
    runner.invoke(add_dataset, [dataset_file, "-d", "data_model", "-v", "1.0"])
    runner.invoke(enable_dataset, ["dataset1", "-d", "data_model", "-v", "1.0"])
    assert _get_status(db, "datasets") == "ENABLED"

    # Test
    result = runner.invoke(
        disable_dataset, ["dataset1", "-d", "data_model", "-v", "1.0"]
    )
    assert _get_status(db, "datasets") == "DISABLED"
    assert result.exit_code == ExitCode.OK
    action_record = db.execute(f"select * from mipdb_metadata.actions").fetchall()
    action_id, action = action_record[3]
    assert action_id == 4
    assert action != ""
    assert json.loads(action)["action"] == "DISABLE DATASET"


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_list_data_models(db):
    # Setup
    runner = CliRunner()
    data_model_file = "tests/data/data_model.json"
    dataset_file = "tests/data/dataset.csv"

    # Check data_model not present already
    assert "data_model:1.0" not in db.get_schemas()
    runner.invoke(init, [])
    result = runner.invoke(list_data_models)
    runner.invoke(add_data_model, [data_model_file, "-v", "1.0"])
    result_with_data_model = runner.invoke(list_data_models)
    runner.invoke(
        add_dataset, [dataset_file, "--data-model", "data_model", "-v", "1.0"]
    )
    result_with_data_model_and_dataset = runner.invoke(list_data_models)

    # Test
    assert result.exit_code == ExitCode.OK
    assert result.stdout == "There are no data models.\n"
    assert result_with_data_model.exit_code == ExitCode.OK
    assert (
        "data_model_id        code version           label    status  count"
        in result_with_data_model.stdout
    )
    assert (
        "0              1  data_model     1.0  The Data Model  DISABLED      0"
        in result_with_data_model.stdout
    )
    assert result_with_data_model_and_dataset.exit_code == ExitCode.OK
    assert (
        "data_model_id        code version           label    status  count"
        in result_with_data_model_and_dataset.stdout
    )
    assert (
        "0              1  data_model     1.0  The Data Model  DISABLED      1"
        in result_with_data_model_and_dataset.stdout
    )


@pytest.mark.database
@pytest.mark.usefixtures("monetdb_container", "cleanup_db")
def test_list_datasets(db):
    # Setup
    runner = CliRunner()
    data_model_file = "tests/data/data_model.json"
    dataset_file = "tests/data/dataset.csv"

    # Check dataset not present already
    runner.invoke(init, [])
    runner.invoke(add_data_model, [data_model_file, "-v", "1.0"])
    result = runner.invoke(list_datasets)
    runner.invoke(
        add_dataset, [dataset_file, "--data-model", "data_model", "-v", "1.0"]
    )
    result_with_dataset = runner.invoke(list_datasets)

    # Test
    assert result.exit_code == ExitCode.OK
    assert result.stdout == "There are no datasets.\n"
    assert result_with_dataset.exit_code == ExitCode.OK
    assert (
        "dataset_id  data_model_id      code label    status  count"
        in result_with_dataset.stdout
    )
    assert (
        "0           1              1  dataset1  None  DISABLED      5"
        in result_with_dataset.stdout
    )


def _get_status(db, schema_name):
    (status, *_), *_ = db.execute(
        f'SELECT status FROM "mipdb_metadata".{schema_name}'
    ).fetchall()
    return status
