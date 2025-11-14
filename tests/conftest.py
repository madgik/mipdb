import os

import pytest

from mipdb.duckdb import DuckDB
from mipdb.reader import JsonFileReader

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
DATA_MODEL_FILE = "tests/data/success/data_model_v_1_0/CDEsMetadata.json"
DATASET_FILE = "tests/data/success/data_model_v_1_0/dataset.csv"
DATA_FOLDER = "tests/data/"
SUCCESS_DATA_FOLDER = DATA_FOLDER + "success"
FAIL_DATA_FOLDER = DATA_FOLDER + "fail"
ABSOLUTE_PATH_DATA_FOLDER = f"{TEST_DIR}/data/"
ABSOLUTE_PATH_DATASET_FILE = f"{TEST_DIR}/data/success/data_model_v_1_0/dataset.csv"
ABSOLUTE_PATH_DATASET_FILE_MULTIPLE_DATASET = (
    f"{TEST_DIR}/data/success/data_model_v_1_0/dataset123.csv"
)
ABSOLUTE_PATH_SUCCESS_DATA_FOLDER = ABSOLUTE_PATH_DATA_FOLDER + "success"
ABSOLUTE_PATH_FAIL_DATA_FOLDER = ABSOLUTE_PATH_DATA_FOLDER + "fail"


@pytest.fixture
def data_model_metadata():
    reader = JsonFileReader(DATA_MODEL_FILE)
    return reader.read()


@pytest.fixture
def duckdb_path(tmp_path):
    return str(tmp_path / "duckdb.db")


@pytest.fixture
def duckdb_option(duckdb_path):
    return ["--duckdb", duckdb_path]


@pytest.fixture
def duckdb(duckdb_path):
    return DuckDB.from_config({"db_path": duckdb_path})
