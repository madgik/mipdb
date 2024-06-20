import os
import time

import pytest
import docker
import toml

from mipdb.monetdb import MonetDB, credentials_from_config
from mipdb.monetdb_tables import User
from mipdb.reader import JsonFileReader
from mipdb.sqlite import SQLiteDB

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
CONF_FILE = f"{TEST_DIR}/config.toml"
IP = "127.0.0.1"
PORT = 50123
USERNAME = "admin"
PASSWORD = "executor"
DB_NAME = "db"

DEFAULT_OPTION = [
    "--conf_file_path",
    TEST_DIR + "/config.toml",
]


@pytest.fixture
def data_model_metadata():
    reader = JsonFileReader(DATA_MODEL_FILE)
    return reader.read()


class MonetDBSetupError(Exception):
    """Raised when the MonetDB container is unable to start."""


class DockerNotFoundError(Exception):
    """Raised when attempting to run tests while docker daemon is not running."""


@pytest.fixture(scope="session")
def monetdb_container():
    try:
        client = docker.from_env()
    except docker.errors.DockerException:
        raise DockerNotFoundError(
            "The docker daemon cannot be found. Make sure it is running." ""
        )
    try:
        container = client.containers.get("mipdb-testing")
    except docker.errors.NotFound:
        container = client.containers.run(
            "madgik/exareme2_db:latest",
            detach=True,
            ports={"50000/tcp": PORT},
            name="mipdb-testing",
            volumes=[f"{ABSOLUTE_PATH_DATA_FOLDER}:{ABSOLUTE_PATH_DATA_FOLDER}"],
            publish_all_ports=True,
        )
    # The time needed to start a monetdb container varies considerably. We need
    # to wait until some phrases appear in the logs to avoid starting the tests
    # too soon. The process is abandoned after 100 tries (50 sec).
    for _ in range(100):
        if b"new database mapi:monetdb" in container.logs():
            break
        time.sleep(0.5)
    else:
        raise MonetDBSetupError
    yield
    container = client.containers.get("mipdb-testing")
    container.remove(v=True, force=True)


@pytest.fixture(scope="function")
def sqlite_db():

    with open(CONF_FILE, "r") as file:
        toml_data = toml.load(file)

    toml_data["SQLITE_DB_PATH"] = f"{TEST_DIR}/sqlite.db"

    with open(CONF_FILE, "w") as file:
        toml.dump(toml_data, file)

    credentials = credentials_from_config(CONF_FILE)
    return SQLiteDB.from_config({"db_path": credentials["SQLITE_DB_PATH"]})


@pytest.fixture(scope="function")
def monetdb():
    credentials = credentials_from_config(CONF_FILE)

    return MonetDB.from_config(
        {
            "username": credentials["MONETDB_ADMIN_USERNAME"],
            "password": credentials["MONETDB_LOCAL_PASSWORD"],
            "ip": credentials["DB_IP"],
            "port": credentials["DB_PORT"],
            "dbfarm": credentials["DB_NAME"],
        }
    )


def cleanup_monetdb(monetdb):
    schemas = monetdb.get_schemas()
    for schema in schemas:
        if schema not in [user.value for user in User]:
            monetdb.drop_schema(schema)


def cleanup_sqlite(sqlite_db):
    sqlite_tables = sqlite_db.get_all_tables()
    if sqlite_tables:
        for table in sqlite_tables:
            sqlite_db.execute(f'DROP TABLE "{table}";')


@pytest.fixture(scope="function")
def cleanup_db(sqlite_db, monetdb):
    yield
    cleanup_sqlite(sqlite_db)
    cleanup_monetdb(monetdb)
